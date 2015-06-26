import collections
import datetime
import itertools
import sqlalchemy as sa
import traceback
import tqdm

from databot.db.serializers import dumps, loads
from databot.logging import PROGRESS, INFO, ERROR
from databot.db.utils import Row, create_row, get_or_create
from databot.db import models
from databot.db.windowedquery import windowed_query


def wrapper(handler, wrap):
    if wrap is None:
        return handler

    def wrapped(row):
        return wrap(handler, row)
    return wrapped


def keyvalueitems(key, value=None):
    if isinstance(key, collections.Iterable) and not isinstance(key, (str, bytes)):
        items = iter(key)
    else:
        return [(key, value)]

    try:
        item = next(items)
    except StopIteration:
        return []

    if isinstance(item, tuple):
        return itertools.chain([item], items)
    else:
        return itertools.chain([(item, None)], ((k, None) for k in items))


class TaskData(object):
    def __init__(self, task):
        self.task = task
        self.table = task.table
        self.engine = task.bot.conn.engine

    def count(self):
        return self.engine.execute(self.table.count()).scalar()

    def rows(self):
        for row in self.engine.execute(self.table.select().order_by(self.table.c.id)):
            yield Row(row, value=loads(row.value))

    def items(self):
        for row in self.rows():
            yield row.key, row.value

    def keys(self):
        for row in self.rows():
            yield row.key

    def values(self):
        for row in self.rows():
            yield row.value


class TaskErrors(object):
    def __init__(self, task):
        self.task = task
        self.engine = task.bot.engine

    def __call__(self):
        if self.task.source:
            error = models.errors.alias('error')
            table = self.task.source.table.alias('table')

            query = (
                sa.select([error, table], use_labels=True).
                select_from(
                    error.
                    join(table, error.c.row_id == table.c.id)
                ).
                where(error.c.state_id == self.task.get_state().id).
                order_by(error.c.id)
            )

            for row in windowed_query(self.engine, query, table.c.id):
                item = create_row(row, 'error_')
                item['row'] = create_row(row, 'table_')
                item.row.value = loads(item.row.value)
                yield item

    def count(self):
        if self.task.source:
            state = self.task.get_state()
            return self.engine.execute(models.errors.count(models.errors.c.state_id == state.id)).scalar()
        else:
            return 0

    def rows(self):
        for error in self():
            yield error.row

    def items(self):
        for row in self.rows():
            yield row.key, row.value

    def keys(self):
        for row in self.rows():
            yield row.key

    def values(self):
        for row in self.rows():
            yield row.value

    def report(self, error_or_row, message):
        self.task.log(ERROR)
        self.task.log(ERROR, message)
        now = datetime.datetime.utcnow()
        if 'retries' in error_or_row:
            error = error_or_row
            self.engine.execute(
                models.errors.update(sa.and_(
                    models.errors.c.state_id == error.state_id,
                    models.errors.c.row_id == error.row_id,
                )).values(
                    retries=models.errors.c.retries + 1,
                    traceback=message,
                    updated=datetime.datetime.utcnow(),
                ),
            )
        else:
            row = error_or_row
            state = self.task.get_state()
            self.engine.execute(
                models.errors.insert(),
                state_id=state.id,
                row_id=row.id,
                retries=0,
                traceback=message,
                created=now,
                updated=now,
            )


class Appender(object):
    def __init__(self, source):
        self.data = []
        self.num = 1
        self.source = source

    def append(self, data):
        self.num += 1
        self.data.extend(data)
        if self.num % 100 == 0:
            self.save()

    def save(self):
        self.source.append(self.data, log=False)
        self.data = []
        engine.execute(models.state.update(models.state.c.id == state.id), offset=row.id)


class Task(object):
    def __init__(self, bot, id, name, handler, table, wrap=None, data=False):
        """

        Parameters:
        - bot: databot.Bot
        - id: int, primary key of this task from databot.db.models.tasks.id
        - name: str, human readable task identifier
        - handler: callable, it will be called with each row if rows=False or with all rows if rows=True
        - table: sqlalchemy.Table, a table where data is stored
        - wrap: callable, a decorator, that wraps handler, can be used to do common initialization for the handler
        - data: pass data appender arbument to handler

        """
        self.bot = bot
        self.id = id
        self.code = 't%d' % id
        self.name = name
        self.handler = wrapper(handler, wrap)
        self.table = table
        self.wrap = wrap
        self.data = TaskData(self)
        self.errors = TaskErrors(self)
        self.pass_data_arg = data

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<databot.Task[%d]: %s>' % (self.id, self.name)

    def __enter__(self):
        self.log(PROGRESS, self.name)
        self.bot.stack.append(self)
        return self

    def __exit__(self, *exc):
        self.bot.stack.pop()
        return False

    @property
    def source(self):
        if self.bot.stack:
            return self.bot.stack[-1]

    def log(self, level, message='', end='\n'):
        if self.bot.stack:
            prefix = '  ' * len(self.bot.stack)
        elif end != '\n':
            prefix = '%s: ' % self.name
        else:
            prefix = ''
        self.bot.log(level, '%s%s' % (prefix, message), end=end)

    def get_state(self):
        source, target = self.source, self
        return get_or_create(self.bot.engine, models.state, ['source_id', 'target_id'], dict(
            source_id=(source.id if source else None),
            target_id=target.id,
            offset=0,
        ))

    def is_filled(self):
        if self.source:
            state = self.get_state()
            query = self.source.table.select(self.source.table.c.id > state.offset).limit(1)
            return len(self.bot.engine.execute(query).fetchall()) > 0
        else:
            return False

    def append(self, key, value=None, conn=None, log=True):
        """Append data to the task's storage

        You can call this method in following ways::

            append(key, value)
            append([key, key, key])
            append([(key, value), (key, value), (key, value)])

        """
        conn = conn or self.bot.engine
        if log:
            self.log(INFO, 'append...', end=' ')

        data = []
        for i, (key, value) in enumerate(keyvalueitems(key, value), 1):
            now = datetime.datetime.utcnow()
            data.append({'key': key, 'value': dumps(value), 'created': now})
            if i % 1000 == 0:
                self.bot.engine.execute(self.table.insert(), data)
                data = []

        if data:
            self.bot.engine.execute(self.table.insert(), data)

        if log:
            self.log(INFO, 'done.')
        return self

    def clean(self, age=None, now=None):
        self.log(INFO, 'clean...', end=' ')
        if age:
            now = now or datetime.datetime.utcnow()
            timestamp = now - age
            query = self.table.delete(self.table.c.created <= timestamp)
        else:
            query = self.table.delete()
        self.bot.engine.execute(query)
        self.log(INFO, 'done.')
        return self

    def dedup(self):
        """Delete all records with duplicate keys except ones created first."""
        self.log(INFO, 'dedup...', end=' ')

        agg = (
            sa.select([self.table.c.key, sa.func.min(self.table.c.id).label('id')]).
            group_by(self.table.c.key).
            having(sa.func.count(self.table.c.id) > 1).
            alias()
        )

        query = (
            sa.select([self.table.c.id]).
            select_from(self.table.join(agg, sa.and_(
                self.table.c.key == agg.c.key,
                self.table.c.id != agg.c.id,
            )))
        )

        if self.bot.engine.name == 'mysql':
            # http://stackoverflow.com/a/45498/475477
            query = sa.select([query.alias().c.id])

        self.bot.engine.execute(self.table.delete(self.table.c.id.in_(query)))
        self.log(INFO, 'done.')
        return self

    def compact(self):
        """Delete all records with duplicate keys except ones created last."""
        self.log(INFO, 'compact...', end=' ')

        agg = (
            sa.select([self.table.c.key, sa.func.max(self.table.c.id).label('id')]).
            group_by(self.table.c.key).
            having(sa.func.count(self.table.c.id) > 1).
            alias()
        )

        query = (
            sa.select([self.table.c.id]).
            select_from(self.table.join(agg, sa.and_(
                self.table.c.key == agg.c.key,
                self.table.c.id != agg.c.id,
            )))
        )

        if self.bot.engine.name == 'mysql':
            # http://stackoverflow.com/a/45498/475477
            query = sa.select([query.alias().c.id])

        self.bot.engine.execute(self.table.delete(self.table.c.id.in_(query)))
        self.log(INFO, 'done.')
        return self

    def count(self):
        """How much items left to process."""
        if self.source:
            state = self.get_state()
            return self.bot.engine.execute(self.source.table.count(self.source.table.c.id > state.offset)).scalar()
        else:
            return 0

    def rows(self):
        if self.source:
            table = self.source.table
            query = table.select(table.c.id > self.get_state().offset).order_by(table.c.id)
            for row in windowed_query(self.bot.engine, query, table.c.id):
                yield Row(row, value=loads(row['value']))

    def items(self):
        for row in self.rows():
            yield row.key, row.value

    def keys(self):
        for row in self.rows():
            yield row.key

    def values(self):
        for row in self.rows():
            yield row.value

    def run(self):
        self.log(INFO, 'run...', end=' ')
        state = self.get_state()
        engine = self.bot.engine

        appender, data, errors = Appender(('data', 'errors'))
        for row in tqdm.tqdm(self.rows(), self.name, total=self.count()):
            try:
                data.append(self.handler(row))
            except KeyboardInterrupt:
                raise
            except:
                self.errors.report(row, traceback.format_exc())
                engine.execute(models.state.update(models.state.c.id == state.id), offset=row.id)

        appender.save()
        if data:
            self.append(data, log=False)
            engine.execute(models.state.update(models.state.c.id == state.id), offset=row.id)

        self.log(INFO, 'done.')
        return self

    def retry(self):
        self.log(INFO, 'retry...', end=' ')
        data = []
        errors = []
        for i, error in enumerate(tqdm.tqdm(self.errors(), self.name, total=self.errors.count()), 1):
            try:
                data.extend(self.handler(error.row))
            except KeyboardInterrupt:
                raise
            except:
                self.errors.report(error, traceback.format_exc())
            else:
                errors.append(error.id)

            if i % 100 == 0:
                self.append(data, log=False)
                self.bot.engine.execute(models.errors.delete(models.errors.c.id.in_(errors)))

        if data:
            self.append(data, log=False)
            self.bot.engine.execute(models.errors.delete(models.errors.c.id.in_(errors)))

        self.log(INFO, 'done.')
        return self

    def export(self, name, file):
        if name.endswith('.csv'):
            import databot.exporters.csv
            exporter = databot.exporters.csv.Exporter(file)
            exporter.export(self.data.rows())
        else:
            raise ValueError("Don't know how to export %s." % name)
