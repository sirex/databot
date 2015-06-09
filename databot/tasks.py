import collections
import datetime
import itertools
import sqlalchemy as sa
import traceback

from databot.db.serializers import dumps, loads
from databot.logging import PROGRESS, INFO, ERROR
from databot.db.utils import Row, get_or_create
from databot.db import models


def wrapper(handler, wrap):
    if wrap is None:
        return handler

    def wrapped(item):
        yield from wrap(handler, item)
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
        for row in self.engine.execute(self.table.select()):
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
            state = self.task.get_state()
            table = self.task.source.table
            query = models.errors.select(models.errors.c.state_id == state.id)
            errors = self.engine.execute(query)
            for error in errors:
                row = self.engine.execute(table.select(table.c.id == error['row_id'])).first()
                yield Row(error, row=Row(row, value=loads(row.value)))

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


class Task(object):
    def __init__(self, bot, id, name, handler, table, wrap=None):
        self.bot = bot
        self.id = id
        self.name = name
        self.handler = wrapper(handler, wrap)
        self.table = table
        self.wrap = wrap
        self.data = TaskData(self)
        self.errors = TaskErrors(self)

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
        for key, value in keyvalueitems(key, value):
            now = datetime.datetime.utcnow()
            self.bot.engine.execute(self.table.insert(), key=key, value=dumps(value), created=now)
        if log:
            self.log(INFO, 'done.')
        return self

    def clean(self, age=None, now=None):
        self.log(INFO, 'clean...', end=' ')
        if age:
            now = now or datetime.datetime.utcnow()
            timestamp = now - age
            query = self.table.delete().where(self.table.c.created <= timestamp)
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
            alias('agg')
        )
        query = (
            sa.select([self.table.c.id]).
            select_from(self.table.join(agg, sa.and_(
                self.table.c.key == agg.c.key,
                self.table.c.id != agg.c.id,
            )))
        )
        self.delete(id for id, in self.bot.engine.execute(query))
        self.log(INFO, 'done.')
        return self

    def compact(self):
        """Delete all records with duplicate keys except ones created last."""
        self.log(INFO, 'compact...', end=' ')
        agg = (
            sa.select([self.table.c.key, sa.func.max(self.table.c.id).label('id')]).
            group_by(self.table.c.key).
            having(sa.func.count(self.table.c.id) > 1).
            alias('agg')
        )
        query = (
            sa.select([self.table.c.id]).
            select_from(self.table.join(agg, sa.and_(
                self.table.c.key == agg.c.key,
                self.table.c.id != agg.c.id,
            )))
        )
        self.delete(id for id, in self.bot.engine.execute(query))
        self.log(INFO, 'done.')
        return self

    def delete(self, items, bufsize=524288):
        ids, size = [], 0
        for id in items:
            ids.append(id)
            size += len(str(id))
            if size > bufsize:
                self.bot.engine.execute(self.table.delete().where(self.table.c.id.in_(ids)))
                ids, size = [], 0
        if ids:
            self.bot.engine.execute(self.table.delete().where(self.table.c.id.in_(ids)))

    def count(self):
        """How much items left to process."""
        if self.source:
            state = self.get_state()
            return self.bot.engine.execute(self.source.table.count(self.source.table.c.id > state.offset)).scalar()
        else:
            return 0

    def rows(self, chunks=None):
        if self.bot.engine.name == 'sqlite':
            chunks = 100
        if self.source:
            table = self.source.table
            has_rows = True
            old_offset = -1
            new_offset = self.get_state().offset
            while has_rows and old_offset < new_offset:
                has_rows = False
                old_offset = new_offset
                new_offset = self.get_state().offset
                query = table.select(table.c.id > new_offset).order_by(table.c.id)
                if chunks:
                    rows = list(self.bot.engine.execute(query.limit(chunks)))
                else:
                    rows = self.bot.engine.execute(query)
                for row in rows:
                    has_rows = True
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
        print(list(self.source.data.items()))
        for row in self.rows():
            print((self, row.key, row.value))
            try:
                for key, value in self.handler(row):
                    self.append(key, value, log=False)
            except:
                exception = traceback.format_exc()
                self.log(ERROR)
                self.log(ERROR, exception)
                now = datetime.datetime.utcnow()
                self.bot.engine.execute(
                    models.errors.insert(),
                    state_id=state.id,
                    row_id=row.id,
                    retries=0,
                    traceback=exception,
                    created=now,
                    updated=now,
                )
            self.bot.engine.execute(models.state.update(models.state.c.id == state.id), offset=row.id)
        self.log(INFO, 'done.')
        return self

    def retry(self):
        self.log(INFO, 'retry...', end=' ')
        for error in self.errors():
            try:
                for key, value in self.handler(error.row):
                    self.append(key, value, log=False)
            except:
                exception = traceback.format_exc()
                self.log(ERROR)
                self.log(ERROR, exception)
                self.bot.engine.execute(
                    models.errors.update(sa.and_(
                        models.errors.c.state_id == error.state_id,
                        models.errors.c.row_id == error.row_id,
                    )).values(
                        retries=models.errors.c.retries + 1,
                        traceback=exception,
                        updated=datetime.datetime.utcnow(),
                    ),
                )
            else:
                self.bot.engine.execute(models.errors.delete(models.errors.c.id == error.id))
        self.log(INFO, 'done.')
        return self
