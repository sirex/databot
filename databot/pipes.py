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
from databot.handlers import download, html
from databot.bulkinsert import BulkInsert
from databot.exporters import csv


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


class PipeData(object):
    def __init__(self, pipe):
        self.pipe = pipe
        self.table = pipe.table
        self.engine = pipe.bot.conn.engine

    def count(self):
        return self.engine.execute(self.table.count()).scalar()

    def rows(self, desc=False):
        order_by = self.table.c.id.desc() if desc else self.table.c.id
        for row in self.engine.execute(self.table.select().order_by(order_by)):
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


class PipeErrors(object):
    def __init__(self, pipe):
        self.pipe = pipe
        self.engine = pipe.bot.engine

    def __call__(self):
        if self.pipe.source:
            error = models.errors.alias('error')
            table = self.pipe.source.table.alias('table')

            query = (
                sa.select([error, table], use_labels=True).
                select_from(
                    error.
                    join(table, error.c.row_id == table.c.id)
                ).
                where(error.c.state_id == self.pipe.get_state().id).
                order_by(error.c.id)
            )

            for row in windowed_query(self.engine, query, table.c.id):
                item = create_row(row, 'error_')
                item['row'] = create_row(row, 'table_')
                item.row.value = loads(item.row.value)
                yield item

    def count(self):
        if self.pipe.source:
            state = self.pipe.get_state()
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

    def report(self, error_or_row, message, bulk=None):
        self.pipe.log(ERROR)
        self.pipe.log(ERROR, message)
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
        elif bulk:
            row = error_or_row
            state = self.pipe.get_state()
            bulk.append(dict(
                state_id=state.id,
                row_id=row.id,
                retries=0,
                traceback=message,
                created=now,
                updated=now,
            ))
        else:
            row = error_or_row
            state = self.pipe.get_state()
            self.engine.execute(
                models.errors.insert(),
                state_id=state.id,
                row_id=row.id,
                retries=0,
                traceback=message,
                created=now,
                updated=now,
            )


class Pipe(object):
    def __init__(self, bot, id, name, table):
        """

        Parameters:
        - bot: databot.Bot
        - id: int, primary key of this pipe from databot.db.models.pipes.id
        - name: str, human readable pipe identifier
        - table: sqlalchemy.Table, a table where data is stored

        """
        self.bot = bot
        self.id = id
        self.name = name
        self.table = table
        self.data = PipeData(self)
        self.errors = PipeErrors(self)

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<databot.Pipe[%d]: %s>' % (self.id, self.name)

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

    def append(self, key, value=None, conn=None, log=True, bulk=None):
        """Append data to the pipe

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
            if bulk:
                bulk.append({'key': key, 'value': dumps(value), 'created': now})
            else:
                self.bot.engine.execute(self.table.insert(), key=key, value=dumps(value), created=now)
        if log:
            self.log(INFO, 'done.')
        return self

    def reset(self):
        state = self.get_state()
        self.bot.engine.execute(models.state.update(models.state.c.id == state.id), offset=0)
        return self

    def skip(self):
        state = self.get_state()
        engine = self.bot.engine
        source = self.source.table
        query = sa.select([source.c.id]).order_by(source.c.id.desc()).limit(1)
        offset = engine.execute(query).scalar()
        if offset:
            engine.execute(models.state.update(models.state.c.id == state.id), offset=offset)
        return self

    def offset(self, value=None):
        state = self.get_state()
        source = self.source.table
        engine = self.bot.engine

        offset = None

        if value:
            query = sa.select([source.c.id])
            if value > 0:
                query = query.where(source.c.id > state.offset).order_by(source.c.id.asc())
            else:
                query = query.where(source.c.id < state.offset).order_by(source.c.id.desc())
            query = query.limit(1).offset(abs(value) - 1)
            offset = engine.execute(query).scalar()
            if offset is None:
                if value > 0:
                    return self.skip()
                else:
                    return self.reset()
        if offset is not None:
            engine.execute(models.state.update(models.state.c.id == state.id), offset=offset)
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

    def call(self, handler):
        self.log(INFO, 'call...', end=' ')
        state = self.get_state()
        engine = self.bot.engine
        desc = '%s -> %s' % (self.source, self)

        if self.bot.args.retry:
            self.retry(handler)

        if self.bot.args.verbosity == 1 and not self.bot.args.debug:
            rows = tqdm.tqdm(self.rows(), desc, self.count())
        else:
            rows = self.rows()

        def post_save():
            if row:
                engine.execute(models.state.update(models.state.c.id == state.id), offset=row.id)

        pipe = BulkInsert(engine, self.table)
        errors = BulkInsert(engine, models.errors)

        if not self.bot.args.debug:
            pipe.post_save(post_save)

        n = 0
        row = None
        for row in rows:
            if self.bot.args.debug:
                self._verbose_append(handler, row, pipe)
            else:
                try:
                    if self.bot.args.verbosity > 1:
                        self._verbose_append(handler, row, pipe)
                    else:
                        self.append(handler(row), log=False, bulk=pipe)
                except:
                    self.errors.report(row, traceback.format_exc(), errors)
            n += 1

        pipe.save(post_save=True)
        errors.save()

        if self.bot.args.verbosity > 0:
            print('%s, rows processed: %d' % (desc, n))

        self.log(INFO, 'done.')
        return self

    def retry(self, handler):
        self.log(INFO, 'retry...', end=' ')

        engine = self.bot.engine
        desc = '%s -> %s (retry)' % (self.source, self)

        if self.bot.args.verbosity == 1 and not self.bot.args.debug:
            errors = tqdm.tqdm(self.errors(), desc, self.errors.count())
        else:
            errors = self.errors()

        def post_save():
            nonlocal error_ids
            if error_ids:
                engine.execute(models.errors.delete(models.errors.c.id.in_(error_ids)))
                error_ids = []

        pipe = BulkInsert(engine, self.table)
        pipe.post_save(post_save)

        n = 0
        error_ids = []
        for error in errors:
            if self.bot.args.debug:
                self._verbose_append(handler, error.row, pipe, append=False)
                error_ids.append(error.id)
            else:
                try:
                    if self.bot.args.verbosity > 1:
                        self._verbose_append(handler, error.row, pipe)
                    else:
                        self.append(handler(error.row), log=False, bulk=pipe)
                except:
                    self.errors.report(error, traceback.format_exc())
                else:
                    error_ids.append(error.id)
            n += 1

        pipe.save(post_save=True)

        if self.bot.args.verbosity == 1:
            print('%s, errors retried: %d' % (desc, n))

        self.log(INFO, 'done.')
        return self

    def _verbose_append(self, handler, row, bulk, append=True):
        print('-' * 72)
        print('source: id=%d key=%r' % (row.id, row.key))
        for key, value in keyvalueitems(handler(row)):
            if append:
                self.append(key, value, log=False, bulk=bulk)
            self.bot.printer.print_key_value(key, value, short=True)

    def export(self, path):
        csv.export(path, self)

    def download(self, key=None, **kwargs):
        from databot import row
        key = key or row.key
        kwargs.setdefault('delay', self.bot.download_delay)
        return self.call(download.download(key, **kwargs))

    def select(self, key, value=None):
        return self.call(html.Select(key, value))
