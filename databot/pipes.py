import collections
import datetime
import itertools
import sqlalchemy as sa
import traceback
import tqdm

import databot
from databot.db.serializers import serrow, serkey
from databot.db.utils import strip_prefix, create_row, get_or_create, Row
from databot.db.windowedquery import windowed_query
from databot.db.models import Compression
from databot.handlers import download, html
from databot.bulkinsert import BulkInsert
from databot.exporters import csv, jsonl


def wrapper(handler, wrap):
    if wrap is None:
        return handler

    def wrapped(row):
        return wrap(handler, row)
    return wrapped


def keyvalueitems(key, value=None):
    if isinstance(key, tuple) and value is None and len(key) == 2:
        return [key]
    elif isinstance(key, collections.Iterable) and not isinstance(key, (str, bytes)):
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
        self.engine = pipe.engine

    def count(self):
        return self.engine.execute(self.table.count()).scalar()

    def rows(self, desc=False):
        order_by = self.table.c.id.desc() if desc else self.table.c.id
        query = self.table.select().order_by(order_by)
        for row in windowed_query(self.engine, query, self.table.c.id):
            yield create_row(row)

    def items(self):
        for row in self.rows():
            yield row.key, row.value

    def keys(self):
        for row in self.rows():
            yield row.key

    def values(self):
        for row in self.rows():
            yield row.value

    def exists(self, key):
        query = sa.select([sa.exists().where(self.table.c.key == serkey(key))])
        return self.engine.execute(query).scalar()

    def getall(self, key, reverse=False):
        order_by = self.table.c.id.desc() if reverse else self.table.c.id
        query = self.table.select().where(self.table.c.key == serkey(key)).order_by(order_by)
        for row in windowed_query(self.engine, query, self.table.c.id):
            yield create_row(row)

    def get(self, key, default=Exception):
        rows = self.getall(key)
        try:
            row = next(rows)
        except StopIteration:
            if default is Exception:
                raise ValueError('%r not found.' % key)
            else:
                return default
        try:
            next(rows)
        except StopIteration:
            return row
        else:
            raise ValueError('%r returned more that one row.' % key)


class PipeErrors(object):
    def __init__(self, pipe):
        self.pipe = pipe
        self.bot = pipe.bot

    def __call__(self, key=None, reverse=False):
        if self.pipe.source:
            state = self.pipe.get_state()
            error = self.bot.models.errors.alias('error')
            table = self.pipe.source.table.alias('table')

            # Filter by key if provided
            if key is not None:
                row = self.pipe.source.last(key)
                if row is None:
                    return
                where = sa.and_(
                    error.c.state_id == state.id,
                    error.c.row_id == row.id,
                )
            else:
                where = error.c.state_id == state.id

            # Ordering
            if reverse:
                order_by = error.c.id.desc()
            else:
                order_by = error.c.id

            # Query if all tables stored in same database
            if self.pipe.samedb and self.pipe.source.samedb:
                query = (
                    sa.select([error, table], use_labels=True).
                    select_from(
                        error.
                        join(table, error.c.row_id == table.c.id)
                    ).
                    where(where).
                    order_by(order_by)
                )

                for row in windowed_query(self.bot.engine, query, table.c.id):
                    item = strip_prefix(row, 'error_')
                    item['row'] = create_row(strip_prefix(row, 'table_'))
                    yield item

            # Query if some tables are stored in external database
            else:
                query = error.select(where).order_by(order_by)
                for err in windowed_query(self.bot.engine, query, error.c.id):
                    query = table.select(table.c.id == err['row_id'])
                    row = self.pipe.source.engine.execute(query).first()
                    if row:
                        yield Row(err, row=create_row(row))

    def last(self, key=None):
        for err in self(key, reverse=True):
            return err

    def count(self):
        if self.pipe.source:
            error = self.bot.models.errors
            state = self.pipe.get_state()
            return self.bot.engine.execute(error.count(error.c.state_id == state.id)).scalar()
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
        now = datetime.datetime.utcnow()
        if 'retries' in error_or_row:
            error = error_or_row
            self.bot.engine.execute(
                self.bot.models.errors.update(sa.and_(
                    self.bot.models.errors.c.state_id == error.state_id,
                    self.bot.models.errors.c.row_id == error.row_id,
                )).values(
                    retries=self.bot.models.errors.c.retries + 1,
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
            self.bot.engine.execute(
                self.bot.models.errors.insert(),
                state_id=state.id,
                row_id=row.id,
                retries=0,
                traceback=message,
                created=now,
                updated=now,
            )

    def resolve(self, key=None):
        if self.pipe.source:
            state = self.pipe.get_state()
            error = self.bot.models.errors
            table = self.pipe.source.table

            if key is None:
                self.bot.engine.execute(error.delete(error.c.state_id == state.id))
            elif self.pipe.samedb and self.pipe.source.samedb:
                query = (
                    sa.select([error.c.id]).
                    select_from(table.join(error, table.c.id == error.c.row_id)).
                    where(sa.and_(error.c.state_id == state.id, table.c.key == serkey(key)))
                )

                if self.bot.engine.name == 'mysql':
                    # http://stackoverflow.com/a/45498/475477
                    query = sa.select([query.alias().c.id])

                self.bot.engine.execute(error.delete(error.c.id.in_(query)))
            else:
                query = table.select(table.c.key == serkey(key))
                row_ids = {row['id'] for row in self.pipe.source.engine.execute(query)}
                if row_ids:
                    query = error.delete(sa.and_(error.c.state_id == state.id, error.c.row_id.in_(row_ids)))
                    self.bot.engine.execute(query)


class Pipe(object):
    def __init__(self, bot, id, name, table, engine, samedb=True, compression=None):
        """

        Parameters
        ----------
        bot : databot.Bot
        id : int
            Primary key of this pipe from ``databot.db.models.pipes.id``.
        name : str
            Human readable pipe identifier.
        table: sqlalchemy.Table
            A table where data is stored.
        engine : sqlalchemy.Engine
        samedb : bool
            Identifies if this pipe is stored in same database as other pipes of ``bot``.

            If a pipe is stored in an external database, some queries will be executed in a bit different way.
        compression : databot.db.models.Compression, optional
            Data compression algorithm.
        """
        self.bot = bot
        self.id = id
        self.name = name
        self.table = table
        self.models = bot.models
        self.engine = engine
        self.samedb = samedb
        self.compression = compression
        self.data = PipeData(self)
        self.errors = PipeErrors(self)

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<databot.Pipe[%d]: %s>' % (self.id, self.name)

    def __enter__(self):
        self.bot.stack.append(self)
        return self

    def __exit__(self, *exc):
        self.bot.stack.pop()
        return False

    @property
    def source(self):
        if self.bot.stack:
            return self.bot.stack[-1]

    def get_state(self):
        source, target = self.source, self
        return get_or_create(self.bot.engine, self.bot.models.state, ['source_id', 'target_id'], dict(
            source_id=(source.id if source else None),
            target_id=target.id,
            offset=0,
        ))

    def is_filled(self):
        if self.source:
            table = self.source.table
            state = self.get_state()
            query = table.select(table.c.id > state.offset).limit(1)
            return len(self.source.engine.execute(query).fetchall()) > 0
        else:
            return False

    def append(self, key, value=None, conn=None, bulk=None, only_missing=False, progress=None, total=-1):
        """Append data to the pipe

        You can call this method in following ways::

            append(key)
            append(key, value)
            append((key, value))
            append([key, key, ...])
            append([(key, value), (key, value), ...])

        """
        conn = conn or self.engine

        # Progress bar
        rows = keyvalueitems(key, value)
        if progress and self.bot.verbosity == 1 and not self.bot.debug:
            rows = tqdm.tqdm(rows, progress, total, file=self.bot.output.output, leave=True)

        # Bulk insert start
        save_bulk = False
        if bulk is None:
            save_bulk = True
            bulk = BulkInsert(conn, self.table)

        # Append
        for key, value in rows:
            # Skip all items if key is None
            if key is not None and (not only_missing or not self.data.exists(key)):
                now = datetime.datetime.utcnow()
                bulk.append(serrow(key, value, created=now, compression=self.compression))

        # Bulk insert finish
        if save_bulk:
            bulk.save()

        return self

    def reset(self):
        state = self.get_state()
        self.bot.engine.execute(self.models.state.update(self.models.state.c.id == state.id), offset=0)
        return self

    def skip(self):
        state = self.get_state()
        source = self.source.table
        query = sa.select([source.c.id]).order_by(source.c.id.desc()).limit(1)
        offset = self.source.engine.execute(query).scalar()
        if offset:
            self.bot.engine.execute(self.models.state.update(self.models.state.c.id == state.id), offset=offset)
        return self

    def offset(self, value=None):
        """Move cursor to the specified offset.

        For example, let say you have 5 items in your pipe:

            [-----]

        Then you will get following state after calling offset:

            offset(1)   [*----]
            offset(-1)  [****-]
            offset(3)   [***--]
            offset(10)  [*****]
            offset(0)   [-----]

        """
        state = self.get_state()
        source = self.source.table

        offset = None

        if value:
            query = sa.select([source.c.id])
            if value > 0:
                query = query.where(source.c.id > state.offset).order_by(source.c.id.asc())
            else:
                query = query.where(source.c.id < state.offset).order_by(source.c.id.desc())
            query = query.limit(1).offset(abs(value) - 1)
            offset = self.source.engine.execute(query).scalar()
            if offset is None:
                if value > 0:
                    return self.skip()
                else:
                    return self.reset()
        if offset is not None:
            self.bot.engine.execute(self.models.state.update(self.models.state.c.id == state.id), offset=offset)
        return self

    def clean(self, age=None, now=None):
        if age:
            now = now or datetime.datetime.utcnow()
            timestamp = now - age
            query = self.table.delete(self.table.c.created <= timestamp)
        else:
            query = self.table.delete()
        self.engine.execute(query)
        return self

    def dedup(self):
        """Delete all records with duplicate keys except ones created first."""
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

        if self.engine.name == 'mysql':
            # http://stackoverflow.com/a/45498/475477
            query = sa.select([query.alias().c.id])

        self.engine.execute(self.table.delete(self.table.c.id.in_(query)))
        return self

    def compact(self):
        """Delete all records with duplicate keys except ones created last."""
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

        if self.engine.name == 'mysql':
            # http://stackoverflow.com/a/45498/475477
            query = sa.select([query.alias().c.id])

        self.engine.execute(self.table.delete(self.table.c.id.in_(query)))
        return self

    def compress(self):
        table = self.table
        rows = self.data.rows()
        if self.bot.verbosity == 1:
            rows = tqdm.tqdm(rows, total=self.data.count(), file=self.bot.output.output)
        for row in rows:
            if row.compression != Compression.gzip:
                data = serrow(row.key, row.value, created=row.created, compression=Compression.gzip)
                self.engine.execute(table.update().where(table.c.id == row['id']).values(data))

    def decompress(self):
        table = self.table
        rows = self.data.rows()
        if self.bot.verbosity == 1:
            rows = tqdm.tqdm(rows, total=self.data.count(), file=self.bot.output.output)
        for row in rows:
            if row.compression is not None:
                data = serrow(row.key, row.value, created=row.created, compression=None)
                self.engine.execute(table.update().where(table.c.id == row['id']).values(data))

    def count(self):
        """How much items left to process."""
        if self.source:
            state = self.get_state()
            table = self.source.table
            return self.source.engine.execute(table.count(table.c.id > state.offset)).scalar()
        else:
            return 0

    def rows(self):
        if self.source:
            table = self.source.table
            query = table.select(table.c.id > self.get_state().offset).order_by(table.c.id)
            for row in windowed_query(self.source.engine, query, table.c.id):
                yield create_row(row)
        else:
            raise RuntimeError('Unknown source pipe. Use `with source: target.rows()`.')

    def items(self):
        for row in self.rows():
            yield row.key, row.value

    def keys(self):
        for row in self.rows():
            yield row.key

    def values(self):
        for row in self.rows():
            yield row.value

    def last(self, key=None):
        if key:
            query = self.table.select().where(self.table.c.key == serkey(key)).order_by(self.table.c.id.desc())
        else:
            query = self.table.select().order_by(self.table.c.id.desc())

        row = self.engine.execute(query).first()
        return create_row(row) if row else None

    def call(self, handler):
        state = self.get_state()
        desc = '%s -> %s' % (self.source, self)

        if self.bot.retry:
            self.retry(handler)

        if self.bot.verbosity == 1 and not self.bot.debug:
            total = self.count()
            rows = tqdm.tqdm(self.rows(), desc, total, leave=True)
        else:
            rows = self.rows()

        def post_save():
            if row:
                self.bot.engine.execute(self.models.state.update(self.models.state.c.id == state.id), offset=row.id)

        pipe = BulkInsert(self.engine, self.table)
        errors = BulkInsert(self.bot.engine, self.bot.models.errors)

        if not self.bot.debug:
            pipe.post_save(post_save)

        n = 0
        row = None
        interrupt = None
        for row in rows:
            if self.bot.limit and n > self.bot.limit:
                break

            if self.bot.debug:
                self._verbose_append(handler, row, pipe, append=False)
            else:
                try:
                    if self.bot.verbosity > 1:
                        self._verbose_append(handler, row, pipe)
                    else:
                        self.append(handler(row), bulk=pipe)
                except KeyboardInterrupt as e:
                    interrupt = e
                    break
                except:
                    self.errors.report(row, traceback.format_exc(), errors)
            n += 1

        pipe.save(post_save=True)
        errors.save()

        if self.bot.verbosity > 1:
            print('%s, rows processed: %d' % (desc, n))

        if interrupt:
            raise interrupt

        return self

    def retry(self, handler):
        desc = '%s -> %s (retry)' % (self.source, self)

        if self.bot.verbosity == 1 and not self.bot.debug:
            total = self.errors.count()
            errors = tqdm.tqdm(self.errors(), desc, total, leave=True)
        else:
            errors = self.errors()

        def post_save():
            nonlocal error_ids
            if error_ids:
                self.bot.engine.execute(self.models.errors.delete(self.models.errors.c.id.in_(error_ids)))
                error_ids = []

        pipe = BulkInsert(self.engine, self.table)
        pipe.post_save(post_save)

        n = 0
        interrupt = None
        error_ids = []
        for error in errors:
            if self.bot.limit and n > self.bot.limit:
                break

            if self.bot.debug:
                self._verbose_append(handler, error.row, pipe, append=False)
                error_ids.append(error.id)
            else:
                try:
                    if self.bot.verbosity > 1:
                        self._verbose_append(handler, error.row, pipe)
                    else:
                        self.append(handler(error.row), bulk=pipe)
                except KeyboardInterrupt as e:
                    interrupt = e
                    break
                except:
                    self.errors.report(error, traceback.format_exc())
                else:
                    error_ids.append(error.id)
            n += 1

        pipe.save(post_save=True)

        if self.bot.verbosity > 1:
            print('%s, errors retried: %d' % (desc, n))

        if interrupt:
            raise interrupt

        return self

    def _verbose_append(self, handler, row, bulk, append=True):
        print('-' * 72)
        print('source: id=%d key=%r' % (row.id, row.key))
        for key, value in keyvalueitems(handler(row)):
            if append:
                self.append(key, value, bulk=bulk)
            self.bot.output.key_value(key, value, short=True)

    def export(self, path, **kwargs):
        if path.endswith('.jsonl'):
            jsonl.export(path, self, **kwargs)
        else:
            csv.export(path, self, **kwargs)

    def download(self, urls=None, **kwargs):
        """Download list of URLs and store downloaded content into a pipe.

        Parameters
        ----------
        urls : None or str or list or callable or databot.rowvalue.Row
            List of URLs to download.

            URL's can be provided in following ways:

            - `str` - string containing single URL.

            - `list` - list of strings where each string is a URL.

            - `None` - takes URLs from connected pipe's key field.

            - `databot.rowvalue.Row` - takes URLs from a specified location in a row.

              For example, code below will take all rows from `a` pipe and will take URL from `a.row.value.url`, which
              is `http://example.com`.

              .. code-block:: python

                 import databot

                 bot = databot.Bot()
                 a = bot.define('a').append([(1, {'url': 'http://example.com'})])
                 bot.define('b').download(a.row.value.url)

        delay : int
            Amount of seconds to delay between requests.

            By default delay is `bot.download_delay`.

        """
        kwargs.setdefault('delay', self.bot.download_delay)

        if isinstance(urls, str):
            urls = [urls]

        if isinstance(urls, list):
            fetch = download.download(self.bot.requests, urls, **kwargs)
            return self.append(next(fetch(url)) for url in urls)

        else:
            urls = urls or databot.row.key
            return self.call(download.download(self.bot.requests, urls, **kwargs))

    def select(self, key, value=None):
        return self.call(html.Select(key, value))
