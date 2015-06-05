import json
import sys
import itertools
import pathlib
import datetime
import collections
import traceback

import dataset
import requests
import sqlalchemy


PROGRESS = 1
ERROR = 2
WARNING = 3
INFO = 4
DEBUG = 5


class DownloadErrror(Exception):
    pass


def loads(value):
    """Convert Python object to a primitive value for storing to database."""
    if value is None:
        return None
    else:
        return json.loads(value.decode('utf-8'))


def dumps(value):
    """Convert primitive value received from database to Python object."""
    return json.dumps(value).encode('utf-8')


def download(row):
    response = requests.get(row.key)
    if response.status_code == 200:
        yield row.key, response.text
    else:
        raise DownloadErrror('Error while downloading %s, returned status code was %s, response content:\n\n%s' % (
            row.key, response.status_code, response.text,
        ))


def export_csv(item):
    pass


def export(path):
    if path.endswith('.csv'):
        return export_csv
    else:
        raise ValueError("Don't know how to export %s." % path)


def html(handler, item):
    yield from handler(item, None)


def wrapper(handler, wrap):
    if wrap is None:
        return handler

    def wrapped(item):
        yield from wrap(handler, item)
    return wrapped


def dummy(row):
    pass


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
        return self.table.count()

    def rows(self):
        for row in self.table.all():
            yield Record(row, value=loads(row.value))

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
        self.errors = task.bot.errors
        self.engine = task.bot.conn.engine

    def __call__(self):
        if self.task.source:
            state = self.task.get_state()
            for error in self.errors.find(state_id=state.id):
                row = self.task.source.table.find_one(id=error.row_id)
                yield Record(error, row=Record(row, value=loads(row.value)))

    def count(self):
        if self.task.source:
            state = self.task.get_state()
            return self.errors.count(state_id=state.id)
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
        table = self.bot.state
        source, target = self.source, self
        params = dict(bot=self.bot.name, source=(source.name if source else None), target=target.name)
        data = dict(params, offset=0)
        data = table.find_one(**params) or Record(data, id=table.insert(data))
        return data

    def query(self):
        table = self.table.table
        state = self.get_state()
        return sqlalchemy.select([table.c.id]).where(table.c.id > state['offset'])

    def is_filled(self):
        engine = self.bot.conn.engine
        if self.source:
            state = self.get_state()
            table = self.source.table.table
            query = sqlalchemy.select([table.c.id]).where(table.c.id > state['offset'])
            return len(engine.execute(query.limit(1)).fetchall()) > 0
        else:
            return False

    def append(self, key, value=None, log=True):
        """Append data to the task's storage

        You can call this method in following ways::

            append(key, value)
            append([key, key, key])
            append([(key, value), (key, value), (key, value)])

        """
        if log:
            self.log(INFO, 'append...', end=' ')
        for key, value in keyvalueitems(key, value):
            now = datetime.datetime.utcnow()
            self.table.insert(dict(key=key, value=dumps(value), created=now))
        if log:
            self.log(INFO, 'done.')
        return self

    def clean(self, age=None, now=None):
        self.log(INFO, 'clean...', end=' ')
        engine = self.bot.conn.engine
        table = self.table.table
        if age:
            now = now or datetime.datetime.utcnow()
            timestamp = now - age
            query = table.delete().where(table.c.created <= timestamp)
        else:
            query = table.delete()
        engine.execute(query)
        self.log(INFO, 'done.')
        return self

    def dedup(self):
        """Delete all records with duplicate keys except ones created first."""
        self.log(INFO, 'dedup...', end=' ')
        engine = self.bot.conn.engine
        table = self.table.table
        agg = (
            sqlalchemy.select([table.c.key, sqlalchemy.func.min(table.c.id).label('id')]).
            group_by(table.c.key).
            having(sqlalchemy.func.count(table.c.id) > 1).
            alias('agg')
        )
        query = (
            sqlalchemy.select([table.c.id]).
            select_from(table.join(agg, sqlalchemy.and_(
                table.c.key == agg.c.key,
                table.c.id != agg.c.id,
            )))
        )
        self.delete(id for id, in engine.execute(query))
        self.log(INFO, 'done.')
        return self

    def compact(self):
        """Delete all records with duplicate keys except ones created last."""
        self.log(INFO, 'compact...', end=' ')
        engine = self.bot.conn.engine
        table = self.table.table
        agg = (
            sqlalchemy.select([table.c.key, sqlalchemy.func.max(table.c.id).label('id')]).
            group_by(table.c.key).
            having(sqlalchemy.func.count(table.c.id) > 1).
            alias('agg')
        )
        query = (
            sqlalchemy.select([table.c.id]).
            select_from(table.join(agg, sqlalchemy.and_(
                table.c.key == agg.c.key,
                table.c.id != agg.c.id,
            )))
        )
        self.delete(id for id, in engine.execute(query))
        self.log(INFO, 'done.')
        return self

    def delete(self, items, bufsize=524288):
        engine = self.bot.conn.engine
        table = self.table.table
        ids, size = [], 0
        for id in items:
            ids.append(id)
            size += len(str(id))
            if size > bufsize:
                engine.execute(table.delete().where(table.c.id.in_(ids)))
                ids, size = [], 0
        if ids:
            engine.execute(table.delete().where(table.c.id.in_(ids)))

    def count(self):
        """How much items left to process."""
        if self.source:
            state = self.get_state()
            engine = self.bot.conn.engine
            table = self.source.table.table
            query = sqlalchemy.select([table]).where(table.c.id > state['offset']).count()
            return engine.execute(query).scalar()
        else:
            return 0

    def rows(self):
        if self.source:
            state = self.get_state()
            engine = self.bot.conn.engine
            table = self.source.table.table
            query = sqlalchemy.select([table]).where(table.c.id > state['offset'])
            for item in engine.execute(query).fetchall():
                yield Record(item, value=loads(item.value))

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
        for i, row in enumerate(self.rows(), 1):
            try:
                for key, value in self.handler(row):
                    self.append(key, value, log=False)
            except:
                exception = traceback.format_exc()
                self.log('ERROR')
                self.log('ERROR', exception)
                now = datetime.datetime.utcnow()
                self.bot.errors.insert(dict(
                    state_id=state.id, row_id=row.id, retries=0, traceback=exception, created=now, updated=now,
                ))
            self.bot.state.update(dict(state, offset=row.id), ['id'])
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
                self.log('ERROR')
                self.log('ERROR', exception)
                self.bot.errors.update(dict(
                    state_id=error.state_id, row_id=error.row_id, retries=error.retries + 1,
                    traceback=exception, updated=datetime.datetime.utcnow(),
                ), ['state_id', 'row_id'])
            else:
                self.bot.errors.delete(id=error.id)
        self.log(INFO, 'done.')
        return self


class Record(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


class Bot(object):
    db = None

    def __init__(self):
        self.tasks = []
        self.tasks_by_name = {}
        self.stack = []
        self.path = pathlib.Path(sys.modules[self.__class__.__module__].__file__).resolve().parent
        self.conn = dataset.connect(self.db.format(path=self.path), row_type=Record)
        self.name = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        self.meta = self.conn['databotmeta']
        self.state = self.conn['databotstate']
        self.errors = self.conn['databoterrors']

    def define(self, name, handler=True, dburi=None, table=None, wrap=None):
        if name in self.tasks_by_name:
            raise ValueError('A task with "%s" name is already defined.' % name)

        if handler is True:
            method_name = 'task_' + name.lower().replace(' ', '_')
            handler = getattr(self, method_name)
        elif handler is None:
            handler = dummy

        params = dict(bot=self.name, task=name)
        data = self.meta.find_one(**params) or dict(params, id=self.meta.insert(params))
        table_name = 't%d' % data['id']
        if table_name not in self.conn.tables:
            table = self.conn.create_table(table_name)
            table.create_column('created', sqlalchemy.DateTime)
            table.create_column('key', sqlalchemy.Unicode)
            table.create_column('value', sqlalchemy.LargeBinary)
        else:
            table = self.conn[table_name]

        task = Task(self, data['id'], name, handler, table, wrap)
        self.tasks.append(task)
        self.tasks_by_name[name] = task

        return task

    def task(self, name):
        return self.tasks_by_name[name]

    def retry(self):
        pass # todo...

    def main(self, args=None):
        args = args or sys.argv
        self.init()
        if 'retry' in args:
            self.retry()
        else:
            self.run()

    def compact(self):
        for task in self.tasks:
            task.compact()

    def log(self, level, message='', end='\n'):
        print(message, end=end)
