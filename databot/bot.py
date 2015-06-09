import sys
import pathlib
import sqlalchemy as sa

import databot.db
import databot.tasks
import databot.handlers.dummy
from databot.db import models
from databot.db.utils import get_or_create
from databot.logging import QUIET


class Bot(object):
    db = None

    def __init__(self, verbosity=QUIET):
        self.tasks = []
        self.tasks_by_name = {}
        self.stack = []
        self.path = pathlib.Path(sys.modules[self.__class__.__module__].__file__).resolve().parent
        self.engine = sa.create_engine(self.db.format(path=self.path))
        self.metadata = sa.MetaData()
        self.conn = self.engine.connect()
        self.name = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        self.verbosity = verbosity
        models.metadata.create_all(self.engine, checkfirst=True)

    def define(self, name, handler=True, dburi=None, table=None, wrap=None):
        if name in self.tasks_by_name:
            raise ValueError('A task with "%s" name is already defined.' % name)

        if handler is True:
            method_name = 'task_' + name.lower().replace(' ', '_')
            handler = getattr(self, method_name)
        elif handler is None:
            handler = databot.handlers.dummy.handler

        row = get_or_create(self.engine, models.tasks, ('bot', 'task'), dict(bot=self.name, task=name))
        table_name = 't%d' % row.id
        table = models.get_data_table(table_name, self.metadata)
        table.create(self.engine, checkfirst=True)

        task = databot.tasks.Task(self, row.id, name, handler, table, wrap)
        self.tasks.append(task)
        self.tasks_by_name[name] = task

        return task

    def task(self, name):
        return self.tasks_by_name[name]

    def retry(self):
        pass  # todo...

    def query_retry_tasks(self):
        pass

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
        if self.verbosity >= level:
            print(message, end=end)
