import sys
import pathlib
import sqlalchemy as sa

import databot.cli
import databot.db
import databot.tasks
import databot.handlers.dummy
from databot.db import models
from databot.db.utils import create_row, get_or_create
from databot.logging import QUIET


class Bot(object):

    def __init__(self, uri_or_engine, verbosity=QUIET):
        self.tasks = []
        self.tasks_by_name = {}
        self.stack = []
        self.path = pathlib.Path(sys.modules[self.__class__.__module__].__file__).resolve().parent
        if isinstance(uri_or_engine, str):
            self.engine = sa.create_engine(uri_or_engine.format(path=self.path))
        else:
            self.engine = uri_or_engine
        self.metadata = sa.MetaData()
        self.conn = self.engine.connect()
        self.name = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        self.verbosity = verbosity
        models.metadata.create_all(self.engine, checkfirst=True)

    def define(self, name, handler=True, dburi=None, table=None, wrap=None, data=False):
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

        task = databot.tasks.Task(self, row.id, name, handler, table, wrap, data)
        self.tasks.append(task)
        self.tasks_by_name[name] = task

        return task

    def task(self, name=None, code=None):
        if code:
            for task in self.tasks:
                if 't%d' % task.id == code:
                    return task
            raise AttributeError('Unknown tasks with code "%s"' % code)
        else:
            return self.tasks_by_name[name]

    def query_tasks_state(self):
        state = models.state.alias('state')
        source = models.tasks.alias('source')
        target = models.tasks.alias('target')

        query = (
            sa.select([state, source, target], use_labels=True).
            select_from(
                state.
                join(source, state.c.source_id == source.c.id).
                join(target, state.c.target_id == target.c.id)
            )
        )

        for row in self.engine.execute(query):
            record = create_row(row, 'state_')
            record['source'] = create_row(row, 'source_')
            record['target'] = create_row(row, 'target_')
            yield record

    def main(self, args=None):
        args = args or sys.argv[1:]
        cli = databot.cli.Cli(self, args)
        cli.main()

    def compact(self):
        for task in self.tasks:
            task.compact()

    def log(self, level, message='', end='\n'):
        if self.verbosity >= level:
            print(message, end=end)
