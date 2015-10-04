import sys
import pathlib
import sqlalchemy as sa
import argparse

import databot.db
import databot.pipes
from databot.db import models
from databot.db.utils import create_row, get_or_create
from databot.logging import QUIET


class Bot(object):

    def __init__(self, uri_or_engine, verbosity=QUIET):
        self.pipes = []
        self.pipes_by_name = {}
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

    def define(self, name, dburi=None, table=None):
        if name in self.pipes_by_name:
            raise ValueError('A pipe with "%s" name is already defined.' % name)

        row = get_or_create(self.engine, models.pipes, ('bot', 'pipe'), dict(bot=self.name, pipe=name))
        table_name = 't%d' % row.id
        table = models.get_data_table(table_name, self.metadata)
        table.create(self.engine, checkfirst=True)

        pipe = databot.pipes.Pipe(self, row.id, name, table)
        self.pipes.append(pipe)
        self.pipes_by_name[name] = pipe

        return pipe

    def pipe(self, name):
        return self.pipes_by_name[name]

    def query_retry_pipes(self):
        errors, state, pipes = models.errors, models.state, models.pipes

        error = errors.alias('error')
        source = pipes.alias('source')
        target = pipes.alias('target')

        query = (
            sa.select([error, source, target], use_labels=True).
            select_from(
                error.
                join(state).
                join(source, state.c.source_id == source.c.id).
                join(target, state.c.target_id == target.c.id)
            )
        )

        for row in self.engine.execute(query):
            error = create_row(row, 'error_')
            error['source'] = create_row(row, 'source_')
            error['target'] = create_row(row, 'target_')
            yield error

    def main(self, args=None):
        args = args or sys.argv
        self.init()
        if 'retry' in args:
            self.retry()
        else:
            self.run()

    def compact(self):
        for pipe in self.pipes:
            pipe.compact()

    def log(self, level, message='', end='\n'):
        if self.verbosity >= level:
            print(message, end=end)

    def status(self):
        pipes = models.pipes
        target = pipes.alias('target')
        pipes = {t.id: t for t in self.pipes}
        lines = []

        lines.append('%5s  %6s %9s  %s' % ('id', '', 'rows', 'source'))
        lines.append('%5s  %6s %9s  %s' % ('', 'errors', 'left', '  target'))
        lines.append(None)
        for source in self.pipes:
            lines.append('%5d  %6s %9d  %s' % (source.id, '', source.data.count(), source))

            query = sa.select([models.state.c.target_id]).where(models.state.c.source_id == source.id)
            for target_id, in self.engine.execute(query):
                if target_id in pipes:
                    target = pipes[target_id]
                    with source:
                        lines.append('%5s  %6s %9d    %s' % ('', target.errors.count(), target.count(), target))

            lines.append(None)

        lenght = max(map(len, filter(None, lines)))
        for line in lines:
            if line is None:
                print('-' * lenght)
            else:
                print(line)

    def try_(self, args):
        source_id, target_id = map(int, args.state.split('/'))

        pipes = {t.id: t for t in self.pipes}

        source = pipes[source_id]

        from .pipes import keyvalueitems
        from databot.handlers import html

        method = html.Select(*args.argument)
        for row in source.data.rows():
            for key, value in keyvalueitems(method(row)):
                print(key, value)
            break

    def argparse(self, argv, define=None, run=None):
        parser = argparse.ArgumentParser()

        # Vorbosity levels:
        # 0 - no output
        # 1 - show progress bar
        parser.add_argument('-v', '--verbosity', type=int, default=1)

        sps = parser.add_subparsers(dest='command')

        sp = sps.add_parser('status')

        sp = sps.add_parser('run')
        sp.add_argument('--retry', action='store_true', default=False, help="Retry failed rows.")
        sp.add_argument('-d', '--debug', action='store_true', default=False, help="Run in debug and verbose mode.")

        sp = sps.add_parser('try')
        sp.add_argument('state', type=str, help="State id, for example: 1/2")
        sp.add_argument('method', type=str, help="Example: select")
        sp.add_argument('argument', type=str, nargs='*')

        self.args = args = parser.parse_args(argv)

        if define is not None:
            define(self)

        if args.command == 'run':
            if run is not None:
                run(self)
        elif args.command == 'try':
            self.try_(args)
        else:
            self.status()

        return self

    @property
    def data(self):
        return self.stack[-1].data

    def select(self, key, value=None):
        pipe = self.stack.pop()
        try:
            pipe.select(key, value)
        finally:
            self.stack.append(pipe)
        return self

    def dedup(self):
        return self.stack[-1].dedup()
