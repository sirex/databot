import sys
import pathlib
import sqlalchemy as sa
import argparse

import databot.db
import databot.pipes
from databot.db import models
from databot.db.utils import create_row, get_or_create
from databot.db.migrations import Migrations
from databot.printing import Printer
from databot import commands


class Bot(object):
    """
    Attributes:
        metadata (sqlalchemy.MetaData): sqlalchemy metadata for data tables
            We use custom metadata just for data tables, because data tables can span multiple databases so we need
            multiple metadatas for each database.

            All other static tables use ``models.metadata``.
    """

    def __init__(self, uri_or_engine, verbosity=0, output=sys.stdout):
        self.output = Printer(output)
        self.pipes = []
        self.pipes_by_name = {}
        self.pipes_by_id = {}
        self.stack = []
        self.options = []
        self.path = pathlib.Path(sys.modules[self.__class__.__module__].__file__).resolve().parent
        if isinstance(uri_or_engine, str):
            self.engine = sa.create_engine(uri_or_engine.format(path=self.path))
        else:
            self.engine = uri_or_engine
        self.metadata = sa.MetaData()
        self.conn = self.engine.connect()
        self.name = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        self.verbosity = verbosity
        self.download_delay = None

        self.migrations = Migrations(models.metadata, self.engine, self.output, verbosity=1)
        if self.migrations.has_initial_state():
            self.migrations.initialize()

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
        self.pipes_by_id[pipe.id] = pipe

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

    def compact(self):
        for pipe in self.pipes:
            pipe.compact()

    def main(self, define=None, run=None, argv=None):
        parser = argparse.ArgumentParser()

        # Vorbosity levels:
        # 0 - no output
        # 1 - show progress bar
        parser.add_argument('-v', '--verbosity', type=int, default=1)

        sps = parser.add_subparsers(dest='command')

        cmgr = commands.CommandsManager(self, sps)
        cmgr.register('run', commands.Run, run)
        cmgr.register('status', commands.Status)
        cmgr.register('select', commands.Select)
        cmgr.register('download', commands.Download)
        cmgr.register('skip', commands.Skip)
        cmgr.register('reset', commands.Reset)
        cmgr.register('offset', commands.Offset)
        cmgr.register('clean', commands.Clean)
        cmgr.register('compact', commands.Compact)
        cmgr.register('show', commands.Show)
        cmgr.register('tail', commands.Tail)
        cmgr.register('export', commands.Export)
        cmgr.register('resolve', commands.Resolve)
        cmgr.register('migrate', commands.Migrate)

        self.args = args = parser.parse_args(argv)

        if args.command == 'migrate':
            cmgr.run(args.command, args, default='status')
        else:
            self.check_migrations(args)

            if define is not None:
                define(self)

            cmgr.run(args.command, args, default='status')

        return self

    def check_migrations(self, args):
        unapplied_migrations = self.migrations.unapplied()
        if unapplied_migrations:
            self.output.error((
                'You need to run database migrations:\n'
                '\n'
                '    %s migrate\n'
                '\n'
                'List of unapplied migrations:\n\n  - %s\n'
            ) % (sys.argv[0], '\n  - '.join([f.__name__ for f in unapplied_migrations])))
            sys.exit(1)

    def run(self, name):
        if self.options:
            return name in self.options
        else:
            return True
