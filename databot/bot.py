import sys
import pathlib
import sqlalchemy as sa
import argparse
import requests

import databot.db
import databot.pipes
from databot.db.models import Models
from databot.db.utils import strip_prefix, get_or_create, get_engine
from databot.db.migrations import Migrations
from databot.printing import Printer
from databot import commands


class Bot(object):

    def __init__(self, uri_or_engine='sqlite:///:memory:', *,
                 debug=False, retry=False, limit=0, verbosity=0, output=sys.stdout, models=None):
        self.path = pathlib.Path(sys.modules[self.__class__.__module__].__file__).resolve().parent
        self.engine = get_engine(uri_or_engine, self.path)
        self.models = models or Models(sa.MetaData(self.engine))
        self.output = Printer(self.models, output)
        self.conn = self.engine.connect()
        self.pipes = []
        self.pipes_by_name = {}
        self.pipes_by_id = {}
        self.stack = []
        self.options = []
        self.name = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        self.debug = debug
        self.retry = retry
        self.limit = limit
        self.verbosity = verbosity
        self.download_delay = None
        self.requests = requests.Session()
        self.commands = self._register_commands(commands.CommandsManager(self))

        self.migrations = Migrations(self.models, self.engine, self.output, verbosity=1)
        if self.migrations.has_initial_state():
            self.migrations.initialize()

    def define(self, name, uri_or_engine=None, compression=None):
        """Defines new pipe for storing data.

        Parameters
        ----------
        name : str
            Pipe name.
        uri_or_engine : str or sqlalchemy.Engine
            Database engine if this pipe used external database (other than defined in bot).
        compression : databot.db.models.Compression, optional
            Default compression for data values.
        """
        if name in self.pipes_by_name:
            raise ValueError('A pipe with "%s" name is already defined.' % name)

        if uri_or_engine is not None:
            samedb = False
            engine = get_engine(uri_or_engine, self.path)
            models = Models(sa.MetaData())
            migrations = Migrations(models, engine, self.output, verbosity=1)
            if migrations.has_initial_state():
                migrations.initialize()
            else:
                unapplied_migrations = migrations.unapplied()
                if unapplied_migrations:
                    self.output.error('\n'.join([
                        "External database '%s' from '%s' pipe has unapplied migrations.\n" % (engine.url, name),
                        "List of unapplied migrations:\n\n  - %s\n" % (
                            '\n  - '.join([f.__name__ for f in unapplied_migrations])
                        ),
                    ]))
                    sys.exit(1)

            defaults = dict(bot=self.name, pipe=name)
            internal = get_or_create(self.engine, self.models.pipes, ('pipe',), defaults)
            external = get_or_create(engine, models.pipes, ('pipe',), defaults)

            table_id = internal.id
            table_name = 't%d' % external.id

        else:
            samedb = True
            engine = self.engine
            models = self.models

            defaults = dict(bot=self.name, pipe=name)
            internal = get_or_create(engine, models.pipes, ('pipe',), defaults)

            table_id = internal.id
            table_name = 't%d' % internal.id

        table = models.get_data_table(table_name)
        table.create(engine, checkfirst=True)

        pipe = databot.pipes.Pipe(self, table_id, name, table, engine, samedb)
        self.pipes.append(pipe)
        self.pipes_by_name[name] = pipe
        self.pipes_by_id[pipe.id] = pipe

        return pipe

    def pipe(self, name):
        return self.pipes_by_name[name]

    def query_retry_pipes(self):
        errors, state, pipes = self.models.errors, self.models.state, self.models.pipes

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
            error = strip_prefix(row, 'error_')
            error['source'] = strip_prefix(row, 'source_')
            error['target'] = strip_prefix(row, 'target_')
            yield error

    def compact(self):
        for pipe in self.pipes:
            pipe.compact()

    def _register_commands(self, cmgr):
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
        cmgr.register('errors', commands.Errors)
        cmgr.register('sh', commands.Shell)
        cmgr.register('rename', commands.Rename)
        return cmgr

    def main(self, define=None, run=None, argv=None):
        parser = argparse.ArgumentParser()

        # Vorbosity levels:
        # 0 - no output
        # 1 - show progress bar
        parser.add_argument('-v', '--verbosity', type=int, default=1)

        sps = parser.add_subparsers(dest='command')

        cmgr = commands.CommandsManager(self, sps)
        cmgr.register('run', commands.Run, run)
        self._register_commands(cmgr)

        args = parser.parse_args(argv)

        self.verbosity = args.verbosity

        if args.command == 'migrate':
            cmgr.run(args.command, args, default='status')
        elif args.command or define:
            self.check_migrations(self.migrations)

            if define is not None:
                define(self)

            try:
                cmgr.run(args.command, args, default='status')
            except KeyboardInterrupt:
                pass

        return self

    def check_migrations(self, migrations):
        unapplied_migrations = migrations.unapplied()
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
