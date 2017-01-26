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
                 debug=False, retry=False, limit=0, error_limit=None, verbosity=0, output=sys.stdout, models=None):
        self.path = pathlib.Path(sys.modules[self.__class__.__module__].__file__).resolve().parent
        self.engine = get_engine(uri_or_engine, self.path)
        self.models = models or Models(sa.MetaData(self.engine))
        self.output = Printer(self.models, output)
        self.conn = self.engine.connect()
        self.pipes = []
        self.pipes_by_name = {}
        self.pipes_by_id = {}
        self.name = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        self.debug = debug
        self.retry = retry
        self.limit = limit
        self.error_limit = error_limit
        self.verbosity = verbosity
        self.download_delay = None
        self.requests = requests.Session()
        self.commands = self._register_commands(commands.CommandsManager(self))

        self.migrations = Migrations(self.models, self.engine, self.output, verbosity=1)
        if self.migrations.has_initial_state():
            self.migrations.initialize()

    def define(self, name, uri_or_engine=None, compress=None):
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
                    dburl = uri_or_engine if isinstance(uri_or_engine, str) else engine.url
                    self.output.error('\n'.join([
                        "External database '%s' from '%s' pipe has unapplied migrations.\n" % (dburl, name),
                        'You need to run database migrations:\n',
                        '    databot %s migrate\n' % dburl,
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

        pipe = databot.pipes.Pipe(self, table_id, name, table, engine, samedb, compress)
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

    def _register_commands(self, cmgr, pipeline=None):
        cmgr._register('run', commands.Run, pipeline)
        cmgr._register('status', commands.Status)
        cmgr._register('select', commands.Select)
        cmgr._register('download', commands.Download)
        cmgr._register('skip', commands.Skip)
        cmgr._register('reset', commands.Reset)
        cmgr._register('offset', commands.Offset)
        cmgr._register('clean', commands.Clean)
        cmgr._register('compact', commands.Compact)
        cmgr._register('show', commands.Show)
        cmgr._register('tail', commands.Tail)
        cmgr._register('export', commands.Export)
        cmgr._register('resolve', commands.Resolve)
        cmgr._register('migrate', commands.Migrate)
        cmgr._register('errors', commands.Errors)
        cmgr._register('sh', commands.Shell)
        cmgr._register('rename', commands.Rename)
        cmgr._register('compress', commands.Compress)
        cmgr._register('decompress', commands.Decompress)
        return cmgr

    def main(self, pipeline=None, argv=None):
        parser = argparse.ArgumentParser()

        # Vorbosity levels:
        # 0 - no output
        # 1 - show progress bar
        parser.add_argument('-v', '--verbosity', type=int, default=1)

        sps = parser.add_subparsers(dest='command')

        cmgr = commands.CommandsManager(self, sps)
        self._register_commands(cmgr, pipeline)

        args = parser.parse_args(argv)

        self.verbosity = args.verbosity

        if args.command == 'migrate':
            cmgr._run(args.command, args, default='status')
        elif args.command or pipeline:
            self.check_migrations(self.migrations)

            if pipeline:
                for expr in pipeline.get('pipes', []):
                    expr._eval(self)

            try:
                cmgr._run(args.command, args, default='status')
            except KeyboardInterrupt:
                pass

        return self

    def check_migrations(self, migrations):
        unapplied_migrations = migrations.unapplied()
        if unapplied_migrations:
            self.output.error((
                'You need to run database migrations:\n'
                '\n'
                '    databot %s migrate\n'
                '\n'
                'List of unapplied migrations:\n\n  - %s\n'
            ) % (self.engine.url, '\n  - '.join([f.__name__ for f in unapplied_migrations])))
            sys.exit(1)
