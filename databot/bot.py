import sys
import pathlib
import sqlalchemy as sa
import argparse

import databot.db
import databot.pipes
from databot.db import models
from databot.db.utils import Row, create_row, get_or_create
from databot.db.serializers import loads
from databot.logging import QUIET
from databot.printing import Printer


class Bot(object):

    def __init__(self, uri_or_engine, verbosity=QUIET):
        self.printer = Printer()
        self.pipes = []
        self.pipes_by_name = {}
        self.pipes_by_id = {}
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
        border = '='
        for line in lines:
            if line is None:
                print(border * lenght)
                border = '-'
            else:
                print(line)

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

        sp = sps.add_parser('select')
        sp.add_argument('source', type=str, help="Source id, for example: 1")
        sp.add_argument('query', type=str, help='Selector query.')
        sp.add_argument('-k', '--key', type=str, help="Try on specific source key.")
        sp.add_argument('-t', '--table', action='store_true', default=False, help="Print ascii table.")

        sp = sps.add_parser('download')
        sp.add_argument('url', type=str)
        sp.add_argument('-x', '--exclude', type=str, help="Exclude items from value.")
        sp.add_argument('-a', '--append', type=str, help="Append downloaded content to specified pipe.")

        sp = sps.add_parser('show')
        sp.add_argument('pipe', type=str, help="Pipe id, for example: 1 or my-pipe")
        sp.add_argument('key', type=str, nargs='?', help="If key is not provided, last item will be shown.")
        sp.add_argument('-x', '--exclude', type=str, help="Exclude items from value.")

        sp = sps.add_parser('tail')
        sp.add_argument('pipe', type=str, help="Pipe id, for example: 1")
        sp.add_argument('-n', type=int, dest='limit', default=10, help="Number of rows to show.")
        sp.add_argument('-x', '--exclude', type=str, help="Exclude items from value.")
        sp.add_argument('-t', '--table', action='store_true', default=False, help="Print ascii table.")

        sp = sps.add_parser('compact')
        sp.add_argument('pipe', type=str, nargs='?', help="Pipe id, for example: 1 or my-pipe")

        self.args = args = parser.parse_args(argv)

        if define is not None:
            define(self)

        if args.command == 'run':
            if run is not None:
                run(self)
        elif args.command == 'select':
            self.command_select(args)
        elif args.command == 'download':
            self.command_download(args)
        elif args.command == 'show':
            self.show(args)
        elif args.command == 'tail':
            self.tail(args)
        elif args.command == 'compact':
            if args.pipe:
                self.get_pipe_from_string(args.pipe).compact()
            else:
                self.compact()
        else:
            self.status()

        return self

    def get_pipe_from_string(self, name):
        if name.isdigit():
            pipe = self.pipes_by_id[int(name)]
        else:
            pipe = self.pipes_by_name.get(name)
            pipe = pipe or self.pipes_by_name[name.replace('-', ' ')]
        return pipe

    def get_last_row(self, pipe, key=None):
        if key:
            query = pipe.table.select().where(pipe.table.c.key == key).order_by(pipe.table.c.id.desc())
        else:
            query = pipe.table.select().order_by(pipe.table.c.id.desc())

        return self.engine.execute(query).first()

    def command_select(self, args):
        from databot.pipes import keyvalueitems
        from databot.handlers import html

        source = self.get_pipe_from_string(args.source)
        selector = html.Select([args.query])
        row = self.get_last_row(source, args.key)
        if row:
            row = Row(row, value=loads(row.value))
            rows = keyvalueitems(selector(row))
            if args.table:
                self.printer.print_table([Row(key=key, value=value) for key, value in rows])
            else:
                for key, value in rows:
                    self.printer.print_key_value(key, value)
        else:
            print('Not found.')

    def command_download(self, args):
        from databot.handlers import download

        exclude = args.exclude.split(',') if args.exclude else None
        key, value = next(download.download(Row(key=args.url, value=None)))
        self.printer.print_key_value(key, value, exclude=exclude)

        if args.append:
            pipe = self.get_pipe_from_string(args.append)
            pipe.append(key, value)

    def show(self, args):
        pipe = self.get_pipe_from_string(args.pipe)
        row = self.get_last_row(pipe, args.key)

        if row:
            exclude = args.exclude.split(',') if args.exclude else None
            self.printer.print_key_value(row.key, loads(row.value), exclude=exclude)
        else:
            print('Not found.')

    def tail(self, args):
        pipe = self.get_pipe_from_string(args.pipe)
        rows = self.engine.execute(pipe.table.select().order_by(pipe.table.c.id.desc()).limit(args.limit))
        rows = list(rows)
        if rows:
            exclude = args.exclude.split(',') if args.exclude else None
            if args.table:
                self.printer.print_table([Row(row, value=loads(row.value)) for row in reversed(rows)], exclude=exclude)
            else:
                for row in reversed(rows):
                    self.printer.print_key_value(row.key, loads(row.value), exclude=exclude)
        else:
            print('Not found.')

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
