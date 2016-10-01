import sqlalchemy as sa
import funcy

from databot import parsevalue
from databot.exceptions import PipeNameError


class CommandsManager(object):

    def __init__(self, bot, sps):
        self.bot = bot
        self.sps = sps
        self.commands = {}

    def register(self, name, Cmd, *args, **kwargs):
        assert name not in self.commands
        parser = self.sps.add_parser(name)
        command = Cmd(self.bot)
        command.init(*args, **kwargs)
        command.add_arguments(parser)
        self.commands[name] = command

    def run(self, name, args, default=None):
        command = self.commands[name if name else default]
        command.run(args)


class Command(object):

    def __init__(self, bot):
        self.bot = bot

    def init(self):
        pass

    def add_arguments(self, parser):
        pass

    def run(self, args):
        raise NotImplementedError

    def pipe(self, name):
        if name.isdigit():
            pipe = self.bot.pipes_by_id.get(int(name))
        else:
            pipe = self.bot.pipes_by_name.get(name)
            pipe = pipe or self.bot.pipes_by_name.get(name.replace('-', ' '))
        if pipe is None:
            raise PipeNameError("Pipe '%s' not found." % name)
        else:
            return pipe

    def debug(self, value):
        self.bot.output.debug(value)

    def info(self, value):
        self.bot.output.info(value)


class Run(Command):

    def init(self, func):
        self.func = func

    def add_arguments(self, parser):
        parser.add_argument('options', type=str, nargs='*', help="Run options if not specified everythig will be run.")
        parser.add_argument('--retry', action='store_true', default=False, help="Retry failed rows.")
        parser.add_argument('-d', '--debug', action='store_true', default=False, help="Run in debug and verbose mode.")

    def run(self, args):
        self.bot.debug = args.debug
        self.bot.retry = args.retry

        if self.func is not None:
            self.bot.options = args.options
            try:
                self.func(self.bot)
            except KeyboardInterrupt:
                pass


class Status(Command):

    def run(self, args):
        self.bot.output.status(self.bot)


class Select(Command):

    def add_arguments(self, parser):
        parser.add_argument('source', type=str, help="Source id, for example: 1")
        parser.add_argument('query', type=str, help='Selector query.')
        parser.add_argument('-k', '--key', type=str, help="Try on specific source key.")
        parser.add_argument('-t', '--table', action='store_true', default=False, help="Print ascii table.")

    def run(self, args):
        import ast

        from databot.pipes import keyvalueitems
        from databot.handlers import html
        from databot.db.utils import Row

        if args.query and args.query[0] in ('[', '{', '"', "'"):
            query = ast.literal_eval(args.query)
        else:
            query = [args.query]

        source = self.pipe(args.source)
        selector = html.Select(query)
        row = source.last(args.key)
        if row:
            rows = keyvalueitems(selector(row))
            if args.table:
                self.bot.output.table([Row(key=key, value=value) for key, value in rows])
            else:
                for key, value in rows:
                    self.bot.output.key_value(key, value)
        else:
            self.info('Not found.')


class Download(Command):

    def add_arguments(self, parser):
        parser.add_argument('url', type=str)
        parser.add_argument('-x', '--exclude', type=str, help="Exclude items from value.")
        parser.add_argument('-a', '--append', type=str, help="Append downloaded content to specified pipe.")

    def run(self, args):
        from databot import row
        from databot.db.utils import Row
        from databot.handlers import download

        exclude = args.exclude.split(',') if args.exclude else None
        key, value = next(download.download(row.key)(Row(key=args.url, value=None)))
        self.bot.output.key_value(key, value, exclude=exclude)

        if args.append:
            self.pipe(args.append).append(key, value)


class Skip(Command):

    def add_arguments(self, parser):
        parser.add_argument('source', type=str, help="Source pipe.")
        parser.add_argument('target', type=str, help="Target pipe.")

    def run(self, args):
        with self.pipe(args.source):
            self.pipe(args.target).skip()
        self.bot.output.status(self.bot)


class Reset(Command):

    def add_arguments(self, parser):
        parser.add_argument('source', type=str, help="Source pipe.")
        parser.add_argument('target', type=str, help="Target pipe.")

    def run(self, args):
        with self.pipe(args.source):
            self.pipe(args.target).reset()
        self.bot.output.status(self.bot)


class Offset(Command):

    def add_arguments(self, parser):
        parser.add_argument('source', type=str, help="Source pipe.")
        parser.add_argument('target', type=str, help="Target pipe.")
        parser.add_argument('offset', type=int, help="Relative offset.")

    def run(self, args):
        with self.pipe(args.source):
            self.pipe(args.target).offset(args.offset)
        self.bot.output.status(self.bot)


class Clean(Command):

    def add_arguments(self, parser):
        parser.add_argument('pipe', type=str, help="Clean all data from specified pipe.")

    def run(self, args):
        self.pipe(args.pipe).clean()
        self.bot.output.status(self.bot)


class Compact(Command):

    def add_arguments(self, parser):
        parser.add_argument('pipe', type=str, nargs='?', help="Pipe id, for example: 1 or my-pipe")

    def run(self, args):
        if args.pipe:
            self.pipe(args.pipe).compact()
        else:
            self.bot.compact()
        self.bot.output.status(self.bot)


class Show(Command):

    def add_arguments(self, parser):
        parser.add_argument('pipe', type=str, help="Pipe id, for example: 1 or my-pipe")
        parser.add_argument('key', type=str, nargs='?', help="If key is not provided, last item will be shown.")
        parser.add_argument('-x', '--exclude', type=str, help="Exclude items from value.")
        parser.add_argument('-b', '--in-browser', action='store_true', help="Show value content in browser.")

    def run(self, args):
        import ast
        import webbrowser
        import tempfile

        key = ast.literal_eval(args.key) if args.key else None
        row = self.pipe(args.pipe).last(key)

        if row:
            exclude = args.exclude.split(',') if args.exclude else None
            self.bot.output.key_value(row.key, row.value, exclude=exclude)
            if args.in_browser:
                with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as f:
                    f.write(row.value['content'])
                webbrowser.open(f.name)
        else:
            print('Not found.')


class Tail(Command):

    def add_arguments(self, parser):
        parser.add_argument('pipe', type=str, help="Pipe id, for example: 1")
        parser.add_argument('-n', type=int, dest='limit', default=10, help="Number of rows to show.")
        parser.add_argument('-x', '--exclude', type=str, help="Exclude fields from row.")
        parser.add_argument('-i', '--include', type=str, help="Include fields from row.")
        parser.add_argument('-t', '--table', action='store_true', default=False, help="Print ascii table.")

    def run(self, args):
        from databot.db.utils import create_row

        pipe = self.pipe(args.pipe)
        rows = pipe.engine.execute(pipe.table.select().order_by(pipe.table.c.id.desc()).limit(args.limit))
        rows = [create_row(row) for row in reversed(list(rows))]

        if rows:
            exclude = args.exclude.split(',') if args.exclude else None
            include = args.include.split(',') if args.include else None
            if args.table:
                self.bot.output.table(rows, exclude=exclude, include=include)
            else:
                for row in rows:
                    self.bot.output.key_value(row.key, row.value, exclude=exclude)
        else:
            print('Not found.')


class Export(Command):

    def add_arguments(self, parser):
        parser.add_argument('pipe', type=str, help="Pipe id, for example: 1")
        parser.add_argument('path', type=str, help="Path CSV file to export to (will be overwritten).")
        parser.add_argument('-x', '--exclude', type=str, help="Exclude columns.")
        parser.add_argument('-i', '--include', type=str, help="Include columns.")
        parser.add_argument('-a', '--append', action='store_true', help="Append existing file.")
        parser.add_argument('--no-header', dest='header', action='store_false', help="Do not write header.")

    def run(self, args):
        from databot.exporters import csv

        pipe = self.pipe(args.pipe)
        exclude = set(args.exclude.split(',') if args.exclude else [])
        include = args.include.split(',') if args.include else None
        csv.export(args.path, pipe, exclude=exclude, include=include, append=args.append, header=args.header)


class Resolve(Command):

    def add_arguments(self, parser):
        parser.add_argument('source', type=str, help="Source pipe id or name.")
        parser.add_argument('target', type=str, help="Target pipe id or name to mark errors as resolved.")
        parser.add_argument('key', type=str, nargs='?',
                            help="Mark as resolve only specific key if not specified marks all errors as resolved.")

    def run(self, args):
        import ast

        key = ast.literal_eval(args.key) if args.key else None
        with self.pipe(args.source):
            self.pipe(args.target).errors.resolve(key)
        self.bot.output.status(self.bot)


class Migrate(Command):

    def run(self, args):
        self.bot.migrations.migrate()


class Errors(Command):

    def add_arguments(self, parser):
        parser.add_argument('source', type=str, help="Source pipe id or name.")
        parser.add_argument('target', type=str, help="Target pipe id or name.")
        parser.add_argument('key', type=str, nargs='?',
                            help="Show errors for specific key only, if not specified show last error.")
        parser.add_argument('-n', type=int, dest='limit', default=1, help="Number of errors to show.")
        parser.add_argument('-x', '--exclude', type=str, help="Exclude items from value.")

    def run(self, args):
        key = parsevalue.parse(args.key)
        exclude = args.exclude.split(',') if args.exclude else None
        with self.pipe(args.source):
            errors = self.pipe(args.target).errors(key, reverse=True)
            errors = funcy.take(args.limit, errors)
            self.bot.output.errors(errors, exclude)


class Shell(Command):

    def run(self, args):
        import IPython
        import funcy

        from databot.shell import ShellHelper

        bot = self.bot  # noqa
        pipe = ShellHelper(self.bot)  # noqa
        take = funcy.compose(list, funcy.take)  # noqa

        IPython.embed(header='\n'.join([
            'Available objects and functions:',
            '  bot - databot.Bot instance',
            '  pipe - helper for accessing pipe instances, type `pipe.<TAB>` to access a pipe',
            '  take - takes n items from an iterable, example: take(10, pipe.mypipe.data.values())',
        ]))


class Rename(Command):

    def add_arguments(self, parser):
        parser.add_argument('old', type=str, help="Old pipe name.")
        parser.add_argument('new', type=str, help="New pipe name.")

    def run(self, args):
        pipes = self.bot.models.pipes
        pipe_id = sa.select([pipes.c.id], pipes.c.pipe == args.old).execute().scalar()
        pipes.update().where(pipes.c.id == pipe_id).values(pipe=args.new).execute()
