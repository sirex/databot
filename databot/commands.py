import sqlalchemy as sa
import funcy

from databot import parsevalue
from databot.exceptions import PipeNameError
from databot.expressions.base import Expression
from databot.runner import run_all_tasks


class CommandsManager(object):

    def __init__(self, bot, sps=None):
        self._bot = bot
        self._sps = sps
        self._commands = {}

    def _register(self, name, Cmd, *args, **kwargs):
        assert name not in self._commands
        command = Cmd(self._bot)
        command.init(*args, **kwargs)
        if self._sps:
            parser = self._sps.add_parser(name)
            command.add_arguments(parser)
        self._commands[name] = command

        if hasattr(command, 'call'):
            setattr(self, name, command.call)

    def _run(self, name, args, default=None):
        command = self._commands[name if name else default]
        command.run(args)


class Command(object):

    def __init__(self, bot):
        self.bot = bot

    def init(self):
        pass

    def add_arguments(self, parser):
        pass

    def run(self, args):
        raise NotImplementedError  # pragma: no cover

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

    def init(self, pipeline):
        self.pipeline = pipeline

    def add_arguments(self, parser):
        parser.add_argument('source', type=str, nargs='?', help="Source pipe, to read data from.")
        parser.add_argument('target', type=str, nargs='?', help="Target pipe, to write data to.")
        parser.add_argument('-r', '--retry', action='store_true', default=False, help=(
            "Retry failed rows, before running the pipeline."
        ))
        parser.add_argument('-d', '--debug', action='store_true', default=False, help="Run in debug and verbose mode.")
        parser.add_argument('-l', '--limit', type=str, default='1,0', help=(
            "Limit number of iteratios for all pipes. Multiple limits can be specified, for example 1,0 will run whole "
            "pipleine once with limit=1, and then runs pipline again with limit=0."
        ))
        parser.add_argument('-f', '--fail', type=int, default=None, const=0, nargs='?', action='store', help=(
            "Stop scraping after specified number of errors."
        ))

    def run(self, args):
        source = self.bot.pipe(args.source) if args.source else None
        target = self.bot.pipe(args.target) if args.target else None
        tasks = self.pipeline.get('tasks', []) if self.pipeline else []
        limits = [int(x) for x in map(str.strip, args.limit.split(',')) if x]
        self.call(tasks, source, target, debug=args.debug, retry=args.retry, limits=limits,
                  error_limit=args.fail)

    def call(self, tasks, source=None, target=None, *, debug=False, retry=False, limits=(1, 0), error_limit=None):
        self.bot.debug = debug
        self.bot.retry = retry

        if tasks:
            self.info('Validating pipeline.')

            # Validate tasks
            assert isinstance(tasks, (list, tuple))
            for expr in tasks:
                if not isinstance(expr, Expression):
                    raise RuntimeError("Unknown task type: %r" % expr)

                task = expr._stack[0]
                if task.name != 'task':
                    raise RuntimeError("Unknown function: %r" % task.name)

            # Reset tasks
            for expr in tasks:
                expr._reset()

            # Run tasks
            for limit in limits:
                self.info('')
                self.info('Run pipeline (limit=%r).' % limit)
                self.bot.limit = limit
                self.bot.error_limit = 1 if limit and error_limit is None else error_limit
                run_all_tasks(self.bot, tasks, source, target)


class Status(Command):

    def run(self, args):
        self.call()

    def call(self):
        """Show status of all pipes."""
        self.bot.output.status(self.bot)


class Select(Command):

    def add_arguments(self, parser):
        parser.add_argument('source', type=str, help="Source pipe.")
        parser.add_argument('target', type=str, nargs='?', help="Target pipe.")
        parser.add_argument('-q', '--query', type=str, help='Selector query.')
        parser.add_argument('-k', '--key', type=str, help="Try on specific source key.")
        parser.add_argument('-t', '--table', action='store_true', default=False, help="Print ascii table.")
        parser.add_argument('-x', '--export', type=str, help="Export all data to specified file.")
        parser.add_argument('-e', '--errors', action='store_true', help="Read data from target's errors.")

    def run(self, args):
        import ast

        source = self.pipe(args.source)
        target = self.pipe(args.target) if args.target else None

        if args.query and args.query[0] in ('[', '{', '(', '"', "'"):
            query = ast.literal_eval(args.query)
        else:
            query = [args.query]

        self.call(source, target, query, key=args.key, table=args.table, export=args.export, errors=args.errors)

    def call(self, source, target=None, query=None, key=None, table=False, export=None, errors=False, raw=False,
             progressbar=True, check=True):
        """Select structured data from an unstructured source.

        Parameters
        ----------
        source : databot.pipes.Pipe
            Source pipe. Should be a pipe with downloaded HTML pages.
        target : databot.pipes.Pipe
            Target pipe.
        query : list | dict | tuple
            Query for selecting data.
        key : str, optional
            Use specific key from source pipe.
        table : bool, optional
            Output results as table.
        export : str, optional
            Export all data to specified file.
        errors : bool, optional
            Read data frm target's errors.
        raw : bool, optional
            Return raw python objects instead of printing results to stdout.
        progressbar : bool, optional
            Show progress bar if export is given.
        check : bool or string, optional
            See ``databot.handlers.html.Select.__init__``.

        """
        import tqdm

        from databot.pipes import keyvalueitems
        from databot.handlers import html
        from databot.db.utils import Row

        assert query is not None

        if isinstance(query, tuple) and len(query) == 2:
            selector = html.Select(query[0], query[1], check=check)
        else:
            selector = html.Select(query, check=check)

        if errors:
            assert target
            pipe = target(source).errors
        elif target:
            pipe = target(source)
        else:
            pipe = source

        if export:
            from databot.exporters import csv, jsonl

            def scrape():
                if progressbar:
                    desc = '%s -> %s' % (source, export)
                    total = pipe.count()
                    rows = tqdm.tqdm(pipe.rows(), desc, total, leave=True)
                else:
                    rows = pipe.rows()

                for row in rows:
                    for key, value in keyvalueitems(selector(row)):
                        if key is not None:
                            yield Row(key=key, value=value)

            if export.endswith('.jsonl'):
                jsonl.export(export, scrape())
            else:
                csv.export(export, scrape())

        else:
            row = pipe.last(key)
            row = row['row'] if row and errors else row

            if raw:
                if row:
                    rows = keyvalueitems(selector(row))
                    return [Row(key=key, value=value) for key, value in rows if key is not None]
                else:
                    return []
            else:
                if row:
                    rows = keyvalueitems(selector(row))
                    if table:
                        self.bot.output.table([Row(key=key, value=value) for key, value in rows if key is not None])
                    else:
                        for key, value in rows:
                            if key is not None:
                                self.bot.output.key_value(key, value)
                else:
                    self.info('Not found.')


class Download(Command):

    def add_arguments(self, parser):
        parser.add_argument('url', type=str)
        parser.add_argument('-x', '--exclude', type=str, help="Exclude items from value.")
        parser.add_argument('-a', '--append', type=str, help="Append downloaded content to specified pipe.")

    def run(self, args):
        from databot import this
        from databot.db.utils import Row
        from databot.handlers import download

        exclude = args.exclude.split(',') if args.exclude else None
        key, value = next(download.download(self.bot.requests, this.key)(Row(key=args.url, value=None)))
        self.bot.output.key_value(key, value, exclude=exclude)

        if args.append:
            self.pipe(args.append).append(key, value)


class Skip(Command):

    def add_arguments(self, parser):
        parser.add_argument('source', type=str, help="Source pipe.")
        parser.add_argument('target', type=str, help="Target pipe.")

    def run(self, args):
        source = self.pipe(args.source)
        target = self.pipe(args.target)
        target(source).skip()
        self.bot.output.status(self.bot)


class Reset(Command):

    def add_arguments(self, parser):
        parser.add_argument('source', type=str, help="Source pipe.")
        parser.add_argument('target', type=str, help="Target pipe.")

    def run(self, args):
        source = self.pipe(args.source)
        target = self.pipe(args.target)
        target(source).reset()
        self.bot.output.status(self.bot)


class Offset(Command):

    def add_arguments(self, parser):
        parser.add_argument('source', type=str, help="Source pipe.")
        parser.add_argument('target', type=str, help="Target pipe.")
        parser.add_argument('offset', type=int, help="Relative offset.")

    def run(self, args):
        source = self.pipe(args.source)
        target = self.pipe(args.target)
        target(source).offset(args.offset)
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
        parser.add_argument('source', type=str, help="Source pipe.")
        parser.add_argument('target', type=str, nargs='?', help="Target pipe.")
        parser.add_argument('-k', '--key', type=str, help="If key is not provided, last item will be shown.")
        parser.add_argument('-x', '--exclude', type=str, help="Exclude items from value.")
        parser.add_argument('-b', '--in-browser', action='store_true', help="Show value content in browser.")
        parser.add_argument('-e', '--errors', action='store_true', help="Read data from target's errors.")

    def run(self, args):
        import ast

        # XXX: https://trello.com/c/fP9v43dF/57-pataisyti-literal-eval-naudojima
        if isinstance(args.key, str) and (args.key[0] in ('[', '{', '"', "'") or args.key.isnumeric()):
            key = ast.literal_eval(args.key)
        else:
            key = args.key or None

        source = self.pipe(args.source)
        target = self.bot.pipe(args.target) if args.target else None

        self.call(source, target, key, errors=args.errors, exclude=args.exclude, in_browser=args.in_browser)

    def call(self, source, target=None, key=None, errors=False, exclude=None, in_browser=False):
        """Show content of a record in a pipe.

        Parameters
        ----------
        source : databot.pipes.Pipe
        target : databot.pipes.Pipe
        key : str, optional
            Use specific key from pipe. If not specified last entry will be shown.
        errors : bool, optional
            Read data frm target's errors.
        exclude : List[str], optional
            Exclude specified fields from output.
        in_browser : boolean, optional
            If this is a downloaded page, show it in your default web browser.

        """

        import webbrowser
        import tempfile

        if errors:
            assert target
            pipe = target(source).errors
        elif target:
            pipe = target(source)
        else:
            pipe = source

        row = pipe.last(key)
        row = row['row'] if row and errors else row

        if row:
            exclude = exclude.split(',') if exclude else None
            self.bot.output.key_value(row.key, row.value, exclude=exclude)
            if in_browser:
                with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as f:
                    f.write(row.value['content'])
                webbrowser.open(f.name)
        else:
            print('Not found.')


class Head(Command):

    def add_arguments(self, parser):
        parser.add_argument('pipe', type=str, help="Pipe id, for example: 1")
        parser.add_argument('-n', type=int, dest='limit', default=10, help="Number of rows to show.")
        parser.add_argument('-x', '--exclude', type=str, help="Exclude fields from row.")
        parser.add_argument('-i', '--include', type=str, help="Include fields from row.")
        parser.add_argument('-t', '--table', action='store_true', default=False, help="Print ascii table.")

    def run(self, args):
        self.call(self.pipe(args.pipe), limit=args.limit, table=args.table, exclude=args.exclude, include=args.include)

    def call(self, pipe, limit=10, table=False, include=None, exclude=None):
        from databot.db.utils import create_row

        rows = [create_row(row) for row in self.rows(pipe, limit)]

        if rows:
            exclude = exclude.split(',') if exclude else None
            include = include.split(',') if include else None
            if table:
                self.bot.output.table(rows, exclude=exclude, include=include)
            else:
                for row in rows:
                    self.bot.output.key_value(row.key, row.value, exclude=exclude)
        else:
            print('Not found.')

    def rows(self, pipe, limit):
        return pipe.engine.execute(pipe.table.select().order_by(pipe.table.c.id.asc()).limit(limit))


class Tail(Head):

    def rows(self, pipe, limit):
        return reversed(list(pipe.engine.execute(pipe.table.select().order_by(pipe.table.c.id.desc()).limit(limit))))


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
        csv.export(args.path, pipe.rows(), exclude=exclude, include=include, append=args.append, header=args.header)


class Resolve(Command):

    def add_arguments(self, parser):
        parser.add_argument('source', type=str, help="Source pipe id or name.")
        parser.add_argument('target', type=str, help="Target pipe id or name to mark errors as resolved.")
        parser.add_argument('key', type=str, nargs='?',
                            help="Mark as resolve only specific key if not specified marks all errors as resolved.")

    def run(self, args):
        import ast

        key = ast.literal_eval(args.key) if args.key else None
        source = self.pipe(args.source)
        target = self.pipe(args.target)
        target(source).errors.resolve(key)
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
        source = self.pipe(args.source)
        target = self.pipe(args.target)
        errors = target(source).errors(key, reverse=True)
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


class Compress(Command):

    def add_arguments(self, parser):
        parser.add_argument('pipes', nargs='+', type=str, help="Pipe name or id")

    def run(self, args):
        pipes = [self.pipe(x) for x in args.pipes]
        self.call(*pipes)

    def call(self, *pipes):
        """Compress specified pipes.

        If you use SQLite, in order for compression to take effect, you need to vacuum SQLite database using this
        command:

            sqlite3 path/to.db vacuum

        This will require at least same amount of free space as path/to.db file is currently taking.

        Parameters
        ----------
        *pipes : databot.pipes.Pipe

        """

        for pipe in pipes:
            pipe.compress()


class Decompress(Command):

    def add_arguments(self, parser):
        parser.add_argument('pipes', nargs='+', type=str, help="Pipe name or id")

    def run(self, args):
        pipes = [self.pipe(x) for x in args.pipes]
        self.call(*pipes)

    def call(self, *pipes):
        for pipe in pipes:
            pipe.decompress()
