import argparse
import prettytable
import pprint
import sqlalchemy as sa
import tqdm

from databot.db import models
from databot.db.utils import create_row
from databot.db.windowedquery import windowed_query


def get_table(cols):
    table = prettytable.PrettyTable(next(zip(*cols)))
    for col, align in cols:
        table.align[col] = align
    return table


class Cli(object):
    def __init__(self, bot, args):
        self.bot = bot
        self.bot.init()
        self.args = args

    def main(self):
        args = list(self.args)
        task = None
        source = None
        target = None
        command = 'state'
        commands = {
            'state': StateCommand,
            'test': TestCommand,
            'retry': RetryCommand,
            'run': RunCommand,
            'skip': SkipCommand,
            'export': ExportCommand,
            'append': AppendCommand,
        }

        # Pop command
        if len(args) > 0 and args[0] in commands:
            command = args.pop(0)

        # Pop task or source:target code
        if len(args) > 0:
            tasks = {task.code for task in self.bot.tasks}
            if args[0] in tasks:
                task = self.bot.task(code=args.pop(0))
            elif ':' in args[0]:
                source, target = args.pop(0).split(':')
                source = self.bot.task(code=source)
                target = self.bot.task(code=target)

        Cmd = commands[command]

        command = Cmd(self, task, source, target, args)
        command.validate()
        command.main()

        # if 'retry' in args:
        #     self.retry()
        # else:
        #     self.run()
        #


class ValidationError(Exception):
    pass


class Command(object):
    require = None

    def __init__(self, cli, task, source, target, args):
        self.bot = cli.bot
        self.cli = cli
        self.task = task
        self.source = source
        self.target = target

        self.parser = argparse.ArgumentParser()
        self.arguments()
        self.args = self.parser.parse_args(args)

    def validate(self):
        if self.require == 'source:target':
            if self.source is None or self.target is None:
                raise ValidationError('source:target is required.')
        elif self.require == 'task':
            if self.task is None:
                raise ValidationError('task is required.')

    def arguments(self):
        pass


class StateCommand(Command):

    def tasks_table(self):
        print('Tasks:\n')

        table = get_table([
            ('task', 'l'),
            ('rows', 'r'),
            ('description', 'l'),
        ])
        for task in self.bot.tasks:
            table.add_row([
                task.code,
                task.data.count(),
                task.name,
            ])

        print(table)

    def state_table(self):
        print('\nJobs:\n')

        table = get_table([
            ('source:target', 'l'),
            ('rows', 'r'),
            ('errors', 'r'),
            ('offset', 'r'),
            ('description', 'l'),
        ])
        for state in self.bot.query_tasks_state():
            if state.source.task in self.bot.tasks_by_name and state.target.task in self.bot.tasks_by_name:
                source = self.bot.task(state.source.task)
                target = self.bot.task(state.target.task)
                with source:
                    table.add_row([
                        '%s:%s' % (source.code, target.code),
                        target.count(),
                        target.errors.count(),
                        state.offset,
                        '%s -> %s' % (source.name, target.name),
                    ])

        print(table)

    def main(self):
        self.tasks_table()
        self.state_table()


class TestCommand(Command):
    require = 'source:target'

    def main(self):
        with self.source:
            row = next(self.target.rows())
            result = self.target.handler(row)
            pprint.pprint(list(result))


class RetryCommand(Command):

    def retry_all(self):
        for state in list(self.bot.query_tasks_state()):
            if state.source.task in self.bot.tasks_by_name and state.target.task in self.bot.tasks_by_name:
                source = self.bot.task(state.source.task)
                target = self.bot.task(state.target.task)
                with source:
                    target.retry()

    def main(self):
        self.retry_all()


class RunCommand(Command):

    def main(self):
        self.bot.run()


class SkipCommand(Command):
    require = 'source:target'

    def main(self):
        with self.source:
            state = self.target.get_state()
            offset = self.bot.engine.execute(
                sa.select([self.source.table.c.id]).
                order_by(self.source.table.c.id.desc()).
                limit(1)
            ).scalar()
            print('Skip offset to %d' % offset)
            self.bot.engine.execute(models.state.update(models.state.c.id == state.id), offset=offset)


class ExportCommand(Command):
    require = 'task'

    def arguments(self):
        self.parser.add_argument('path', type=argparse.FileType('w', encoding='UTF-8'))

    def main(self):
        self.task.export(self.args.path.name, self.args.path)


class AppendCommand(Command):
    require = 'task'

    def arguments(self):
        self.parser.add_argument('key')
        self.parser.add_argument('value', nargs='?', default=None)

    def main(self):
        self.task.append(self.args.key, self.args.value)
