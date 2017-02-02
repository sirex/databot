import sys
import argparse
import databot

from databot.db.services import get_pipe_tables


def main(argv=None, output=sys.stdout):
    argv = argv or sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument('db', help='path to sqlite datbase or database connection string')
    args = parser.parse_args(argv[:1])
    bot = databot.Bot(args.db, output=output)

    pipeline = {
        'pipes': [databot.define(pipe.pipe) for pipe in get_pipe_tables(bot)],
        'tasks': [],
    }

    bot.main(pipeline, argv=argv[1:])


if __name__ == '__main__':
    main()
