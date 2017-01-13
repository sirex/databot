import sys
import argparse
import databot

from sqlalchemy.engine import reflection


def define(bot):
    tables = set(reflection.Inspector.from_engine(bot.engine).get_table_names())
    for pipe in bot.engine.execute(bot.models.pipes.select()):
        table_name = 't%s' % pipe.id
        if table_name in tables:
            yield databot.define(pipe.pipe)


def main(argv=None, output=sys.stdout):
    argv = argv or sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument('db', help='path to sqlite datbase or database connection string')
    args = parser.parse_args(argv[:1])
    bot = databot.Bot(args.db, output=output)

    pipeline = {
        'pipes': list(define(bot)),
        'tasks': [],
    }

    bot.main(pipeline, argv=argv[1:])


if __name__ == '__main__':
    main()
