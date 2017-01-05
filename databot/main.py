import sys
import argparse
import databot


def define(bot):
    for pipe in bot.engine.execute(bot.models.pipes.select()):
        bot.define(pipe.pipe)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db', help='path to sqlite datbase or database connection string')
    args = parser.parse_args(sys.argv[1:2])

    databot.Bot(args.db).main(define, argv=sys.argv[2:])


if __name__ == '__main__':
    main()
