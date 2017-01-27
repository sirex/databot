from databot import task
from databot.runner import run_all_tasks


def test_watch(bot):
    a = bot.define('a')
    b = bot.define('b')

    def handler(row):
        if row.key < 16:
            yield row.key + row.key

    bot.commands.run([
        task('a').once().append(1),
        task('a', 'b', watch=True).call(handler),
        task('b', 'a', watch=True).call(handler),
        task('b').once().append(1),
    ])

    assert list(a.keys()) == [1, 4, 16, 2, 8]
    assert list(b.keys()) == [2, 8, 1, 4, 16]


def test_watch_limits(bot):
    def handler(row):
        if row.key < 16:
            yield row.key + 1

    tasks = [
        task('a').once().append(1),
        task('a', 'b', watch=True).call(handler),
        task('b', 'a', watch=True).call(handler),
    ]

    a = bot.define('a')
    b = bot.define('b')

    #  a            | b         | run
    # --------------+-----------+----------------------------------------------
    #  [1]          | []        | task('a').once().append(1)
    #               |           |   watch:
    #  [1]          | [2]       |     task('a', 'b', watch=True).call(handler)
    #  [1, 3]       | [2]       |     task('b', 'a', watch=True).call(handler)
    #  [1, 3]       | [2, 4]    | task('a', 'b', watch=True).call(handler)
    #               |           |   watch:
    #  [1, 3, 5]    | [2, 4]    |     task('b', 'a', watch=True).call(handler)
    #  [1, 3, 5]    | [2, 4]    | task('b', 'a', watch=True).call(handler)
    #               |           |   watch:
    #  [1, 3, 5]    | [2, 4, 6] |     task('a', 'b', watch=True).call(handler)
    #  [1, 3, 5, 7] | [2, 4, 6] |     task('b', 'a', watch=True).call(handler)
    bot.limit = 1
    run_all_tasks(bot, tasks)
    assert list(a.keys()) == [1, 3, 5, 7]
    assert list(b.keys()) == [2, 4, 6]

    bot.limit = 1
    run_all_tasks(bot, tasks)
    assert list(a.keys()) == [1, 3, 5, 7, 9, 11, 13]
    assert list(b.keys()) == [2, 4, 6, 8, 10, 12]

    bot.limit = 0
    run_all_tasks(bot, tasks)
    assert list(a.keys()) == [1, 3, 5, 7, 9, 11, 13, 15]
    assert list(b.keys()) == [2, 4, 6, 8, 10, 12, 14, 16]
