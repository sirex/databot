import pytest
import freezegun

from databot import Bot, define, task, this


def test_run():
    pipeline = {
        'pipes': [
            define('a'),
            define('b'),
        ],
        'tasks': [
            task('a').append(['a', 'A', 'b']),
            task('a', 'b').select(this.key.upper()),
            task().compact(),
        ],
    }

    bot = Bot()
    bot.main(pipeline, ['run', '-f'])

    assert list(bot.pipe('a').keys()) == ['a', 'A', 'b']
    assert list(bot.pipe('b').keys()) == ['A', 'B']


def test_run_target():
    pipeline = {
        'pipes': [],
        'tasks': [
            task('a').once().append(['a']),
            task('a', 'b').select(this.key.upper()),
            task('b', 'c').select(this.key.lower()),
            task().compact(),
        ],
    }

    bot = Bot()
    bot.define('a')
    bot.define('b')
    bot.define('c')

    bot.main(pipeline, ['run', 'a', '-f'])
    assert list(bot.pipe('a').keys()) == ['a']
    assert list(bot.pipe('b').keys()) == []
    assert list(bot.pipe('c').keys()) == []

    bot.main(pipeline, ['run', 'b', '-f'])
    assert list(bot.pipe('a').keys()) == ['a']
    assert list(bot.pipe('b').keys()) == ['A']
    assert list(bot.pipe('c').keys()) == []

    bot.pipe('a').append('b')
    bot.main(pipeline, ['run', 'a', 'b', '-f'])
    assert list(bot.pipe('a').keys()) == ['a', 'b']
    assert list(bot.pipe('b').keys()) == ['A', 'B']
    assert list(bot.pipe('c').keys()) == []

    bot.main(pipeline, ['run', 'b', 'c', '-f'])
    assert list(bot.pipe('a').keys()) == ['a', 'b']
    assert list(bot.pipe('b').keys()) == ['A', 'B']
    assert list(bot.pipe('c').keys()) == ['a', 'b']

    bot.pipe('b').append('C')
    bot.main(pipeline, ['run', 'c', '-f'])
    assert list(bot.pipe('a').keys()) == ['a', 'b']
    assert list(bot.pipe('b').keys()) == ['A', 'B', 'C']
    assert list(bot.pipe('c').keys()) == ['a', 'b', 'c']

    bot.main(pipeline, ['run', '-f'])
    assert list(bot.pipe('a').keys()) == ['b', 'a']
    assert list(bot.pipe('b').keys()) == ['B', 'C', 'A']
    assert list(bot.pipe('c').keys()) == ['b', 'c', 'a']


def test_run_limits():
    pipeline = {
        'tasks': [
            task('p1').once().append(['a', 'b', 'c']),
            task('p1', 'p2').select(this.key.upper()),
        ],
    }

    bot = Bot()
    p1 = bot.define('p1')
    p2 = bot.define('p2')

    bot.main(pipeline, ['run', '-l', '1,1,0'])
    assert list(p1.keys()) == ['a', 'b', 'c']
    assert list(p2.keys()) == ['A', 'B', 'C']
    assert pipeline['tasks'][0]._evals == 3
    assert pipeline['tasks'][1]._evals == 3


def test_run_once():
    tasks = [
        task('p1').once().append(1),
        task('p1').once().append(2),
        task('p1').append(3),
    ]

    bot = Bot()
    p1 = bot.define('p1')

    bot.commands.run(tasks, limits=(1, 1, 0))
    assert list(p1.keys()) == [1, 2, 3, 3, 3]


def test_run_limits_and_fail():
    def handler(row):
        if row.key == 'b':
            raise ValueError('b')
        else:
            yield row.key.upper()

    pipeline = {
        'tasks': [
            task('p1').once().append(['a', 'b', 'c']),
            task('p1', 'p2').call(handler),
        ],
    }

    bot = Bot()
    p1 = bot.define('p1')
    p2 = bot.define('p2')

    with pytest.raises(ValueError):
        bot.main(pipeline, ['run', '-l', '1,1,0'])

    assert list(p1.keys()) == ['a', 'b', 'c']
    assert list(p2.keys()) == ['A']
    assert pipeline['tasks'][0]._evals == 2
    assert pipeline['tasks'][1]._evals == 2


def test_run_limits_and_fail_smaller():
    def handler(row):
        if row.key == 'b':
            raise ValueError('b')
        else:
            yield row.key.upper()

    pipeline = {
        'tasks': [
            task('p1').once().append(['a', 'b', 'c']),
            task('p1', 'p2').call(handler),
        ],
    }

    bot = Bot()
    p1 = bot.define('p1')
    p2 = bot.define('p2')

    bot.main(pipeline, ['run', '-l', '1,1,0', '-f', '2'])

    assert list(p1.keys()) == ['a', 'b', 'c']
    assert list(p2.keys()) == ['A', 'C']
    assert list(p2(p1).errors.keys()) == ['b']
    assert pipeline['tasks'][0]._evals == 3
    assert pipeline['tasks'][1]._evals == 3


def test_run_daily():
    tasks = [
        task('p1').daily().append(['a']),
        task('p1', 'p2').select(this.key.upper()),
    ]

    bot = Bot()
    p1 = bot.define('p1')
    p2 = bot.define('p2')

    with freezegun.freeze_time('2017-01-01 00:00:00'):
        bot.commands.run(tasks)

    with freezegun.freeze_time('2017-01-01 12:00:00'):
        bot.commands.run(tasks)

    assert list(p1.keys()) == ['a']
    assert list(p2.keys()) == ['A']

    with freezegun.freeze_time('2017-01-02 00:00:00'):
        bot.commands.run(tasks)

    assert list(p1.keys()) == ['a', 'a']
    assert list(p2.keys()) == ['A', 'A']


def test_run_weekly():
    tasks = [
        task('p1').weekly().append(['a']),
        task('p1', 'p2').select(this.key.upper()),
    ]

    bot = Bot()
    p1 = bot.define('p1')
    p2 = bot.define('p2')

    with freezegun.freeze_time('2017-01-01 00:00:00'):
        bot.commands.run(tasks)

    with freezegun.freeze_time('2017-01-06 00:00:00'):
        bot.commands.run(tasks)

    assert list(p1.keys()) == ['a']
    assert list(p2.keys()) == ['A']

    with freezegun.freeze_time('2017-01-08 00:00:00'):
        bot.commands.run(tasks)

    assert list(p1.keys()) == ['a', 'a']
    assert list(p2.keys()) == ['A', 'A']


def test_run_monthly():
    tasks = [
        task('p1').monthly().append(['a']),
        task('p1', 'p2').select(this.key.upper()),
    ]

    bot = Bot()
    p1 = bot.define('p1')
    p2 = bot.define('p2')

    with freezegun.freeze_time('2017-01-01 00:00:00'):
        bot.commands.run(tasks)

    with freezegun.freeze_time('2017-01-15 00:00:00'):
        bot.commands.run(tasks)

    assert list(p1.keys()) == ['a']
    assert list(p2.keys()) == ['A']

    with freezegun.freeze_time('2017-02-15 00:00:00'):
        bot.commands.run(tasks)

    assert list(p1.keys()) == ['a', 'a']
    assert list(p2.keys()) == ['A', 'A']


def test_run_freq():
    tasks = [
        task('p1').freq(days=3).append(['a']),
        task('p1', 'p2').select(this.key.upper()),
    ]

    bot = Bot()
    p1 = bot.define('p1')
    p2 = bot.define('p2')

    with freezegun.freeze_time('2017-01-01 00:00:00'):
        bot.commands.run(tasks)

    with freezegun.freeze_time('2017-01-02 00:00:00'):
        bot.commands.run(tasks)

    assert list(p1.keys()) == ['a']
    assert list(p2.keys()) == ['A']

    with freezegun.freeze_time('2017-01-04 00:00:00'):
        bot.commands.run(tasks)

    assert list(p1.keys()) == ['a', 'a']
    assert list(p2.keys()) == ['A', 'A']
