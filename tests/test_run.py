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
            task('a').append(['a']),
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
