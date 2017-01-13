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

    assert list(bot.pipe('a').data.keys()) == ['a', 'A', 'b']
    assert list(bot.pipe('b').data.keys()) == ['A', 'B']
