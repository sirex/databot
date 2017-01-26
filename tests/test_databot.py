from databot import define, task, this


def test_main(db):
    pipeline = {
        'pipes': [
            define('p1'),
            define('p2'),
        ],
        'tasks': [
            task('p1').append([('1', 'a'), ('2', 'b'), ('3', 'c')]),
            task('p1', 'p2').select(this.key, this.value.upper()),
        ],
    }

    bot = db.Bot().main(pipeline, argv=['-v0', 'run'])

    assert list(bot.pipe('p1').items()) == [('1', 'a'), ('2', 'b'), ('3', 'c')]
    assert list(bot.pipe('p2').items()) == [('1', 'A'), ('2', 'B'), ('3', 'C')]
