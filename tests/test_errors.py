import pytest
import databot
import databot.testing


def test_main(db):
    t2 = databot.testing.ErrorHandler('2')

    bot = db.Bot()
    bot.define('t1')
    bot.define('t2')
    bot.pipe('t1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])

    bot.main(argv=['-v0', 'run'])
    with bot.pipe('t1'):
        bot.pipe('t2').call(t2)
        assert bot.pipe('t2').errors.count() == 1
        assert list(bot.pipe('t2').errors.keys()) == ['2']
        assert list(bot.pipe('t2').data.items()) == [('1', 'A'), ('3', 'C')]

    bot.main(argv=['-v0', 'run', '--retry'])
    with bot.pipe('t1'):
        bot.pipe('t2').call(t2)
        assert bot.pipe('t2').errors.count() == 1
        assert list(bot.pipe('t2').errors.keys()) == ['2']
        assert list(bot.pipe('t2').data.items()) == [('1', 'A'), ('3', 'C')]

    t2.error_keys = set()
    bot.main(argv=['-v0', 'run', '--retry'])
    with bot.pipe('t1'):
        bot.pipe('t2').call(t2)
        assert bot.pipe('t2').errors.count() == 0
        assert list(bot.pipe('t2').errors.keys()) == []
        assert list(bot.pipe('t2').data.items()) == [('1', 'A'), ('3', 'C'), ('2', 'B')]


def test_retry_query(db):
    error_keys = {'1', '3'}

    def t2(row):
        nonlocal error_keys
        if row.key in error_keys:
            raise ValueError('Error.')
        else:
            yield row.key, row.value.upper()

    bot = db.Bot()
    bot.define('t1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])
    bot.define('t2')

    bot.main(argv=['-v0', 'run'])
    with bot.pipe('t1'):
        bot.pipe('t2').call(t2)
        assert list(bot.pipe('t2').errors.keys()) == ['1', '3']

    assert [(error.source.pipe, error.target.pipe) for error in bot.query_retry_pipes()] == [
        ('t1', 't2'),
        ('t1', 't2'),
    ]
    assert list(bot.pipe('t2').data.items()) == [('2', 'B')]

    error_keys = {}
    bot.main(argv=['-v0', 'run', '--retry'])
    with bot.pipe('t1'):
        bot.pipe('t2').call(t2)

    assert list(bot.pipe('t2').data.items()), [('2', 'B') == ('1', 'A'), ('3', 'C')]


@pytest.fixture
def bot(db):
    return db.Bot()


@pytest.fixture
def t1(bot):
    return bot.define('pipe 1', None).append([('1', 'a'), ('2', 'b'), ('3', 'c')])


@pytest.fixture
def t2(bot, t1):
    t2 = bot.define('pipe 2', None)

    rows = list(t1.data.rows())
    with t1:
        t2.errors.report(rows[0], 'Error 1')
        t2.errors.report(rows[2], 'Error 2')

    return t2


def test_count_without_source(t2):
    assert t2.errors.count() is 0


def test_count_with_source(t1, t2):
    with t1:
        assert t2.errors.count() == 2


def test_keys(t1, t2):
    with t1:
        assert list(t2.errors.keys()) == ['1', '3']


def test_values(t1, t2):
    with t1:
        assert list(t2.errors.values()) == ['a', 'c']


def test_items(t1, t2):
    with t1:
        assert list(t2.errors.items()) == [('1', 'a'), ('3', 'c')]


def test_rows(t1, t2):
    with t1:
        assert [(row.key, row.value) for row in t2.errors.rows()] == [('1', 'a'), ('3', 'c')]


def test_resolve_all(t1, t2):
    with t1:
        t2.errors.resolve()
        assert list(t2.errors.keys()) == []


def test_resolve_key(t1, t2):
    with t1:
        t2.errors.resolve('1')
        assert list(t2.errors.keys()) == ['3']
