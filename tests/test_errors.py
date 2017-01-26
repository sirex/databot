import pytest
import databot
import databot.testing


def test_main(db):
    handler = databot.testing.ErrorHandler('2')

    bot = db.Bot()
    bot.define('t1')
    bot.define('t2')
    bot.pipe('t1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])

    bot.main(argv=['-v0', 'run'])
    t1 = bot.pipe('t1')
    t2 = bot.pipe('t2')
    t2(t1).call(handler)
    assert t2(t1).errors.count() == 1
    assert list(t2(t1).errors.keys()) == ['2']
    assert list(t2.items()) == [('1', 'A'), ('3', 'C')]

    bot.main(argv=['-v0', 'run', '--retry'])
    t1 = bot.pipe('t1')
    t2 = bot.pipe('t2')
    t2(t1).call(handler)
    assert t2(t1).errors.count() == 1
    assert list(t2(t1).errors.keys()) == ['2']
    assert list(t2.items()) == [('1', 'A'), ('3', 'C')]

    handler.error_keys = set()
    bot.main(argv=['-v0', 'run', '--retry'])
    t1 = bot.pipe('t1')
    t2 = bot.pipe('t2')
    t2(t1).call(handler)
    assert t2(t1).errors.count() == 0
    assert list(t2(t1).errors.keys()) == []
    assert list(t2.items()) == [('1', 'A'), ('3', 'C'), ('2', 'B')]


def test_retry_query(db):
    error_keys = {'1', '3'}

    def handler(row):
        nonlocal error_keys
        if row.key in error_keys:
            raise ValueError('Error.')
        else:
            yield row.key, row.value.upper()

    bot = db.Bot()
    bot.define('t1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])
    bot.define('t2')

    bot.main(argv=['-v0', 'run'])
    t1 = bot.pipe('t1')
    t2 = bot.pipe('t2')
    t2(t1).call(handler)
    assert list(t2(t1).errors.keys()) == ['1', '3']

    assert [(error.source.pipe, error.target.pipe) for error in bot.query_retry_pipes()] == [
        ('t1', 't2'),
        ('t1', 't2'),
    ]
    assert list(t2.items()) == [('2', 'B')]

    error_keys = {}
    bot.main(argv=['-v0', 'run', '--retry'])
    t1 = bot.pipe('t1')
    t2 = bot.pipe('t2')
    t2(t1).call(handler)

    assert list(t2.items()), [('2', 'B') == ('1', 'A'), ('3', 'C')]


@pytest.fixture
def bot(db):
    return db.Bot()


@pytest.fixture
def t1(bot):
    return bot.define('pipe 1', None).append([('1', 'a'), ('2', 'b'), ('3', 'c')])


@pytest.fixture
def t2(bot, t1):
    t2 = bot.define('pipe 2', None)

    rows = list(t1.rows())
    t2(t1).errors.report(rows[0], 'Error 1')
    t2(t1).errors.report(rows[2], 'Error 2')

    return t2


def test_count_without_source(t2):
    assert t2(None).errors.count() == 0


def test_count_with_source(t1, t2):
    assert t2(t1).errors.count() == 2


def test_keys(t1, t2):
    assert list(t2(t1).errors.keys()) == ['1', '3']


def test_values(t1, t2):
    assert list(t2(t1).errors.values()) == ['a', 'c']


def test_items(t1, t2):
    assert list(t2(t1).errors.items()) == [('1', 'a'), ('3', 'c')]


def test_rows(t1, t2):
    assert [(row.key, row.value) for row in t2(t1).errors.rows()] == [('1', 'a'), ('3', 'c')]


def test_resolve_all(t1, t2):
    t2(t1).errors.resolve()
    assert list(t2(t1).errors.keys()) == []


def test_resolve_key(t1, t2):
    t2(t1).errors.resolve('1')
    assert list(t2(t1).errors.keys()) == ['3']


@pytest.mark.parametrize('limit, errors, left, raises', [
    (None, 2, 0, False),
    (0, 0, 3, True),
    (1, 1, 3, True),
    (2, 2, 2, True),
    (3, 2, 0, False),
])
def test_error_limit(limit, errors, left, raises):

    def errorif(*keys):
        def handler(row):
            if row.key in keys:
                raise ValueError('Error for key: %r.' % row.key)
            else:
                yield row.key, row.value.upper()
        return handler

    bot = databot.Bot()
    t1 = bot.define('t1').append([(1, 'a'), (2, 'b'), (3, 'c')])
    t2 = bot.define('t2')

    if raises:
        with pytest.raises(ValueError):
            t2(t1).call(errorif(1, 2), error_limit=limit)
    else:
        t2(t1).call(errorif(1, 2), error_limit=limit)
    assert t2(t1).errors.count() == errors
    assert t2(t1).count() == left
