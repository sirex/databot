import io
import textwrap
import pytest
import pandas as pd


def handler(row):
    yield row.key**2


def interrupt(row):
    if row.key > 1:
        raise KeyboardInterrupt
    else:
        yield row.key**2


@pytest.fixture
def task(db):
    bot = db.Bot()
    p1 = bot.define('p1').append([1, 2, 3])
    p2 = bot.define('p2')
    return p2(p1)


def test_count(task):
    assert task.count() == 3
    assert task.target(None).count() == 0


def test_rows(task):
    assert [(x.key, x.value) for x in task.rows()] == [(1, None), (2, None), (3, None)]
    assert [(x.key, x.value) for x in task.target(None).rows()] == []


def test_call_debug(bot):
    bot.debug = True
    bot.output.output = io.StringIO()
    p1 = bot.define('p1').append([1, 2, 3])
    p2 = bot.define('p2')
    p2(p1).call(handler)

    assert list(p2.keys()) == []
    assert bot.output.output.getvalue() == textwrap.dedent('''\
    ------------------------------------------------------------------------
    source: id=1 key=1
    - key: 1
      value: None
    ------------------------------------------------------------------------
    source: id=2 key=2
    - key: 4
      value: None
    ------------------------------------------------------------------------
    source: id=3 key=3
    - key: 9
      value: None
    ''')


def test_call_verbose(bot):
    bot.verbosity = 2
    bot.output.output = io.StringIO()
    p1 = bot.define('p1').append([1, 2, 3])
    p2 = bot.define('p2')
    p2(p1).call(handler)

    assert list(p2.keys()) == [1, 4, 9]
    assert bot.output.output.getvalue() == textwrap.dedent('''\
    ------------------------------------------------------------------------
    source: id=1 key=1
    - key: 1
      value: None
    ------------------------------------------------------------------------
    source: id=2 key=2
    - key: 4
      value: None
    ------------------------------------------------------------------------
    source: id=3 key=3
    - key: 9
      value: None
    ''')


@pytest.mark.parametrize('limit,result', [
    (0, [1, 4, 9]),
    (1, [1]),
    (2, [1, 4]),
    (3, [1, 4, 9]),
    (4, [1, 4, 9]),
])
def test_call_limit(bot, limit, result):
    bot.limit = limit
    p1 = bot.define('p1').append([1, 2, 3])
    p2 = bot.define('p2')
    p2(p1).call(handler)
    assert list(p2.keys()) == result


def test_call_keyboard_interrupt(bot):
    p1 = bot.define('p1').append([1, 2, 3])
    p2 = bot.define('p2')

    with pytest.raises(KeyboardInterrupt):
        p2(p1).call(interrupt)

    assert list(p2.keys()) == [1]


def test_retry_verbose(bot):
    bot.verbosity = 1
    bot.output.output = io.StringIO()

    p1 = bot.define('p1').append([1, 2, 3])
    p2 = bot.define('p2')

    task = p2(p1)

    task.errors.report(p1.get(1), 'Error 1')
    task.errors.report(p1.get(3), 'Error 2')

    task.retry(handler)

    assert list(p2.keys()) == [1, 9]
    assert 'p1 -> p2 (retry):' in bot.output.output.getvalue()


@pytest.mark.parametrize('limit,result', [
    (0, [1, 4, 9]),
    (1, [1]),
    (2, [1, 4]),
    (3, [1, 4, 9]),
    (4, [1, 4, 9]),
])
def test_retry_limit(bot, limit, result):

    p1 = bot.define('p1').append([1, 2, 3])
    p2 = bot.define('p2')

    task = p2(p1)

    task.errors.report(p1.get(1), 'Error 1')
    task.errors.report(p1.get(2), 'Error 2')
    task.errors.report(p1.get(3), 'Error 3')

    bot.limit = limit
    task.retry(handler)

    assert list(p2.keys()) == result


def test_retry_debug(bot):
    bot.debug = True
    bot.output.output = io.StringIO()
    p1 = bot.define('p1').append([1, 2, 3])
    p2 = bot.define('p2')
    p2(p1).errors.report(p1.get(1), 'Error 1')
    p2(p1).retry(handler)

    assert list(p2.keys()) == []
    assert bot.output.output.getvalue() == textwrap.dedent('''\
    ------------------------------------------------------------------------
    source: id=1 key=1
    - key: 1
      value: None
    ''')


def test_retry_verbose_2(bot):
    bot.verbosity = 2
    bot.output.output = io.StringIO()
    p1 = bot.define('p1').append([1, 2, 3])
    p2 = bot.define('p2')
    p2(p1).errors.report(p1.get(1), 'Error 1')
    p2(p1).retry(handler)

    assert list(p2.keys()) == [1]
    assert bot.output.output.getvalue() == textwrap.dedent('''\
    ------------------------------------------------------------------------
    source: id=1 key=1
    - key: 1
      value: None
    ''')


def test_retry_keyboard_interrupt(bot):
    p1 = bot.define('p1').append([1, 2, 3])
    p2 = bot.define('p2')
    p2(p1).errors.report(p1.get(1), 'Error 1')
    p2(p1).errors.report(p1.get(2), 'Error 2')
    p2(p1).errors.report(p1.get(3), 'Error 3')

    with pytest.raises(KeyboardInterrupt):
        p2(p1).retry(interrupt)

    assert list(p2.keys()) == [1]


def test_export_csv(tmpdir, bot):
    path = tmpdir.join('data.csv')
    bot.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')]).export(str(path))
    assert path.read() == textwrap.dedent('''\
    key,value
    1,a
    2,b
    3,c
    ''')


def test_export_jsonl(tmpdir, bot):
    path = tmpdir.join('data.jsonl')
    bot.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')]).export(str(path))
    assert path.read() == textwrap.dedent('''\
    {"key": 1, "value": "a"}
    {"key": 2, "value": "b"}
    {"key": 3, "value": "c"}
    ''')


def test_export_pandas(bot):
    frame = bot.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')]).export(pd)
    assert [dict(x._asdict()) for x in frame.itertuples()] == [
        {'Index': 1, 'value': 'a'},
        {'Index': 2, 'value': 'b'},
        {'Index': 3, 'value': 'c'},
    ]
