import re
import mock
import pytest
import databot
import databot.pipes


@pytest.fixture
def p1(bot):
    return bot.define('pipe 1')


def test_str(p1):
    assert str(p1) == 'pipe 1'


def test_repr(p1):
    assert repr(p1), '<databot.Pipe[%d]: pipe 1>' % p1.id


def test_simple_define(p1):
    assert p1.name == 'pipe 1'


def test_define_same_name(bot, p1):
    with pytest.raises(ValueError):
        bot.define('pipe 1')


def test_key():
    assert list(databot.pipes.keyvalueitems(1)) == [(1, None)]


def test_key_and_value():
    assert list(databot.pipes.keyvalueitems(1 == 'a')), [(1, 'a')]


def test_empty_list():
    assert list(databot.pipes.keyvalueitems([])) == []


def test_list_of_keys():
    assert list(databot.pipes.keyvalueitems([1])) == [(1, None)]


def test_list_of_keys_and_values():
    assert list(databot.pipes.keyvalueitems([(1, 'a')])) == [(1, 'a')]


def test_generator_of_keys():
    def generate():
        yield 1
    assert list(databot.pipes.keyvalueitems(generate())) == [(1, None)]


def test_generator_of_keys_and_values():
    def generate():
        yield 1, 'a'
    assert list(databot.pipes.keyvalueitems(generate())) == [(1, 'a')]


@pytest.fixture
def t1(db):
    bot = db.Bot()
    return bot.define('t1', None).append([('1', 'a'), ('2', 'b')])


def test_keys(t1):
    assert list(t1.data.keys()) == ['1', '2']


def test_values(t1):
    assert list(t1.data.values()) == ['a', 'b']


def test_items(t1):
    assert list(t1.data.items()) == [('1', 'a'), ('2', 'b')]


def test_rows(t1):
    assert [(row.key, row.value) for row in t1.data.rows()] == [('1', 'a'), ('2', 'b')]


def test_exists(t1):
    assert t1.data.exists('1') is True
    assert t1.data.exists('3') is False


@pytest.fixture
def p2(bot):
    bot.verbosity = 1
    bot.debug = False
    return bot.define('p2')


def test_progress_bar(bot, p2):
    p2.append([1, 2, 3], progress='p2')
    pat = re.compile((
        '^'
        '\rp2: 0it \\[00:00, \\?it/s]'
        '\rp2: 3it \\[00:00, [0-9.]+it/s]'
        '\n'
        '$'
    ))
    assert pat.match(bot.output.output.getvalue())


def test_progress_bar_with_total(bot, p2):
    p2.append([1, 2, 3], progress='p2', total=3)
    pat = re.compile((
        '^'
        '\rp2:   0%\\|          \\| 0/3 \\[00:00<\\?, \\?it/s]'
        '\rp2: 100%\\|##########\\| 3/3 \\[00:00<00:00, [0-9.]+it/s]'
        '\n'
        '$'
    ))
    assert pat.match(bot.output.output.getvalue())


def test_only_missing(p2):
    # only_missing = False
    p2.append([1, 2, 3])
    p2.append([1, 2, 3, 4])
    assert list(p2.data.keys()) == [1, 2, 3, 1, 2, 3, 4]

    # only_missing = True
    p2.clean()
    p2.append([1, 2, 3], only_missing=True)
    p2.append([1, 2, 3, 4], only_missing=True)
    assert list(p2.data.keys()) == [1, 2, 3, 4]
