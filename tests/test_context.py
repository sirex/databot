import pytest


@pytest.fixture
def t1(bot):
    return bot.define('pipe 1').append([('1', 'a'), ('2', 'b')])


@pytest.fixture
def t2(bot):
    return bot.define('pipe 2')


def test_count_with_context(t2):
    assert t2.count() is 0


def test_count_without_context(t1, t2):
    assert t2(t1).count() == 2


def test_is_filled_with_source(t1, t2):
    assert t2(t1).is_filled() is True


def test_is_filled_without_source(t1, t2):
    assert t2(None).is_filled() is False


def test_keys(t1, t2):
    assert list(t2(t1).keys()) == ['1', '2']


def test_values(t1, t2):
    assert list(t2(t1).values()) == ['a', 'b']


def test_items(t1, t2):
    assert list(t2(t1).items()) == [('1', 'a'), ('2', 'b')]


def test_rows(t1, t2):
    assert [(row.key, row.value) for row in t2(t1).rows()] == [('1', 'a'), ('2', 'b')]
