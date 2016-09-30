import pytest


@pytest.fixture
def t1(bot):
    return bot.define('pipe 1').append([('1', 'a'), ('2', 'b')])


@pytest.fixture
def t2(bot):
    return bot.define('pipe 2')


def test_context(bot, t1, t2):
    assert bot.pipe('pipe 1').source is None
    assert bot.pipe('pipe 2').source is None

    with bot.pipe('pipe 1'):
        assert bot.pipe('pipe 1').source.name == 'pipe 1'
        assert bot.pipe('pipe 2').source.name == 'pipe 1'

        with bot.pipe('pipe 2'):
            assert bot.pipe('pipe 1').source.name == 'pipe 2'

        assert bot.pipe('pipe 1').source.name == 'pipe 1'
        assert bot.pipe('pipe 2').source.name == 'pipe 1'

    assert bot.pipe('pipe 1').source is None
    assert bot.pipe('pipe 2').source is None


def test_count_with_context(t2):
    assert t2.count() is 0


def test_count_without_context(t1, t2):
    with t1:
        assert t2.count() == 2


def test_is_filled_with_source(t1, t2):
    with t1:
        assert t2.is_filled() is True


def test_is_filled_without_source(t1, t2):
    assert t2.is_filled() is False


def test_keys(t1, t2):
    with t1:
        assert list(t2.keys()) == ['1', '2']


def test_values(t1, t2):
    with t1:
        assert list(t2.values()) == ['a', 'b']


def test_items(t1, t2):
    with t1:
        assert list(t2.items()) == [('1', 'a'), ('2', 'b')]


def test_rows(t1, t2):
    with t1:
        assert [(row.key, row.value) for row in t2.rows()] == [('1', 'a'), ('2', 'b')]
