import pytest


@pytest.fixture
def bot(bot):
    bot.define('a').append([1, 2, 3, 4, 5])
    bot.define('b')
    return bot


def test_offset_skip_reset(bot):

    def keys(p):
        return list(map(int, p.keys()))

    with bot.pipe('a'):
        b = bot.pipe('b')
        assert keys(b) == [1, 2, 3, 4, 5]
        assert keys(b.offset(3)) == [4, 5]
        assert keys(b.offset(-2)) == [2, 3, 4, 5]
        assert keys(b.offset(1)) == [3, 4, 5]
        assert keys(b.offset(0)) == [3, 4, 5]
        assert keys(b.offset(10)) == []
        assert keys(b.offset(-10)) == [1, 2, 3, 4, 5]
        assert keys(b.skip()) == []
        assert keys(b.reset()) == [1, 2, 3, 4, 5]
        assert keys(b.skip()) == []
        assert keys(b.reset()) == [1, 2, 3, 4, 5]
        assert keys(b.offset(+1)) == [2, 3, 4, 5]
        assert keys(b.offset(+1)) == [3, 4, 5]
        assert keys(b.offset(+1)) == [4, 5]
        assert keys(b.offset(+1)) == [5]
        assert keys(b.offset(+1)) == []
        assert keys(b.offset(+1)) == []
        assert keys(b.offset(-1)) == [5]
        assert keys(b.offset(-1)) == [4, 5]
        assert keys(b.offset(-1)) == [3, 4, 5]
        assert keys(b.offset(-1)) == [2, 3, 4, 5]
        assert keys(b.offset(-1)) == [1, 2, 3, 4, 5]
        assert keys(b.offset(-1)) == [1, 2, 3, 4, 5]
