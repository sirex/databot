import pytest


@pytest.fixture
def bot(bot):
    bot.define('a').append([1, 2, 3, 4, 5])
    bot.define('b')
    return bot


def test_offset_skip_reset(bot):

    def keys(p):
        return list(map(int, p.keys()))

    a = bot.pipe('a')
    b = bot.pipe('b')
    task = b(a)
    assert keys(task) == [1, 2, 3, 4, 5]
    assert keys(task.offset(3)) == [4, 5]
    assert keys(task.offset(-2)) == [2, 3, 4, 5]
    assert keys(task.offset(1)) == [3, 4, 5]
    assert keys(task.offset(0)) == [3, 4, 5]
    assert keys(task.offset(10)) == []
    assert keys(task.offset(-10)) == [1, 2, 3, 4, 5]
    assert keys(task.skip()) == []
    assert keys(task.reset()) == [1, 2, 3, 4, 5]
    assert keys(task.skip()) == []
    assert keys(task.reset()) == [1, 2, 3, 4, 5]
    assert keys(task.offset(+1)) == [2, 3, 4, 5]
    assert keys(task.offset(+1)) == [3, 4, 5]
    assert keys(task.offset(+1)) == [4, 5]
    assert keys(task.offset(+1)) == [5]
    assert keys(task.offset(+1)) == []
    assert keys(task.offset(+1)) == []
    assert keys(task.offset(-1)) == [5]
    assert keys(task.offset(-1)) == [4, 5]
    assert keys(task.offset(-1)) == [3, 4, 5]
    assert keys(task.offset(-1)) == [2, 3, 4, 5]
    assert keys(task.offset(-1)) == [1, 2, 3, 4, 5]
    assert keys(task.offset(-1)) == [1, 2, 3, 4, 5]
