import pytest
import sqlalchemy as sa

from databot import Bot


def test_init():
    assert str(Bot('sqlite:///:memory:').engine.url) == 'sqlite:///:memory:'


def test_init_no_args():
    assert str(Bot().engine.url) == 'sqlite:///:memory:'


def test_init_path(tmpdir):
    path = str(tmpdir.join('data.db'))
    assert str(Bot(path).engine.url) == 'sqlite:///%s' % path


def test_init_with_engine_instance():
    Bot(sa.create_engine('sqlite:///:memory:'))


def test_define(bot):
    execute, pipes, pipe = bot.engine.execute, bot.models.pipes, bot.models.pipes.c.pipe
    bot.define('pipe')
    row = execute(pipes.select(pipe == 'pipe')).fetchone()
    assert row['bot'] == 'databot.bot.Bot'


def test_define_duplicate(bot):
    bot.define('pipe')
    with pytest.raises(ValueError):
        bot.define('pipe')


def test_pipe(bot):
    bot.define('pipe')
    pipe = bot.pipe('pipe')
    assert pipe.name == 'pipe'


def test_compact_empty_pipe(bot):
    pipe = bot.define('pipe')
    bot.compact()
    assert list(pipe.keys()) == []


def test_compact_full_pipe(bot):
    pipe = bot.define('pipe')
    pipe.append([1, 1, 2, 1, 1])
    assert list(pipe.keys()) == [1, 1, 2, 1, 1]

    bot.compact()
    list(pipe.keys()) == [2, 1]


def test_compress(bot):
    pipe = bot.define('pipe')
    pipe.append([1, 1, 2, 1, 1])
    assert list(pipe.keys()) == [1, 1, 2, 1, 1]

    bot.compact()
    list(pipe.keys()) == [2, 1]


def test_autodefine():
    engine = sa.create_engine('sqlite:///:memory:')

    bot = Bot(engine)
    bot.define('a').append([1, 2, 3])

    bot = Bot(engine)
    with pytest.raises(KeyError) as e:
        bot.pipe('a')
    assert str(e.value) == "'a'"

    bot = Bot(engine).autodefine()
    assert list(bot.pipe('a').keys()) == [1, 2, 3]
