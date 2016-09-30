import datetime
import pytest

from databot.db.serializers import serrow


def exclude(data, keys):
    return {k: v for k, v in data.items() if k not in keys}


@pytest.fixture
def bot(db):
    bot = db.Bot().main(argv=['-v0', 'run'])
    bot.define('pipe 1')
    bot.define('pipe 2')
    return bot


def test_append(bot):
    t1 = bot.pipe('pipe 1')
    t1.append('foo', 'bar').append('a', 'b')
    data = [(row['key'], row['value']) for row in bot.engine.execute(t1.table.select())]
    assert data == [
        ('bd4339d24800a1815bb1b7382320cab96bba0cde', b'\x92\xa3foo\xa3bar'),
        ('9eddb6860ecf7699015ffff67e3b375820a48cad', b'\x92\xa1a\xa1b'),
    ]


def test_clean(bot):
    t1 = bot.pipe('pipe 1')
    t2 = bot.pipe('pipe 2')

    day = datetime.timedelta(days=1)
    now = datetime.datetime(2015, 6, 1, 1, 1, 0)
    bot.engine.execute(t1.table.insert(), serrow('1', 'a', created=now - 1 * day))
    bot.engine.execute(t1.table.insert(), serrow('2', 'b', created=now - 2 * day))
    bot.engine.execute(t1.table.insert(), serrow('3', 'c', created=now - 3 * day))

    with t1:
        assert t2.count() == 3

        t1.clean(3 * day, now=now)
        assert t2.count() == 2
        assert list(map(int, t2.keys())) == [1, 2]

        t1.clean(2 * day, now=now)
        assert t2.count() == 1
        assert list(map(int, t2.keys())) == [1]

        t1.clean()
        assert t2.count() == 0


def test_dedup(bot):
    t1 = bot.pipe('pipe 1')
    t1.append('1', 'a')
    t1.append('2', 'b')
    t1.append('2', 'c')
    t1.append('3', 'd')
    t1.append('3', 'e')
    assert t1.data.count() == 5
    assert t1.dedup().data.count() == 3
    assert list(t1.data.items()) == [
        ('1', 'a'),
        ('2', 'b'),
        ('3', 'd'),
    ]


def test_compact(bot):
    t1 = bot.pipe('pipe 1')
    t1.append('1', 'a')
    t1.append('2', 'b')
    t1.append('2', 'c')
    t1.append('3', 'd')
    t1.append('3', 'e')
    assert t1.data.count() == 5
    assert t1.compact().data.count() == 3
    assert list(t1.data.items()) == [
        ('1', 'a'),
        ('2', 'c'),
        ('3', 'e'),
    ]

    t1.append('3', 'x')
    assert t1.data.count() == 4

    bot.compact()
    assert t1.data.count() == 3


def test_initial_state(bot):
    t1 = bot.pipe('pipe 1')
    t2 = bot.pipe('pipe 2')

    assert exclude(t1.get_state(), 'id') == {
        'offset': 0,
        'source_id': None,
        'target_id': t1.id,
    }

    assert exclude(t2.get_state(), 'id') == {
        'offset': 0,
        'source_id': None,
        'target_id': t2.id,
    }


def test_context_state(bot):
    t1 = bot.pipe('pipe 1')
    t2 = bot.pipe('pipe 2')

    with t1:
        assert exclude(t2.get_state(), 'id') == {
            'offset': 0,
            'source_id': t1.id,
            'target_id': t2.id,
        }


def test_offset(bot):
    items_processed = 0

    def handler(item):
        nonlocal items_processed
        items_processed += 1
        yield item.key, item.value

    bot.define('pipe 3')

    t1 = bot.pipe('pipe 1')
    t3 = bot.pipe('pipe 3')

    t1.append('1', 'a').append('2', 'b')

    with t1:
        assert items_processed == 0
        assert t3.is_filled()

        t3.call(handler)
        assert exclude(t3.get_state(), 'id') == {
            'offset': 2,
            'source_id': t1.id,
            'target_id': t3.id,
        }

        assert items_processed == 2
        assert t3.is_filled() is False

    t1.append('3', 'c')

    with t1:
        assert items_processed == 2
        assert t3.is_filled()

        t3.call(handler)
        assert exclude(t3.get_state(), 'id') == {
            'offset': 3,
            'source_id': t1.id,
            'target_id': t3.id,
        }

        assert items_processed == 3
        assert t3.is_filled() is False
