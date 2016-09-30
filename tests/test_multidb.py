import pytest
import databot
import databot.pipes
import databot.testing


def handler(row):
    return row.key, row.value.upper()


def external_internal(db):
    external = databot.Bot('sqlite:///:memory:')
    external.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')])

    internal = db.Bot().main(argv=['-v0', 'run'])
    internal.define('p1', external.engine)
    internal.define('p2')

    return internal


def internal_external(db):
    external = databot.Bot('sqlite:///:memory:')
    external.define('p2')

    internal = db.Bot().main(argv=['-v0', 'run'])
    internal.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')])
    internal.define('p2', external.engine)

    return internal


def both_internal(db):
    bot = db.Bot().main(argv=['-v0', 'run'])
    bot.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')])
    bot.define('p2')
    return bot


def both_external(db):
    external1 = databot.Bot('sqlite:///:memory:')
    external1.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')])

    external2 = databot.Bot('sqlite:///:memory:')
    external2.define('p2')

    internal = db.Bot().main(argv=['-v0', 'run'])
    internal.define('p1', external1.engine)
    internal.define('p2', external2.engine)

    return internal


@pytest.fixture(params=[
    external_internal,
    internal_external,
    both_internal,
    both_external,
])
def bot(request, db):
    return request.param(db)


def test_call(bot):
    p1, p2 = bot.pipe('p1'), bot.pipe('p2')

    with p1:
        p2.call(handler)

    assert list(p2.data.items()) == [(1, 'A'), (2, 'B'), (3, 'C')]


def test_data(bot):
    p1 = bot.pipe('p1')

    assert list(p1.data.items()) == [(1, 'a'), (2, 'b'), (3, 'c')]


def test_is_filled(bot):
    p1, p2 = bot.pipe('p1'), bot.pipe('p2')

    with p1:
        assert p2.is_filled() is True
        p2.call(handler)
        assert p2.is_filled() is False


def test_last(bot):
    p1 = bot.pipe('p1')

    assert p1.last().value == 'c'


def test_skip(bot):
    p1, p2 = bot.pipe('p1'), bot.pipe('p2')

    with p1:
        assert p2.count() == 3
        assert p2.skip().count() == 0


def test_offset(bot):
    p1, p2 = bot.pipe('p1'), bot.pipe('p2')

    with p1:
        assert p2.count() == 3
        assert p2.offset(1).count() == 2


def test_errors(bot):
    p1, p2 = bot.pipe('p1'), bot.pipe('p2')

    handler = databot.testing.ErrorHandler(2)

    with p1:
        p2.call(handler)
        assert p2.errors.count() == 1
        assert list(p2.errors.keys()) == [2]

    assert list(p2.data.items()) == [(1, 'A'), (3, 'C')]

    handler = databot.testing.ErrorHandler(None)

    bot.main(argv=['-v0', 'run', '--retry'])

    with p1:
        p2.call(handler)
        assert p2.errors.count() == 0

    assert list(p2.data.items()) == [(1, 'A'), (3, 'C'), (2, 'B')]


def test_errors_with_key(bot):
    p1, p2 = bot.pipe('p1'), bot.pipe('p2')

    with p1:
        p2.call(databot.testing.ErrorHandler(2))
        errors = [err.row.value for err in p2.errors(2)]
        assert errors == ['b']


def test_errors_with_missing_key(bot):
    p1, p2 = bot.pipe('p1'), bot.pipe('p2')

    with p1:
        p2.call(databot.testing.ErrorHandler(2))
        errors = [err.row.value for err in p2.errors(42)]
        assert errors == []


def test_errors_reversed(bot):
    p1, p2 = bot.pipe('p1'), bot.pipe('p2')

    with p1:
        p2.call(databot.testing.ErrorHandler(2, 3))

        # Without reverse
        errors = [err.row.value for err in p2.errors(reverse=False)]
        assert errors == ['b', 'c']

        # Reverse
        errors = [err.row.value for err in p2.errors(reverse=True)]
        assert errors == ['c', 'b']


def test_errors_last(bot):
    p1, p2 = bot.pipe('p1'), bot.pipe('p2')

    with p1:
        p2.call(databot.testing.ErrorHandler(1, 2, 3))

        assert p2.errors.last().row.key == 3
        assert p2.errors.last(1).row.key == 1
        assert p2.errors.last(42) is None


def test_errors_resolve_all(bot):
    p1, p2 = bot.pipe('p1'), bot.pipe('p2')

    handler = databot.testing.ErrorHandler(2, 3)

    with p1:
        p2.call(handler)
        assert p2.errors.count() == 2
        assert list(p2.errors.keys()) == [2, 3]

        p2.errors.resolve()
        assert p2.errors.count() == 0
        assert list(p2.errors.keys()) == []


def test_errors_resolve_key(bot):
    p1, p2 = bot.pipe('p1'), bot.pipe('p2')

    handler = databot.testing.ErrorHandler(2, 3)

    with p1:
        p2.call(handler)
        assert p2.errors.count() == 2
        assert list(p2.errors.keys()) == [2, 3]

        p2.errors.resolve(2)
        assert p2.errors.count() == 1
        assert list(p2.errors.keys()) == [3]


def test_missing_pipe_name(db):
    external = databot.Bot('sqlite:///:memory:')
    external.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')])

    bot = db.Bot().main(argv=['-v0', 'run'])
    bot.define('pp', external.engine)
    bot.define('p2')

    pp, p2 = bot.pipe('pp'), bot.pipe('p2')

    with pp:
        assert p2.count() is 0


def test_external_write(db):
    external = databot.Bot('sqlite:///:memory:')
    external.define('p1')

    bot = db.Bot().main(argv=['-v0', 'run'])
    bot.define('p1', external.engine)
    bot.define('p2').append([(1, 'a'), (2, 'b'), (3, 'c')])

    p1, p2 = bot.pipe('p1'), bot.pipe('p2')

    with p2:
        p1.call(handler)

    assert list(p1.data.items()) == [(1, 'A'), (2, 'B'), (3, 'C')]
    assert list(p2.data.items()) == [(1, 'a'), (2, 'b'), (3, 'c')]
    assert list(external.pipe('p1').data.items()) == [(1, 'A'), (2, 'B'), (3, 'C')]
