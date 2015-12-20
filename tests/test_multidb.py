import databot
import databot.pipes
import databot.testing

import tests.db


def handler(row):
    return row.key, row.value.upper()


class MultiDB(object):

    def test_call(self):
        p1, p2 = self.bot.pipe('p1'), self.bot.pipe('p2')

        with p1:
            p2.call(handler)

        self.assertEqual(list(p2.data.items()), [(1, 'A'), (2, 'B'), (3, 'C')])

    def test_data(self):
        p1 = self.bot.pipe('p1')

        self.assertEqual(list(p1.data.items()), [(1, 'a'), (2, 'b'), (3, 'c')])

    def test_is_filled(self):
        p1, p2 = self.bot.pipe('p1'), self.bot.pipe('p2')

        with p1:
            self.assertTrue(p2.is_filled())
            p2.call(handler)
            self.assertFalse(p2.is_filled())

    def test_last(self):
        p1 = self.bot.pipe('p1')

        self.assertEqual(p1.last().value, 'c')

    def test_skip(self):
        p1, p2 = self.bot.pipe('p1'), self.bot.pipe('p2')

        with p1:
            self.assertEqual(p2.count(), 3)
            self.assertEqual(p2.skip().count(), 0)

    def test_offset(self):
        p1, p2 = self.bot.pipe('p1'), self.bot.pipe('p2')

        with p1:
            self.assertEqual(p2.count(), 3)
            self.assertEqual(p2.offset(1).count(), 2)

    def test_errors(self):
        p1, p2 = self.bot.pipe('p1'), self.bot.pipe('p2')

        handler = databot.testing.ErrorHandler(2)

        with p1:
            p2.call(handler)
            self.assertEqual(p2.errors.count(), 1)
            self.assertEqual(list(p2.errors.keys()), [2])

        self.assertEqual(list(p2.data.items()), [(1, 'A'), (3, 'C')])

        handler = databot.testing.ErrorHandler(None)

        self.bot.main(argv=['-v0', 'run', '--retry'])

        with p1:
            p2.call(handler)
            self.assertEqual(p2.errors.count(), 0)

        self.assertEqual(list(p2.data.items()), [(1, 'A'), (3, 'C'), (2, 'B')])

    def test_errors_with_key(self):
        p1, p2 = self.bot.pipe('p1'), self.bot.pipe('p2')

        with p1:
            p2.call(databot.testing.ErrorHandler(2))
            errors = [err.row.value for err in p2.errors(2)]
            self.assertEqual(errors, ['b'])

    def test_errors_with_missing_key(self):
        p1, p2 = self.bot.pipe('p1'), self.bot.pipe('p2')

        with p1:
            p2.call(databot.testing.ErrorHandler(2))
            errors = [err.row.value for err in p2.errors(42)]
            self.assertEqual(errors, [])

    def test_errors_reversed(self):
        p1, p2 = self.bot.pipe('p1'), self.bot.pipe('p2')

        with p1:
            p2.call(databot.testing.ErrorHandler(2, 3))

            # Without reverse
            errors = [err.row.value for err in p2.errors(reverse=False)]
            self.assertEqual(errors, ['b', 'c'])

            # Reverse
            errors = [err.row.value for err in p2.errors(reverse=True)]
            self.assertEqual(errors, ['c', 'b'])

    def test_errors_last(self):
        p1, p2 = self.bot.pipe('p1'), self.bot.pipe('p2')

        with p1:
            p2.call(databot.testing.ErrorHandler(1, 2, 3))

            self.assertEqual(p2.errors.last().row.key, 3)
            self.assertEqual(p2.errors.last(1).row.key, 1)
            self.assertEqual(p2.errors.last(42), None)

    def test_errors_resolve_all(self):
        p1, p2 = self.bot.pipe('p1'), self.bot.pipe('p2')

        handler = databot.testing.ErrorHandler(2, 3)

        with p1:
            p2.call(handler)
            self.assertEqual(p2.errors.count(), 2)
            self.assertEqual(list(p2.errors.keys()), [2, 3])

            p2.errors.resolve()
            self.assertEqual(p2.errors.count(), 0)
            self.assertEqual(list(p2.errors.keys()), [])

    def test_errors_resolve_key(self):
        p1, p2 = self.bot.pipe('p1'), self.bot.pipe('p2')

        handler = databot.testing.ErrorHandler(2, 3)

        with p1:
            p2.call(handler)
            self.assertEqual(p2.errors.count(), 2)
            self.assertEqual(list(p2.errors.keys()), [2, 3])

            p2.errors.resolve(2)
            self.assertEqual(p2.errors.count(), 1)
            self.assertEqual(list(p2.errors.keys()), [3])


@tests.db.usedb()
class ExternalInternalTests(MultiDB):

    def setUp(self):
        super().setUp()

        external = databot.Bot('sqlite:///:memory:')
        external.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')])

        self.bot = databot.Bot(self.db.engine, models=self.db.models).main(argv=['-v0', 'run'])
        self.bot.define('p1', external.engine)
        self.bot.define('p2')


@tests.db.usedb()
class InternalExternalTests(MultiDB):

    def setUp(self):
        super().setUp()

        external = databot.Bot('sqlite:///:memory:')
        external.define('p2')

        self.bot = databot.Bot(self.db.engine, models=self.db.models).main(argv=['-v0', 'run'])
        self.bot.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')])
        self.bot.define('p2', external.engine)


@tests.db.usedb()
class BothInternalTests(MultiDB):

    def setUp(self):
        super().setUp()

        self.bot = databot.Bot(self.db.engine, models=self.db.models).main(argv=['-v0', 'run'])
        self.bot.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')])
        self.bot.define('p2')


@tests.db.usedb()
class BothExternalTests(MultiDB):

    def setUp(self):
        super().setUp()

        external1 = databot.Bot('sqlite:///:memory:')
        external1.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')])

        external2 = databot.Bot('sqlite:///:memory:')
        external2.define('p2')

        self.bot = databot.Bot(self.db.engine, models=self.db.models).main(argv=['-v0', 'run'])
        self.bot.define('p1', external1.engine)
        self.bot.define('p2', external2.engine)


@tests.db.usedb()
class MultidbDefineTests(object):

    def test_missing_pipe_name(self):
        external = databot.Bot('sqlite:///:memory:')
        external.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c')])

        bot = databot.Bot(self.db.engine, models=self.db.models).main(argv=['-v0', 'run'])
        bot.define('pp', external.engine)
        bot.define('p2')

        pp, p2 = bot.pipe('pp'), bot.pipe('p2')

        with pp:
            self.assertEqual(p2.count(), 0)

    def test_external_write(self):
        external = databot.Bot('sqlite:///:memory:')
        external.define('p1')

        bot = databot.Bot(self.db.engine, models=self.db.models).main(argv=['-v0', 'run'])
        bot.define('p1', external.engine)
        bot.define('p2').append([(1, 'a'), (2, 'b'), (3, 'c')])

        p1, p2 = bot.pipe('p1'), bot.pipe('p2')

        with p2:
            p1.call(handler)

        self.assertEqual(list(p1.data.items()), [(1, 'A'), (2, 'B'), (3, 'C')])
        self.assertEqual(list(p2.data.items()), [(1, 'a'), (2, 'b'), (3, 'c')])
        self.assertEqual(list(external.pipe('p1').data.items()), [(1, 'A'), (2, 'B'), (3, 'C')])
