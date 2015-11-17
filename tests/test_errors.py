import databot

import tests.db


class ErrorHandler(object):
    def __init__(self, error_key):
        self.error_key = error_key

    def __call__(self, row):
        if row.key == self.error_key:
            raise ValueError('Error.')
        else:
            yield row.key, row.value.upper()


@tests.db.usedb()
class ErrorHandlingTests(object):
    def test_main(self):
        t2 = ErrorHandler('2')

        bot = databot.Bot(self.db.engine)
        bot.define('t1')
        bot.define('t2')
        bot.pipe('t1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])

        bot.main(argv=['-v0', 'run'])
        with bot.pipe('t1'):
            bot.pipe('t2').call(t2)
            self.assertEqual(bot.pipe('t2').errors.count(), 1)
            self.assertEqual(list(bot.pipe('t2').errors.keys()), ['2'])
            self.assertEqual(list(bot.pipe('t2').data.items()), [('1', 'A'), ('3', 'C')])

        bot.main(argv=['-v0', 'run', '--retry'])
        with bot.pipe('t1'):
            bot.pipe('t2').call(t2)
            self.assertEqual(bot.pipe('t2').errors.count(), 1)
            self.assertEqual(list(bot.pipe('t2').errors.keys()), ['2'])
            self.assertEqual(list(bot.pipe('t2').data.items()), [('1', 'A'), ('3', 'C')])

        t2.error_key = None
        bot.main(argv=['-v0', 'run', '--retry'])
        with bot.pipe('t1'):
            bot.pipe('t2').call(t2)
            self.assertEqual(bot.pipe('t2').errors.count(), 0)
            self.assertEqual(list(bot.pipe('t2').errors.keys()), [])
            self.assertEqual(list(bot.pipe('t2').data.items()), [('1', 'A'), ('3', 'C'), ('2', 'B')])


@tests.db.usedb()
class RetryTests(object):
    def test_retry_query(self):
        error_keys = {'1', '3'}

        def t2(row):
            nonlocal error_keys
            if row.key in error_keys:
                raise ValueError('Error.')
            else:
                yield row.key, row.value.upper()

        bot = databot.Bot(self.db.engine)
        bot.define('t1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])
        bot.define('t2')

        bot.main(argv=['-v0', 'run'])
        with bot.pipe('t1'):
            bot.pipe('t2').call(t2)
            self.assertEqual(list(bot.pipe('t2').errors.keys()), ['1', '3'])

        self.assertEqual([(error.source.pipe, error.target.pipe) for error in bot.query_retry_pipes()], [
            ('t1', 't2'),
            ('t1', 't2'),
        ])
        self.assertEqual(list(bot.pipe('t2').data.items()), [('2', 'B')])

        error_keys = {}
        bot.main(argv=['-v0', 'run', '--retry'])
        with bot.pipe('t1'):
            bot.pipe('t2').call(t2)

        self.assertEqual(list(bot.pipe('t2').data.items()), [('2', 'B'), ('1', 'A'), ('3', 'C')])


@tests.db.usedb()
class ErrorDataTests(object):
    def setUp(self):
        super().setUp()
        self.bot = databot.Bot(self.db.engine)
        self.t1 = self.bot.define('pipe 1', None).append([('1', 'a'), ('2', 'b'), ('3', 'c')])
        self.t2 = self.bot.define('pipe 2', None)

        rows = list(self.t1.data.rows())
        with self.t1:
            self.t2.errors.report(rows[0], 'Error 1')
            self.t2.errors.report(rows[2], 'Error 2')

    def test_count_without_source(self):
        self.assertEqual(self.t2.errors.count(), 0)

    def test_count_with_source(self):
        with self.t1:
            self.assertEqual(self.t2.errors.count(), 2)

    def test_keys(self):
        with self.t1:
            self.assertEqual(list(self.t2.errors.keys()), ['1', '3'])

    def test_values(self):
        with self.t1:
            self.assertEqual(list(self.t2.errors.values()), ['a', 'c'])

    def test_items(self):
        with self.t1:
            self.assertEqual(list(self.t2.errors.items()), [('1', 'a'), ('3', 'c')])

    def test_rows(self):
        with self.t1:
            self.assertEqual([(row.key, row.value) for row in self.t2.errors.rows()], [('1', 'a'), ('3', 'c')])

    def test_resolve_all(self):
        with self.t1:
            self.t2.errors.resolve()
            self.assertEqual(list(self.t2.errors.keys()), [])

    def test_resolve_key(self):
        with self.t1:
            self.t2.errors.resolve(1)
            self.assertEqual(list(self.t2.errors.keys()), ['3'])
