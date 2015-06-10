import unittest
import databot


class TestBot(databot.Bot):
    db = 'sqlite:///:memory:'


class ErrorHandlingTests(unittest.TestCase):
    def test_main(self):
        error_key = '2'

        def t2(row):
            nonlocal error_key
            if row.key == error_key:
                raise ValueError('Error.')
            else:
                yield row.key, row.value.upper()

        bot = TestBot()
        bot.define('t1', None).append([('1', 'a'), ('2', 'b'), ('3', 'c')])
        bot.define('t2', t2)

        with bot.task('t1'):
            bot.task('t2').run()
            self.assertEqual(bot.task('t2').errors.count(), 1)
            self.assertEqual(list(bot.task('t2').errors.keys()), ['2'])
            self.assertEqual(list(bot.task('t2').data.items()), [('1', 'A'), ('3', 'C')])

        with bot.task('t1'):
            bot.task('t2').retry()
            self.assertEqual(bot.task('t2').errors.count(), 1)
            self.assertEqual(list(bot.task('t2').errors.keys()), ['2'])
            self.assertEqual(list(bot.task('t2').data.items()), [('1', 'A'), ('3', 'C')])

        error_key = None

        with bot.task('t1'):
            bot.task('t2').retry()
            self.assertEqual(bot.task('t2').errors.count(), 0)
            self.assertEqual(list(bot.task('t2').errors.keys()), [])
            self.assertEqual(list(bot.task('t2').data.items()), [('1', 'A'), ('3', 'C'), ('2', 'B')])


class RetryTests(unittest.TestCase):
    def test_retry_query(self):
        error_keys = {'1', '3'}

        def t2(row):
            nonlocal error_keys
            if row.key in error_keys:
                raise ValueError('Error.')
            else:
                yield row.key, row.value.upper()

        bot = TestBot()
        bot.define('t1', None).append([('1', 'a'), ('2', 'b'), ('3', 'c')])
        bot.define('t2', t2)

        with bot.task('t1'):
            bot.task('t2').run()
            self.assertEqual(list(bot.task('t2').errors.keys()), ['1', '3'])

        self.assertEqual([(error.source.task, error.target.task) for error in bot.query_retry_tasks()], [
            ('t1', 't2'),
            ('t1', 't2'),
        ])
        self.assertEqual(list(bot.task('t2').data.items()), [('2', 'B')])

        error_keys = {}
        bot.retry()

        self.assertEqual(list(bot.task('t2').data.items()), [('2', 'B'), ('1', 'A'), ('3', 'C')])


class ErrorDataTests(unittest.TestCase):
    def setUp(self):
        self.bot = TestBot()
        self.t1 = self.bot.define('task 1', None).append([('1', 'a'), ('2', 'b'), ('3', 'c')])
        self.t2 = self.bot.define('task 2', None)

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
