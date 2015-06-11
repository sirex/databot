import unittest
import databot


class TestBot(databot.Bot):
    def task_task_1(self):
        pass

    def task_task_2(self):
        pass

    def init(self):
        self.define('task 1')
        self.define('task 2')


class ContextTests(unittest.TestCase):
    def setUp(self):
        self.bot = TestBot('sqlite:///:memory:')
        self.bot.init()

    def test_context(self):
        self.assertTrue(self.bot.task('task 1').source is None)
        self.assertTrue(self.bot.task('task 2').source is None)

        with self.bot.task('task 1'):
            self.assertEqual(self.bot.task('task 1').source.name, 'task 1')
            self.assertEqual(self.bot.task('task 2').source.name, 'task 1')

            with self.bot.task('task 2'):
                self.assertEqual(self.bot.task('task 1').source.name, 'task 2')

            self.assertEqual(self.bot.task('task 1').source.name, 'task 1')
            self.assertEqual(self.bot.task('task 2').source.name, 'task 1')

        self.assertTrue(self.bot.task('task 1').source is None)
        self.assertTrue(self.bot.task('task 2').source is None)


class DataTests(unittest.TestCase):
    def setUp(self):
        self.bot = TestBot('sqlite:///:memory:')
        self.t1 = self.bot.define('task 1').append([('1', 'a'), ('2', 'b')])
        self.t2 = self.bot.define('task 2')

    def test_count_with_context(self):
        self.assertEqual(self.t2.count(), 0)

    def test_count_without_context(self):
        with self.t1:
            self.assertEqual(self.t2.count(), 2)

    def test_is_filled_with_source(self):
        with self.t1:
            self.assertEqual(self.t2.is_filled(), True)

    def test_is_filled_without_source(self):
        self.assertEqual(self.t2.is_filled(), False)

    def test_keys(self):
        with self.t1:
            self.assertEqual(list(self.t2.keys()), ['1', '2'])

    def test_values(self):
        with self.t1:
            self.assertEqual(list(self.t2.values()), ['a', 'b'])

    def test_items(self):
        with self.t1:
            self.assertEqual(list(self.t2.items()), [('1', 'a'), ('2', 'b')])

    def test_rows(self):
        with self.t1:
            self.assertEqual([(row.key, row.value) for row in self.t2.rows()], [('1', 'a'), ('2', 'b')])
