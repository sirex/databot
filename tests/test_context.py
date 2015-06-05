import unittest
import databot


class TestBot(databot.Bot):
    db = 'sqlite:///:memory:'

    def task_task_1(self):
        pass

    def task_task_2(self):
        pass

    def init(self):
        self.define('task 1')
        self.define('task 2')


class ContextTests(unittest.TestCase):
    def setUp(self):
        self.bot = TestBot()
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
