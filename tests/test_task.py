import unittest
import databot


class TestBot(databot.Bot):
    db = 'sqlite:///:memory:'

    def task_task_1(self):
        pass

    def init(self):
        self.define('task 1')


class TaskTests(unittest.TestCase):
    def setUp(self):
        self.bot = TestBot()
        self.bot.init()

    def test_str(self):
        self.assertEqual(str(self.bot.task('task 1')), 'task 1')

    def test_repr(self):
        task = self.bot.task('task 1')
        self.assertEqual(repr(task), '<databot.Task[%d]: task 1>' % task.id)
