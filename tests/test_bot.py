import unittest
import sqlalchemy as sa

from databot import Bot


class BotInitTests(unittest.TestCase):

    def test_init(self):
        Bot('sqlite:///:memory:')

    def test_init_with_engine_instance(self):
        Bot(sa.create_engine('sqlite:///:memory:'))


class BotTests(unittest.TestCase):

    def setUp(self):
        self.bot = Bot('sqlite:///:memory:')
        self.engine = self.bot.engine

    def test_define(self):
        bot, execute, pipes, pipe = self.bot, self.engine.execute, self.bot.models.pipes, self.bot.models.pipes.c.pipe
        bot.define('pipe')
        row = execute(pipes.select(pipe == 'pipe')).fetchone()
        self.assertEqual(row['bot'], 'databot.bot.Bot')

    def test_define_duplicate(self):
        self.bot.define('pipe')
        self.assertRaises(ValueError, self.bot.define, 'pipe')

    def test_pipe(self):
        self.bot.define('pipe')
        pipe = self.bot.pipe('pipe')
        self.assertEqual(pipe.name, 'pipe')

    def test_compact_empty_pipe(self):
        pipe = self.bot.define('pipe')
        self.bot.compact()
        self.assertEqual(list(pipe.data.keys()), [])

    def test_compact_full_pipe(self):
        pipe = self.bot.define('pipe')
        pipe.append([1, 1, 2, 1, 1])
        self.assertEqual(list(pipe.data.keys()), [1, 1, 2, 1, 1])

        self.bot.compact()
        self.assertEqual(list(pipe.data.keys()), [2, 1])
