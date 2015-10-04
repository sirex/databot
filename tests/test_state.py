import unittest
import databot
import databot.pipes


class DefinePipeTests(unittest.TestCase):
    def setUp(self):
        self.bot = bot = databot.Bot('sqlite:///:memory:')
        bot.define('a').append([1, 2, 3, 4, 5])
        bot.define('b')

    def test_offset_skip_reset(self):
        keys = lambda p: list(map(int, p.keys()))
        with self.bot.pipe('a'):
            b = self.bot.pipe('b')
            self.assertEqual(keys(b), [1, 2, 3, 4, 5])
            self.assertEqual(keys(b.offset(3)), [4, 5])
            self.assertEqual(keys(b.offset(-2)), [2, 3, 4, 5])
            self.assertEqual(keys(b.offset(1)), [3, 4, 5])
            self.assertEqual(keys(b.offset(0)), [3, 4, 5])
            self.assertEqual(keys(b.offset(10)), [])
            self.assertEqual(keys(b.offset(-10)), [1, 2, 3, 4, 5])
            self.assertEqual(keys(b.skip()), [])
            self.assertEqual(keys(b.reset()), [1, 2, 3, 4, 5])
            self.assertEqual(keys(b.skip()), [])
            self.assertEqual(keys(b.reset()), [1, 2, 3, 4, 5])
            self.assertEqual(keys(b.offset(+1)), [2, 3, 4, 5])
            self.assertEqual(keys(b.offset(+1)), [3, 4, 5])
            self.assertEqual(keys(b.offset(+1)), [4, 5])
            self.assertEqual(keys(b.offset(+1)), [5])
            self.assertEqual(keys(b.offset(+1)), [])
            self.assertEqual(keys(b.offset(+1)), [])
            self.assertEqual(keys(b.offset(-1)), [5])
            self.assertEqual(keys(b.offset(-1)), [4, 5])
            self.assertEqual(keys(b.offset(-1)), [3, 4, 5])
            self.assertEqual(keys(b.offset(-1)), [2, 3, 4, 5])
            self.assertEqual(keys(b.offset(-1)), [1, 2, 3, 4, 5])
            self.assertEqual(keys(b.offset(-1)), [1, 2, 3, 4, 5])
