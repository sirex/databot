import io
import mock
import unittest
import databot
import databot.pipes

import tests.db


class TestBot(databot.Bot):
    def init(self):
        self.define('pipe 1')


class PipeTests(unittest.TestCase):
    def setUp(self):
        self.bot = TestBot('sqlite:///:memory:')
        self.bot.init()

    def test_str(self):
        self.assertEqual(str(self.bot.pipe('pipe 1')), 'pipe 1')

    def test_repr(self):
        pipe = self.bot.pipe('pipe 1')
        self.assertEqual(repr(pipe), '<databot.Pipe[%d]: pipe 1>' % pipe.id)


class DefinePipeTests(unittest.TestCase):
    def setUp(self):
        self.bot = TestBot('sqlite:///:memory:')

    def test_simple_define(self):
        self.bot.define('pipe 1')
        self.assertEqual(self.bot.pipe('pipe 1').name, 'pipe 1')

    def test_define_same_name(self):
        self.bot.define('pipe 1')
        self.assertRaises(ValueError, self.bot.define, 'pipe 1')


class KeyValueItemsTests(unittest.TestCase):
    def test_key(self):
        self.assertEqual(list(databot.pipes.keyvalueitems(1)), [(1, None)])

    def test_key_and_value(self):
        self.assertEqual(list(databot.pipes.keyvalueitems(1, 'a')), [(1, 'a')])

    def test_empty_list(self):
        self.assertEqual(list(databot.pipes.keyvalueitems([])), [])

    def test_list_of_keys(self):
        self.assertEqual(list(databot.pipes.keyvalueitems([1])), [(1, None)])

    def test_list_of_keys_and_values(self):
        self.assertEqual(list(databot.pipes.keyvalueitems([(1, 'a')])), [(1, 'a')])

    def test_generator_of_keys(self):
        def generate():
            yield 1
        self.assertEqual(list(databot.pipes.keyvalueitems(generate())), [(1, None)])

    def test_generator_of_keys_and_values(self):
        def generate():
            yield 1, 'a'
        self.assertEqual(list(databot.pipes.keyvalueitems(generate())), [(1, 'a')])


@tests.db.usedb()
class PipeDataTests(object):
    def setUp(self):
        super().setUp()
        self.bot = TestBot(self.db.engine)
        self.t1 = self.bot.define('t1', None).append([('1', 'a'), ('2', 'b')])

    def test_keys(self):
        self.assertEqual(list(self.t1.data.keys()), ['1', '2'])

    def test_values(self):
        self.assertEqual(list(self.t1.data.values()), ['a', 'b'])

    def test_items(self):
        self.assertEqual(list(self.t1.data.items()), [('1', 'a'), ('2', 'b')])

    def test_rows(self):
        self.assertEqual([(row.key, row.value) for row in self.t1.data.rows()], [('1', 'a'), ('2', 'b')])

    def test_exists(self):
        self.assertTrue(self.t1.data.exists('1'))
        self.assertFalse(self.t1.data.exists('3'))


class AppendTests(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self.bot = databot.Bot('sqlite:///:memory:', output=self.output)
        self.bot.args = mock.Mock(verbosity=1, debug=False)
        self.pipe = self.bot.define('p1')

    def test_progress_bar(self):
        self.pipe.append([1, 2, 3], progress='p1')
        self.assertRegex(self.output.getvalue(), (
            '^'
            '\rp1: 0it \\[00:00, \\?it/s]'
            '\rp1: 3it \\[00:00, [0-9.]+it/s]'
            '\n'
            '$'
        ))

    def test_progress_bar_with_total(self):
        self.pipe.append([1, 2, 3], progress='p1', total=3)
        self.assertRegex(self.output.getvalue(), (
            '^'
            '\rp1:   0%\\|          \\| 0/3 \\[00:00<\\?, \\?it/s]'
            '\rp1: 100%\\|##########\\| 3/3 \\[00:00<00:00, [0-9.]+it/s]'
            '\n'
            '$'
        ))

    def test_only_missing(self):
        # only_missing = False
        self.pipe.append([1, 2, 3])
        self.pipe.append([1, 2, 3, 4])
        self.assertEqual(list(self.pipe.data.keys()), ['1', '2', '3', '1', '2', '3', '4'])

        # only_missing = True
        self.pipe.clean()
        self.pipe.append([1, 2, 3], only_missing=True)
        self.pipe.append([1, 2, 3, 4], only_missing=True)
        self.assertEqual(list(self.pipe.data.keys()), ['1', '2', '3', '4'])
