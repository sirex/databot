import unittest
import databot
import databot.tasks

import tests.db


class TestBot(databot.Bot):
    def task_task_1(self):
        pass

    def init(self):
        self.define('task 1')


class TaskTests(unittest.TestCase):
    def setUp(self):
        self.bot = TestBot('sqlite:///:memory:')
        self.bot.init()

    def test_str(self):
        self.assertEqual(str(self.bot.task('task 1')), 'task 1')

    def test_repr(self):
        task = self.bot.task('task 1')
        self.assertEqual(repr(task), '<databot.Task[%d]: task 1>' % task.id)


class DefineTaskTests(unittest.TestCase):
    def setUp(self):
        self.bot = TestBot('sqlite:///:memory:')

    def test_simple_define(self):
        self.bot.define('task 1')
        self.assertEqual(self.bot.task('task 1').name, 'task 1')

    def test_define_same_name(self):
        self.bot.define('task 1')
        self.assertRaises(ValueError, self.bot.define, 'task 1')

    def test_wrap(self):
        def handler(key):
            yield key

        def wrapper(handler, row):
            yield from handler(row.key.upper())

        t1 = self.bot.define('task 1', handler, wrap=wrapper)

        with self.bot.define('source', None).append(['a', 'b']):
            t1.run()

        self.assertEqual(list(t1.data.items()), [('A', None), ('B', None)])


class KeyValueItemsTests(unittest.TestCase):
    def test_key(self):
        self.assertEqual(list(databot.tasks.keyvalueitems(1)), [(1, None)])

    def test_key_and_value(self):
        self.assertEqual(list(databot.tasks.keyvalueitems(1, 'a')), [(1, 'a')])

    def test_empty_list(self):
        self.assertEqual(list(databot.tasks.keyvalueitems([])), [])

    def test_list_of_keys(self):
        self.assertEqual(list(databot.tasks.keyvalueitems([1])), [(1, None)])

    def test_list_of_keys_and_values(self):
        self.assertEqual(list(databot.tasks.keyvalueitems([(1, 'a')])), [(1, 'a')])

    def test_generator_of_keys(self):
        def generate():
            yield 1
        self.assertEqual(list(databot.tasks.keyvalueitems(generate())), [(1, None)])

    def test_generator_of_keys_and_values(self):
        def generate():
            yield 1, 'a'
        self.assertEqual(list(databot.tasks.keyvalueitems(generate())), [(1, 'a')])


@tests.db.usedb()
class TaskDataTests(object):
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
