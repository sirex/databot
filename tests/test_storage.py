import unittest
import datetime
import databot


def exclude(data, keys):
    return {k: v for k, v in data.items() if k not in keys}


class TestBot(databot.Bot):
    db = 'sqlite:///:memory:'

    def task_task_1(self):
        pass

    def task_task_2(self):
        pass

    def init(self):
        self.define('task 1')
        self.define('task 2')


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.bot = TestBot()
        self.bot.init()

    def test_append(self):
        t1 = self.bot.task('task 1')
        t1.append('foo', 'bar').append('a', 'b')
        data = [(row.key, row.value) for row in t1.table.all()]
        self.assertEqual(data, [('foo', b'"bar"'), ('a', b'"b"')])

    def test_clean(self):
        t1 = self.bot.task('task 1')
        t2 = self.bot.task('task 2')

        day = datetime.timedelta(days=1)
        now = datetime.datetime(2015, 6, 1, 1, 1, 0)
        t1.table.insert(dict(key='1', value=databot.dumps('a'), created=now - 1*day))
        t1.table.insert(dict(key='2', value=databot.dumps('b'), created=now - 2*day))
        t1.table.insert(dict(key='3', value=databot.dumps('c'), created=now - 3*day))

        with t1:
            self.assertEqual(t2.count(), 3)

            t1.clean(3*day, now=now)
            self.assertEqual(t2.count(), 2)
            self.assertEqual(list(map(int, t2.keys())), [1, 2])

            t1.clean(2*day, now=now)
            self.assertEqual(t2.count(), 1)
            self.assertEqual(list(map(int, t2.keys())), [1])

            t1.clean()
            self.assertEqual(t2.count(), 0)

    def test_dedup(self):
        t1 = self.bot.task('task 1')
        t1.append('1', 'a')
        t1.append('2', 'b')
        t1.append('2', 'c')
        t1.append('3', 'd')
        t1.append('3', 'e')
        self.assertEqual(t1.table.count(), 5)
        self.assertEqual(t1.dedup().table.count(), 3)
        self.assertEqual([(row.key, databot.loads(row.value)) for row in t1.table.all()], [
            ('1', 'a'),
            ('2', 'b'),
            ('3', 'd'),
        ])

    def test_compact(self):
        t1 = self.bot.task('task 1')
        t1.append('1', 'a')
        t1.append('2', 'b')
        t1.append('2', 'c')
        t1.append('3', 'd')
        t1.append('3', 'e')
        self.assertEqual(t1.table.count(), 5)
        self.assertEqual(t1.compact().table.count(), 3)
        self.assertEqual([(row.key, databot.loads(row.value)) for row in t1.table.all()], [
            ('1', 'a'),
            ('2', 'c'),
            ('3', 'e'),
        ])

        t1.append('3', 'x')
        self.assertEqual(t1.table.count(), 4)

        self.bot.compact()
        self.assertEqual(t1.table.count(), 3)


class StateTests(unittest.TestCase):
    def setUp(self):
        self.bot = TestBot()
        self.bot.init()

    def test_initial_state(self):
        t1 = self.bot.task('task 1')
        t2 = self.bot.task('task 2')

        self.assertEqual(exclude(t1.get_state(), 'id'), {
            'offset': 0,
            'source': None,
            'target': 'task 1',
            'bot': 'tests.test_storage.TestBot',
        })

        self.assertEqual(exclude(t2.get_state(), 'id'), {
            'offset': 0,
            'source': None,
            'target': 'task 2',
            'bot': 'tests.test_storage.TestBot',
        })

    def test_context_state(self):
        t1 = self.bot.task('task 1')
        t2 = self.bot.task('task 2')

        with t1:
            self.assertEqual(exclude(t2.get_state(), 'id'), {
                'offset': 0,
                'source': 'task 1',
                'target': 'task 2',
                'bot': 'tests.test_storage.TestBot',
            })

    def test_offset(self):
        items_processed = 0

        def handler(item):
            nonlocal items_processed
            items_processed += 1
            yield item.key, item.value

        self.bot.define('task 3', handler)

        t1 = self.bot.task('task 1')
        t3 = self.bot.task('task 3')

        t1.append('1', 'a').append('2', 'b')

        with t1:
            self.assertEqual(items_processed, 0)
            self.assertTrue(t3.is_filled())

            t3.run()
            self.assertEqual(exclude(t3.get_state(), 'id'), {
                'offset': 2,
                'source': 'task 1',
                'target': 'task 3',
                'bot': 'tests.test_storage.TestBot',
            })

            self.assertEqual(items_processed, 2)
            self.assertFalse(t3.is_filled())

        t1.append('3', 'c')

        with t1:
            self.assertEqual(items_processed, 2)
            self.assertTrue(t3.is_filled())

            t3.run()
            self.assertEqual(exclude(t3.get_state(), 'id'), {
                'offset': 3,
                'source': 'task 1',
                'target': 'task 3',
                'bot': 'tests.test_storage.TestBot',
            })

            self.assertEqual(items_processed, 3)
            self.assertFalse(t3.is_filled())
