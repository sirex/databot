import unittest

import databot.appender


class TestAppender(databot.appender.Appender):
    def __init__(self, storage, **kwargs):
        self.storage = storage
        super().__init__(**kwargs)

    def commit(self):
        self.storage.extend(self.data)


class SizeAppender(TestAppender):
    def size(self, value):
        return len(value)


class AppenderTests(unittest.TestCase):
    def test_watch(self):
        storage = {'data': [], 'errors': []}
        data = TestAppender(storage['data'], threshold=2)
        errors = TestAppender(storage['errors'], threshold=2)
        appender = databot.appender.Appender()
        appender.watch(data, errors)

        data.append(1)
        self.assertEqual(storage, {'data': [], 'errors': []})

        data.append(2)
        self.assertEqual(storage, {'data': [], 'errors': []})

        data.append(3)
        self.assertEqual(storage, {'data': [1, 2], 'errors': []})

        errors.append(1)
        self.assertEqual(storage, {'data': [1, 2], 'errors': []})

        appender.save()
        self.assertEqual(storage, {'data': [1, 2, 3], 'errors': [1]})

        errors.append(2)
        self.assertEqual(storage, {'data': [1, 2, 3], 'errors': [1]})

        errors.append(3)
        self.assertEqual(storage, {'data': [1, 2, 3], 'errors': [1]})

        errors.append(4)
        self.assertEqual(storage, {'data': [1, 2, 3], 'errors': [1, 2, 3]})

        appender.save()
        self.assertEqual(storage, {'data': [1, 2, 3], 'errors': [1, 2, 3, 4]})

    def test_watch_save(self):
        storage = {'data': [], 'errors': []}
        data = TestAppender(storage['data'], threshold=3)
        errors = TestAppender(storage['errors'], threshold=3)
        appender = databot.appender.Appender()
        appender.watch(data, errors)

        data.append(1)
        data.append(2)
        errors.append(1)
        errors.append(2)
        self.assertEqual(storage, {'data': [], 'errors': []})

        appender.save()
        self.assertEqual(storage, {'data': [1, 2], 'errors': [1, 2]})

    def test_size(self):
        storage = []
        data = SizeAppender(storage, threshold=4)

        data.append('a')
        self.assertEqual(storage, [])
        self.assertEqual(data.offset, 1)

        data.append('ab')
        self.assertEqual(storage, [])
        self.assertEqual(data.offset, 3)

        data.append('abc')
        self.assertEqual(storage, ['a', 'ab'])
        self.assertEqual(data.offset, 3)

        data.append('abcd')
        self.assertEqual(storage, ['a', 'ab', 'abc'])
        self.assertEqual(data.offset, 4)

        data.append('a')
        self.assertEqual(storage, ['a', 'ab', 'abc', 'abcd'])
        self.assertEqual(data.offset, 1)

        data.append('a')
        self.assertEqual(storage, ['a', 'ab', 'abc', 'abcd'])
        self.assertEqual(data.offset, 2)

        data.append('a')
        self.assertEqual(storage, ['a', 'ab', 'abc', 'abcd'])
        self.assertEqual(data.offset, 3)

        data.save()
        self.assertEqual(storage, ['a', 'ab', 'abc', 'abcd', 'a', 'a', 'a'])
        self.assertEqual(data.offset, 0)
