import unittest
import databot

from databot.db.utils import Row
from databot.exporters.csv import get_fields, get_values, flatten_rows


class FlatteningTests(unittest.TestCase):
    def setUp(self):
        self.data = {
            'a': 1,
            'b': 2,
            'c': {
                'x': 1,
                'y': 2,
                'z': ['foo', 'bar', 'baz'],
            }
        }

    def test_get_fields(self):
        self.assertEqual(get_fields(self.data), [
            ('a',),
            ('b',),
            ('c', 'x'),
            ('c', 'y'),
            ('c', 'z'),
        ])

    def test_get_values(self):
        fields = [
            ('a',),
            ('b',),
            ('c', 'x'),
            ('c', 'z'),
        ]
        self.assertEqual(get_values(fields, self.data), (1, 2, 1, ['foo', 'bar', 'baz']))

    def test_missing_value(self):
        fields = [
            ('a',),
            ('z',),
        ]
        self.assertEqual(get_values(fields, self.data), (1, None))

    def test_update(self):
        rows = [
            Row(key=1, value={'text': 'abc'}),
            Row(key=1, value={'text': 'abcde'}),
        ]
        update = {'size': databot.row.value['text'].length}
        self.assertEqual(list(flatten_rows(rows, include=['key', 'size'], update=update)), [
            ['key', 'size'],
            [1, 3],
            [1, 5],
        ])
