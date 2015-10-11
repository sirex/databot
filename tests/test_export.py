import json
import unittest


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
