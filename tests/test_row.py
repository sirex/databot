import unittest

from databot import row
from databot.db.utils import Row


class RowTests(unittest.TestCase):

    def test_row(self):
        self.assertEqual(row(Row(key=1, value=2)), {'key': 1, 'value': 2})

    def test_key(self):
        self.assertEqual(row.key(Row(key=1, value=2)), 1)

    def test_value(self):
        self.assertEqual(row.value(Row(key=1, value=2)), 2)

    def test_value_item(self):
        self.assertEqual(row.value['x'](Row(key=1, value={'x': 3, 'y': 4})), 3)

    def test_value_multiple_items(self):
        self.assertEqual(row.value['x']['y']['z'](Row(key=1, value={'x': {'y': {'z': 42}}})), 42)

    def test_value_length(self):
        self.assertEqual(row.value['x'].length(Row(key=1, value={'x': 'abc'})), 3)
