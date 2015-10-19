import unittest

from databot import row
from databot.db.utils import Row
from databot.recursive import call


class RecursiveCall(unittest.TestCase):

    def test_nested_dict(self):
        source = {'value': {'number': row.value}}
        target = {'value': {'number': 42}}
        self.assertEqual(call(source, Row(value=42)), target)

    def test_list(self):
        source = {'value': [row.value, row.value]}
        target = {'value': [42, 42]}
        self.assertEqual(call(source, Row(value=42)), target)
