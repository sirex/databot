import unittest

from databot.db.serializers import serkey


class SerkeyTests(unittest.TestCase):

    def test_serkey(self):
        self.assertEqual(serkey(1), 'bf8b4530d8d246dd74ac53a13471bba17941dff7')
        self.assertEqual(serkey('1'), 'deb12f0578a40628ec941aa2bd60d7a838765ed0')
        self.assertEqual(serkey([1, 2]), 'a4001841d163db31660e03679efe46d9f99a54eb')
        self.assertEqual(serkey((1, 2)), 'a4001841d163db31660e03679efe46d9f99a54eb')
        self.assertRaises(AssertionError, serkey, {1, 2})
        self.assertRaises(AssertionError, serkey, {1: 2})
