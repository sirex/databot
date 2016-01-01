import unittest

from databot.shell import name_to_attr


class NameToAttrTests(unittest.TestCase):

    def test_name_to_attr(self):
        self.assertEqual(name_to_attr('name'), 'name')
        self.assertEqual(name_to_attr('two words'), 'two_words')
        self.assertEqual(name_to_attr('lietuviškas žodis'), 'lietuviskas_zodis')
        self.assertEqual(name_to_attr('42 name'), '_42_name')
