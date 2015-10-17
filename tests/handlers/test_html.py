import unittest
import databot
import databot.pipes

from databot import call, value
from databot.handlers import html


class HtmlCssSelectTests(unittest.TestCase):
    def setUp(self):
        bot = databot.Bot('sqlite:///:memory:')
        key = 'http://exemple.com'
        value = {'text': (
            '<p id="a">value-a</p>'
            '<p id="b">value-b</p>'
            '<p id="c">value-c</p>'
        )}
        self.row, = bot.define('a').append(key, value).data.rows()

    def test_key_value(self):
        select = html.Select('#a@id', 'p#a:text')
        self.assertEqual(select(self.row), [('a', 'value-a')])

    def test_only_keys(self):
        select = html.Select(['p@id'])
        self.assertEqual(select(self.row), ['a', 'b', 'c'])

    def test_dict(self):
        select = html.Select('#a@id', {'b': 'p#b:text', 'c': 'p#c:text'})
        self.assertEqual(select(self.row), [('a', {'b': 'value-b', 'c': 'value-c'})])

    def test_list(self):
        select = html.Select(None, ['p:text'])
        self.assertEqual(select(self.row), [(None, ['value-a', 'value-b', 'value-c'])])

    def test_not_fould_value_error(self):
        select = html.Select(None, '#missing')
        self.assertRaises(ValueError, select, self.row)

    def test_multiple_values_error(self):
        select = html.Select(None, 'p:text')
        self.assertRaises(ValueError, select, self.row)

    def test_dict_and_list(self):
        select = html.Select(None, {'a': ['p:text']})
        self.assertEqual(select(self.row), [(None, {'a': ['value-a', 'value-b', 'value-c']})])

    def test_nth_child(self):
        # `p[1]` should be replaced to `p:nth-child(1)`
        select = html.Select(None, {'a': 'p[1]:text', 'c': 'p[3]:text'})
        self.assertEqual(select(self.row), [(None, {'a': 'value-a', 'c': 'value-c'})])


class HtmlCssSelectTailTests(unittest.TestCase):
    def setUp(self):
        bot = databot.Bot('sqlite:///:memory:')
        key = 'http://exemple.com'
        value = {'text': '\n'.join([
            '<div>',
            '  <a href="#">link</a>t1<br>t2<br>t3',
            '</div>',
        ])}
        self.row, = bot.define('a').append(key, value).data.rows()

    def test_tail_select(self):
        select = html.Select(None, ['div *:tail'])
        self.assertEqual(select(self.row), [(None, ['t1', 't2', 't3\n'])])


class CallbackTests(unittest.TestCase):
    def setUp(self):
        bot = databot.Bot('sqlite:///:memory:')
        key = 'http://exemple.com'
        value = {'text': '<div>value</div>'}
        self.row, = bot.define('a').append(key, value).data.rows()

    def test_direct(self):
        select = html.Select((lambda row, html: row.key), 'div:text')
        self.assertEqual(select(self.row), [('http://exemple.com', 'value')])

    def test_row(self):
        select = html.Select(databot.row.key(), 'div:text')
        self.assertEqual(select(self.row), [('http://exemple.com', 'value')])


class MixedQueriesTests(unittest.TestCase):
    def setUp(self):
        bot = databot.Bot('sqlite:///:memory:')
        key = 'http://exemple.com'
        value = {'text': '\n'.join([
            '<div><p><span><b>value</b></span></p></div>',
        ])}
        self.row, = bot.define('a').append(key, value).data.rows()

    def test_mixed_query(self):
        select = html.Select('div xpath:p css: span b:text')
        self.assertEqual(select(self.row), [('value', None)])


class ComplexListTests(unittest.TestCase):
    def setUp(self):
        bot = databot.Bot('sqlite:///:memory:')
        key = 'http://exemple.com'
        value = {'text': '\n'.join([
            '<table>',
            '  <tr>',
            '    <td>1</td>',
            '    <td>2</td>',
            '  </tr>',
            '  <tr>',
            '    <td>1</td>',
            '    <td>2</td>',
            '  </tr>',
            '</table>',
        ])}
        self.row, = bot.define('a').append(key, value).data.rows()

    def test_mixed_query(self):
        select = html.Select(['table tr', {'a': 'td[1]:text', 'b': 'td[2]:text'}])
        self.assertEqual(select(self.row), [{'a': '1', 'b': '2'}, {'a': '1', 'b': '2'}])


class SingleElementTests(unittest.TestCase):
    def setUp(self):
        bot = databot.Bot('sqlite:///:memory:')
        key = 'http://exemple.com'
        value = {'text': '\n'.join([
            '<div><a name="1">a</a><a name="2">b</a></div>',
        ])}
        self.row, = bot.define('a').append(key, value).data.rows()

    def test_mixed_query(self):
        select = html.Select(['div > a', ('@name', ':text')])
        self.assertEqual(select(self.row), [('1', 'a'), ('2', 'b')])


class InlineCallTests(unittest.TestCase):
    def setUp(self):
        bot = databot.Bot('sqlite:///:memory:')
        key = 'http://exemple.com'
        value = {'text': '\n'.join([
            '<div><a name="1">a</a><a name="2">b</a></div>',
        ])}
        self.row, = bot.define('a').append(key, value).data.rows()

    def test_mixed_query(self):
        select = html.Select(['div > a', ('@name', call(lambda v: v + '!', ':text'))])
        self.assertEqual(select(self.row), [('1', 'a!'), ('2', 'b!')])


class AbsoluteCssSelectorTests(unittest.TestCase):
    def setUp(self):
        bot = databot.Bot('sqlite:///:memory:')
        key = 'http://exemple.com'
        value = {'text': '\n'.join([
            '<div><h2>heading</h2><a name="1">a</a><a name="2">b</a></div>',
        ])}
        self.row, = bot.define('a').append(key, value).data.rows()

    def test_mixed_query(self):
        select = html.Select(['div > a', ('@name', '/h2:text')])
        self.assertEqual(select(self.row), [('1', 'heading'), ('2', 'heading')])


class ValueTests(unittest.TestCase):
    def setUp(self):
        bot = databot.Bot('sqlite:///:memory:')
        key = 'http://exemple.com'
        value = {'text': '\n'.join([
            '<div><h2>heading</h2><a name="1">a</a><a name="2">b</a></div>',
        ])}
        self.row, = bot.define('a').append(key, value).data.rows()

    def test_mixed_query(self):
        select = html.Select(['div > a', ('@name', value('x'))])
        self.assertEqual(select(self.row), [('1', 'x'), ('2', 'x')])


class JoinTests(unittest.TestCase):
    def setUp(self):
        bot = databot.Bot('sqlite:///:memory:')
        key = 'http://exemple.com'
        value = {'text': '\n'.join([
            '<div>',
            '  <div id="nr1"><a name="1">a</a><a name="2">b</a></div>',
            '  <div id="nr2"><a name="3">c</a><a name="4">d</a></div>',
            '</div>',
        ])}
        self.row, = bot.define('a').append(key, value).data.rows()

    def test_mixed_query(self):
        select = html.Select(databot.join(
            ['div#nr1 > a', ('@name', ':text')],
            ['div#nr2 > a', ('@name', ':text')],
        ))
        self.assertEqual(select(self.row), [('1', 'a'), ('2', 'b'), ('3', 'c'), ('4', 'd')])
