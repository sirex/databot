import pytest
import databot
import databot.pipes

from databot import call, value
from databot.handlers import html


@pytest.fixture
def Html(bot):
    def factory(lines):
        key = 'http://exemple.com'
        value = {'content': '\n'.join(lines).encode('utf-8')}
        row, = bot.define('a').append(key, value).data.rows()
        return row
    return factory


def test_css_key_value(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    select = html.Select('#a@id', 'p#a:text')
    assert select(row) == [('a', 'value-a')]


def test_css_only_keys(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    select = html.Select(['p@id'])
    assert select(row) == ['a', 'b', 'c']


def test_css_dict(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    select = html.Select('#a@id', {'b': 'p#b:text', 'c': 'p#c:text'})
    assert select(row) == [('a', {'b': 'value-b', 'c': 'value-c'})]


def test_css_list(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    select = html.Select(None, ['p:text'])
    assert select(row) == [(None, ['value-a', 'value-b', 'value-c'])]


def test_css_not_fould_value_error(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    select = html.Select(None, '#missing')
    with pytest.raises(ValueError):
        select(row)


def test_css_multiple_values_error(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    select = html.Select(None, 'p:text')
    with pytest.raises(ValueError):
        select(row)


def test_css_dict_and_list(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    select = html.Select(None, {'a': ['p:text']})
    assert select(row) == [(None, {'a': ['value-a', 'value-b', 'value-c']})]


def test_css_nth_child(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    # `p[1]` should be replaced to `p:nth-child(1)`
    select = html.Select(None, {'a': 'p[1]:text', 'c': 'p[3]:text'})
    assert select(row) == [(None, {'a': 'value-a', 'c': 'value-c'})]


def test_css_tail_select(Html):
    row = Html([
        '<div>',
        '  <a href="#">link</a>t1<br>t2<br>t3',
        '</div>',
    ])
    select = html.Select(None, ['div *:tail'])
    assert select(row) == [(None, ['t1', 't2', 't3\n'])]


def test_callback_direct(Html):
    row = Html(['<div>value</div>'])
    select = html.Select((lambda row, html: row.key), 'div:text')
    assert select(row) == [('http://exemple.com', 'value')]


def test_callback_row(Html):
    row = Html(['<div>value</div>'])
    select = html.Select(databot.row.key, 'div:text')
    assert select(row) == [('http://exemple.com', 'value')]


def test_mixed_query(Html):
    row = Html(['<div><p><span><b>value</b></span></p></div>'])
    select = html.Select('div xpath:p css: span b:text')
    assert select(row) == [('value', None)]


def test_complex_list(Html):
    row = Html([
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
    ])
    select = html.Select(['table tr', {'a': 'td[1]:text', 'b': 'td[2]:text'}])
    assert select(row) == [{'a': '1', 'b': '2'}, {'a': '1', 'b': '2'}]


def test_single_element(Html):
    row = Html(['<div><a name="1">a</a><a name="2">b</a></div>'])
    select = html.Select(['div > a', ('@name', ':text')])
    assert select(row) == [('1', 'a'), ('2', 'b')]


def test_inline_call(Html):
    row = Html(['<div><a name="1">a</a><a name="2">b</a></div>'])
    select = html.Select(['div > a', ('@name', call(lambda v: v + '!', ':text'))])
    assert select(row) == [('1', 'a!'), ('2', 'b!')]


def test_absolute_css_selector(Html):
    row = Html(['<div><h2>heading</h2><a name="1">a</a><a name="2">b</a></div>'])
    select = html.Select(['div > a', ('@name', '/h2:text')])
    assert select(row) == [('1', 'heading'), ('2', 'heading')]


def test_value(Html):
    row = Html(['<div><h2>heading</h2><a name="1">a</a><a name="2">b</a></div>'])
    select = html.Select(['div > a', ('@name', value('x'))])
    assert select(row) == [('1', 'x'), ('2', 'x')]


def test_join(Html):
    row = Html([
        '<div>',
        '  <div id="nr1"><a name="1">a</a><a name="2">b</a></div>',
        '  <div id="nr2"><a name="3">c</a><a name="4">d</a></div>',
        '</div>',
    ])
    select = html.Select(databot.join(
        ['div#nr1 > a', ('@name', ':text')],
        ['div#nr2 > a', ('@name', ':text')],
    ))
    assert select(row) == [('1', 'a'), ('2', 'b'), ('3', 'c'), ('4', 'd')]


def test_first_missing(Html):
    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    select = html.Select(databot.first('#missing?', 'a[name="1"]:text'))
    assert select(row) == 'a'


def test_first_value(Html):
    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    select = html.Select(databot.first('#missing?', value('z')))
    assert select(row) == 'z'


def test_first_empty_string(Html):
    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    select = html.Select(databot.first('a[name="2"]:text', 'a[name="1"]:text'))
    assert select(row) == 'a'


def test_func(Html):
    length = databot.func()(len)
    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    assert html.Select(length(['a']))(row) == 2


def test_url(Html):
    row = Html([
        '<div>',
        '<a href="http://example.com/?id=1">a</a>',
        '<a href="http://example.com/">b</a>',
        '</div>',
    ])
    selector = html.Select([
        'a', databot.url('@href', query='id'),
    ])
    assert selector(row) == ['http://example.com/?id=1', None]


def test_tuple(Html):
    row = Html(['<div class="a">foobar</div>'])
    selector = html.Select(databot.row.key, ':content')
    assert selector(row) == [('http://exemple.com', 'foobar')]


def test_func_on_list_prefix(Html):
    @databot.func()
    def skip(nodes):
        return nodes[1:]

    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    selector = html.Select([skip('a'), '@name'])
    assert selector(row) == ['2']


def test_func_inside_list(Html):
    @databot.func()
    def number(value):
        return int(value)

    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    selector = html.Select([number('a@name')])
    assert selector(row) == [1, 2]


def test_func_outside_list(Html):
    @databot.func()
    def number(values):
        return list(map(int, values))

    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    selector = html.Select(number(['a@name']))
    assert selector(row) == [1, 2]
