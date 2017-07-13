from textwrap import dedent

import pytest
import databot
import databot.pipes

from databot import select, value, this, oneof

from databot.handlers import html
from databot.db.utils import Row


@pytest.fixture
def Html(bot):
    def factory(lines):
        key = 'http://exemple.com'
        value = {'content': '\n'.join(lines).encode('utf-8')}
        row, = bot.define('a').append(key, value).rows()
        return row
    return factory


def test_css_key_value(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    qry = html.Select('#a@id', 'p#a:text')
    assert qry(row) == [('a', 'value-a')]


def test_css_only_keys(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    qry = html.Select(['p@id'])
    assert qry(row) == ['a', 'b', 'c']


def test_css_dict(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    qry = html.Select('#a@id', {'b': 'p#b:text', 'c': 'p#c:text'})
    assert qry(row) == [('a', {'b': 'value-b', 'c': 'value-c'})]


def test_css_list(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    qry = html.Select(None, ['p:text'])
    assert qry(row) == [(None, ['value-a', 'value-b', 'value-c'])]


def test_css_not_fould_value_error(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    qry = html.Select(None, '#missing')
    with pytest.raises(html.SelectorError):
        qry(row)


def test_css_multiple_values_error(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    qry = html.Select(None, 'p:text')
    with pytest.raises(html.SelectorError):
        qry(row)


def test_css_dict_and_list(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    qry = html.Select(None, {'a': ['p:text']})
    assert qry(row) == [(None, {'a': ['value-a', 'value-b', 'value-c']})]


def test_css_nth_child(Html):
    row = Html(['<p id="a">value-a</p><p id="b">value-b</p><p id="c">value-c</p>'])
    # `p[1]` should be replaced to `p:nth-child(1)`
    qry = html.Select(None, {'a': 'p[1]:text', 'c': 'p[3]:text'})
    assert qry(row) == [(None, {'a': 'value-a', 'c': 'value-c'})]


def test_css_tail_select(Html):
    row = Html([
        '<div>',
        '  <a href="#">link</a>t1<br>t2<br>t3',
        '</div>',
    ])
    qry = html.Select(None, ['div *:tail'])
    assert qry(row) == [(None, ['t1', 't2', 't3\n'])]


def test_callback_direct(Html):
    row = Html(['<div>value</div>'])
    qry = html.Select((lambda row, html: row.key), 'div:text')
    assert qry(row) == [('http://exemple.com', 'value')]


def test_callback_row(Html):
    row = Html(['<div>value</div>'])
    qry = html.Select(this.key, 'div:text')
    assert qry(row) == [('http://exemple.com', 'value')]


def test_mixed_query(Html):
    row = Html(['<div><p><span><b>value</b></span></p></div>'])
    qry = html.Select('div xpath:p css: span b:text')
    assert qry(row) == [('value', None)]


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
    qry = html.Select(['table tr', {'a': 'td[1]:text', 'b': 'td[2]:text'}])
    assert qry(row) == [{'a': '1', 'b': '2'}, {'a': '1', 'b': '2'}]


def test_single_element(Html):
    row = Html(['<div><a name="1">a</a><a name="2">b</a></div>'])
    qry = html.Select(['div > a', ('@name', ':text')])
    assert qry(row) == [('1', 'a'), ('2', 'b')]


def test_inline_call(Html):
    row = Html(['<div><a name="1">a</a><a name="2">b</a></div>'])
    qry = html.Select(['div > a', ('@name', select(':text').upper())])
    assert qry(row) == [('1', 'A'), ('2', 'B')]


def test_call_getitem(Html):
    row = Html(['<div><a name="1">a</a><a name="2">b</a></div>'])
    qry = html.Select(select(['div > a'])[0].text().upper())
    assert qry(row) == 'A'


def test_absolute_css_selector(Html):
    row = Html(['<div><h2>heading</h2><a name="1">a</a><a name="2">b</a></div>'])
    qry = html.Select(['div > a', ('@name', '/h2:text')])
    assert qry(row) == [('1', 'heading'), ('2', 'heading')]


def test_value(Html):
    row = Html(['<div><h2>heading</h2><a name="1">a</a><a name="2">b</a></div>'])
    qry = html.Select(['div > a', ('@name', value('x'))])
    assert qry(row) == [('1', 'x'), ('2', 'x')]


def test_join(Html):
    row = Html([
        '<div>',
        '  <div id="nr1"><a name="1">a</a><a name="2">b</a></div>',
        '  <div id="nr2"><a name="3">c</a><a name="4">d</a></div>',
        '</div>',
    ])
    qry = html.Select(databot.join(
        ['div#nr1 > a', ('@name', ':text')],
        ['div#nr2 > a', ('@name', ':text')],
    ))
    assert qry(row) == [('1', 'a'), ('2', 'b'), ('3', 'c'), ('4', 'd')]


def test_first_missing(Html):
    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    qry = html.Select(databot.first('#missing?', 'a[name="1"]:text'))
    assert qry(row) == 'a'


def test_first_value(Html):
    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    qry = html.Select(databot.first('#missing?', value('z')))
    assert qry(row) == 'z'


def test_first_empty_string(Html):
    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    qry = html.Select(databot.first('a[name="2"]:text', 'a[name="1"]:text'))
    assert qry(row) == 'a'


def test_first_from_list(Html):
    row = Html(['<div><a name="1">a</a><a name="2">b</a></div>'])

    qry = html.Select(databot.first('a:text'))
    with pytest.raises(html.SelectorError):
        qry(row)

    qry = html.Select(databot.first(['a:text']))
    assert qry(row) == 'a'


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
    selector = html.Select(['a', select('@href').url(query='id')])
    assert selector(row) == ['http://example.com/?id=1', None]


def test_tuple(Html):
    row = Html(['<div class="a">foobar</div>'])
    selector = html.Select(this.key, ':content')
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


def test_select_inside_list(Html):
    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    selector = html.Select([select('a@name').cast(int)])
    assert selector(row) == [1, 2]


def test_select_outside_list(Html):
    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    selector = html.Select(select(['a@name']).apply(len))
    assert selector(row) == 2


def test_select_outside_nested_list(Html):
    row = Html(['<div><a name="1">a</a><a name="2"></a></div>'])
    selector = html.Select(['a@name', select().cast(int)])
    assert selector(row) == [1, 2]


def test_text(Html):
    row = Html(['<div><p>p1</p>p2<br>p3<br>p4</div>'])
    selector = html.Select(select('div').text())
    assert selector(row).splitlines() == [
        'p1', '',
        'p2', '',
        'p3', '',
        'p4'
    ]


def test_text_comments(Html):
    row = Html(['<div><p>text <!-- comment --><o:p></o:p></p></div>'])
    selector = html.Select(select('div').text())
    assert selector(row) == 'text'


def test_text_processing_instructions(Html):
    row = Html(['<div><p>text <?xml:namespace prefix="o" /></p></div>'])
    selector = html.Select(select('div').text())
    assert selector(row) == 'text'


def test_text_text(Html):
    row = Html(['<div><p>1</p>2<p>3</p>4</div>'])
    selector = html.Select(select(['xpath://p[1]/following-sibling::node()']).text())
    assert selector(row) == '2 3\n\n4'


def test_empty_result(Html):
    row = Html([
        '<div>',
        '  <h1>Test</h1>',
        '  <p>1</p>',
        '  <p>2</p>',
        '  <p>3</p>',
        '  <h2></h2>',
        '</div>',
    ])

    # Raise error, if selector did not found anything
    selector = html.Select(['p.new:text'])
    with pytest.raises(html.SelectorError) as e:
        selector(row)
    assert str(e.value) == (
        "Select query did not returned any results. Row key: 'http://exemple.com'. Query: ['p.new:text']"
    )

    # Allow empty result from selector.
    selector = html.Select(['p.new:text'], check=False)
    assert selector(row) == []

    # Allow empty result from selector, but check if we still looking at the right page.
    selector = html.Select(['p.new:text'], check='xpath://h1[text() = "Test"]')
    assert selector(row) == []

    # Existing element without content should not be threated as emtpy.
    selector = html.Select(select('xpath://h2').text())
    assert selector(row) == ''


def test_float(Html):
    row = Html(['<div><p>p1</p>p2<br>p3<br>p4</div>'])
    selector = html.Select('xpath:count(//p)')
    assert selector(row) == [
        (1.0, None)
    ]


def test_null(Html, freezetime):
    freezetime('2017-05-18T14:43:40.876642')

    row = Html(['<div><p id="this"> p1 </p>/div>'])

    selector = html.Select(select('#wrong:text?').strip())
    with pytest.raises(html.SelectorError) as e:
        selector(row)
    assert str(e.value) == dedent('''\
        Expression error while evaluating None. Error: error while processing expression:
          this.
          strip()
        evaluated with:
          Row({
              'compression': None,
              'created': datetime.datetime(2017, 5, 18, 14, 43, 40, 876642),
              'id': 1,
              'key': 'http://exemple.com',
              'value': {'content': b'<div><p id="this"> p1 </p>/div>'},
          }). Context:

        <html>
          <body>
            <div><p id="this"> p1 </p>/div&gt;</div>
          </body>
        </html>
    ''')

    selector = html.Select(select('#wrong:text?').null().strip())
    assert selector(row) is None

    selector = html.Select(select('#this:text?').null().strip())
    assert selector(row) == 'p1'


def test_check_expr(bot):
    key = 'http://exemple.com'
    value = {'content': b'<div></div>', 'encoding': 'utf-8', 'headers': {'Content-Type': 'text/html'}}
    row = bot.define('a').append(key, value).last()

    selector = html.Select(['h1'], check=this.value.headers['Content-Type'].header().subtype == 'html')
    assert selector(row) == []

    selector = html.Select(['h1'])
    with pytest.raises(html.SelectorError) as e:
        selector(row)
    assert str(e.value) == "Select query did not returned any results. Row key: 'http://exemple.com'. Query: ['h1']"


def test_apply_with_select_arg(Html):
    def f(value, link):
        return value, link

    row = Html(['<div><p>1</p></div>'])
    selector = html.Select(select('p:text').cast(int).apply(f, this.key))
    assert selector(row) == (1, 'http://exemple.com')


def test_oneof(Html):
    row = Html([
        '<div>'
        '  <p><a>1</a></p>'
        '  <p><b>2</b></p>'
        '  <p><i>3</i></p>'
        '</div>'
    ])
    selector = html.Select([
        'div p', oneof(
            select('a:text'),
            select('b:text'),
            select('i:text'),
        )
    ])
    assert selector(row) == ['1', '2', '3']


def test_select_method(bot):
    row = Row({
        'key': 1,
        'value': {
            'xml': (
                '<div>'
                '  <p>1</p>'
                '  <p>2</p>'
                '  <p>3</p>'
                '</div>'
            ),
        },
    })

    selector = html.Select(this.value.xml.select([select('div p:text').cast(int)]))
    assert selector(row) == [1, 2, 3]
