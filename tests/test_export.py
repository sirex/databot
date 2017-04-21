import io

import pytest
import databot
import pandas as pd

from databot.db.utils import Row
from databot.exporters.utils import get_fields, get_values, flatten_rows
from databot.exporters import jsonl
from databot.exporters import pandas


@pytest.fixture
def data():
    return {
        'a': 1,
        'b': 2,
        'c': {
            'x': 1,
            'y': 2,
            'z': ['foo', 'bar', 'baz'],
        }
    }


def test_get_fields(data):
    assert get_fields(data) == [
        ('a',),
        ('b',),
        ('c', 'x'),
        ('c', 'y'),
        ('c', 'z'),
    ]


def test_get_values(data):
    fields = [
        ('a',),
        ('b',),
        ('c', 'x'),
        ('c', 'z'),
    ]
    assert get_values(fields, data) == (1, 2, 1, ['foo', 'bar', 'baz'])


def test_missing_value(data):
    fields = [
        ('a',),
        ('z',),
    ]
    assert get_values(fields, data) == (1, None)


def group_fields(fields):
    pass


def test_group_fields():
    pass


def test_nested_value(data):
    data = {'a': 1, 'b': [
        {'c': 1, 'd': 1},
        {'c': 2, 'd': 2},
    ]}
    fields = [
        ('a',),
        ('b[]', 'c'),
        ('b[]', 'd'),
    ]
    fields = [
        ('a',),
        ('b', ['c', 'd']),
    ]
    assert get_values(fields, data) == (1, None)


def test_flatten_rows_update(data):
    rows = [
        Row(key=1, value={'text': 'abc'}),
        Row(key=1, value={'text': 'abcde'}),
    ]
    update = {'size': databot.this.value.text.apply(len)}
    assert list(flatten_rows(rows, include=['key', 'size'], update=update)) == [
        ['key', 'size'],
        [1, 3],
        [1, 5],
    ]


def flatten_nested(nested, field=()):
    if isinstance(nested, dict):
        for k, v in nested.items():
            yield from flatten_nested(v, field + (k,))
    elif isinstance(nested, list):
        if field:
            field = field[:-1] + (field[-1] + '[]',)
        else:
            field = ('[]',)
        for v in nested:
            yield from flatten_nested(v, field)
    else:
        yield (field, nested)


def split_flattened(flat):
    for key, value in flat:
        for i in range(1, len(key) - 1):
            # yield key[:i], key[i:], value
            yield '.'.join(key[:i]), '.'.join(key[i:]), value


def group_flattened(flat):
    from itertools import groupby
    from operator import itemgetter

    for key, group in groupby(split_flattened(flat), key=itemgetter(0)):
        yield key, dict(x[1:] for x in group)


def test_flattenjson():
    rows = [
        {'key': 1, 'value': {'events': [
            {'name': 'Event 1', 'date': '2017-01-01'},
            {'name': 'Event 2', 'date': '2017-02-01'},
        ]}},
        {'key': 2, 'value': {'events': [
            {'name': 'Event 3', 'date': '2017-03-01'},
            {'name': 'Event 4', 'date': '2017-04-01'},
        ]}},
    ]
    # assert list(flatten_nested(rows)) == []
    assert list(group_flattened(flatten_nested(rows))) == []


def test_flatten_rows_update_without_include(data):
    rows = [
        Row(key=1, value={'text': 'abc'}),
        Row(key=1, value={'text': 'abcde'}),
    ]
    update = {'size': databot.this.value.text.apply(len)}
    assert list(flatten_rows(rows, update=update)) == [
        ['key', 'size', 'text'],
        [1, 3, 'abc'],
        [1, 5, 'abcde'],
    ]


def test_flatten_rows_callable_update(data):
    rows = [
        Row(key=1, value={'text': 'abc'}),
        Row(key=1, value={'text': 'abcde'}),
    ]

    def update(row):
        return {'size': len(row.value['text'])}

    assert list(flatten_rows(rows, update=update)) == [
        ['size'],
        [3],
        [5],
    ]


def test_flatten_rows_include(data):
    rows = [
        Row(key=1, value={'a': 1}),
        Row(key=2, value={'b': 2}),
    ]
    assert list(flatten_rows(rows, include=['a', 'b'])) == [
        ['a', 'b'],
        [1, None],
        [None, 2],
    ]


def test_flatten_rows_include_value(data):
    rows = [
        Row(key=1, value='a'),
        Row(key=2, value='b'),
    ]
    assert list(flatten_rows(rows, include=['key', 'value'])) == [
        ['key', 'value'],
        [1, 'a'],
        [2, 'b'],
    ]


def test_flatten_rows_value(data):
    rows = [
        Row(key=1, value='a'),
        Row(key=2, value='b'),
    ]
    assert list(flatten_rows(rows)) == [
        ['key', 'value'],
        [1, 'a'],
        [2, 'b'],
    ]


def test_flatten_int_key(data):
    rows = [
        Row(key=1, value={'year': {2000: 1, 2001: 2}}),
        Row(key=2, value={'year': {2000: 3, 2001: 4}}),
    ]
    assert list(flatten_rows(rows)) == [
        ['key', 'year.2000', 'year.2001'],
        [1, 1, 2],
        [2, 3, 4],
    ]


def test_flatten_list(data):
    rows = [
        Row(key=1, value={'events': [
            {'name': 'Event 1', 'date': '2017-01-01'},
            {'name': 'Event 2', 'date': '2017-02-01'},
        ]}),
        Row(key=2, value={'events': [
            {'name': 'Event 3', 'date': '2017-03-01'},
            {'name': 'Event 4', 'date': '2017-04-01'},
        ]}),
    ]
    assert list(flatten_rows(rows)) == [
        ['key', 'events[].date', 'events[].name'],
    ]


def test_jsonl(bot):
    pipe = bot.define('p1').append([('1', 'a'), ('2', 'b')])
    stream = io.StringIO()
    jsonl.export(stream, pipe.rows())
    assert stream.getvalue().splitlines() == [
        '{"key": "1", "value": "a"}',
        '{"key": "2", "value": "b"}',
    ]


def test_jsonl_dict(bot):
    pipe = bot.define('p1').append([('1', {'a': 2}), ('2', {'b': 3})])
    stream = io.StringIO()
    jsonl.export(stream, pipe.rows())
    assert stream.getvalue().splitlines() == [
        '{"key": "1", "a": 2}',
        '{"key": "2", "b": 3}',
    ]


def test_pandas_rows_to_dataframe_items():
    rows = [
        [1, 'a', 'x'],
        [2, 'b', 'y'],
    ]
    assert list(pandas.rows_to_dataframe_items(rows, 0)) == [
        (1, ['a', 'x']),
        (2, ['b', 'y'])
    ]
    assert list(pandas.rows_to_dataframe_items(rows, 2)) == [
        ('x', [1, 'a']),
        ('y', [2, 'b'])
    ]


def test_pandas(bot):
    pipe = bot.define('p1').append([
        (1, {'a': 10}),
        (2, {'a': 20}),
    ])
    assert [dict(x._asdict()) for x in pipe.export(pd).itertuples()] == [
        {'Index': 1, 'a': 10.0},
        {'Index': 2, 'a': 20.0},
    ]
