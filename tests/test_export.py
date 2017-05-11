import io

import pytest
import databot
import pandas as pd

from databot.db.utils import Row
from databot.exporters.utils import flatten_nested_lists, flatten_nested_dicts, get_level_keys, flatten, sort_fields
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


def test_flatten_rows_update(data):
    rows = [
        Row(key=1, value={'text': 'abc'}),
        Row(key=1, value={'text': 'abcde'}),
    ]
    update = {'size': databot.this.value.text.apply(len)}
    assert list(flatten(rows, include=['key', 'size'], update=update)) == [
        ('key', 'size'),
        (1, 3),
        (1, 5),
    ]


def test_flattenjson():
    rows = [
        {'key': 1, 'value': {'foo': 'bar', 'events': [
            {'name': 'Event 1', 'date': '2017-01-01', 'people': ['a', 'b']},
            {'name': 'Event 2', 'date': '2017-01-02', 'people': ['a']},
        ]}},
        {'key': 2, 'value': {'foo': 'baz', 'events': [
            {'name': 'Event 3', 'date': '2017-01-03', 'people': ['x', 'y']},
            {'name': 'Event 4', 'date': '2017-01-04', 'people': ['z']},
        ]}},
    ]
    assert list(map(dict, flatten_nested_lists(rows, include={('key',), ('value', 'events', 'date')}))) == [
        {('key',): 1, ('value', 'events', 'date'): '2017-01-01'},
        {('key',): 1, ('value', 'events', 'date'): '2017-01-02'},
        {('key',): 2, ('value', 'events', 'date'): '2017-01-03'},
        {('key',): 2, ('value', 'events', 'date'): '2017-01-04'},
    ]

    assert list(map(dict, flatten_nested_lists(rows, include={('key',), ('value', 'events', 'people')}))) == [
        {('key',): 1, ('value', 'events', 'people'): 'a'},
        {('key',): 1, ('value', 'events', 'people'): 'b'},
        {('key',): 1, ('value', 'events', 'people'): 'a'},
        {('key',): 2, ('value', 'events', 'people'): 'x'},
        {('key',): 2, ('value', 'events', 'people'): 'y'},
        {('key',): 2, ('value', 'events', 'people'): 'z'},
    ]

    assert [{v for k, v in x} for x in flatten_nested_lists(rows, include=[('key',), ('value',)])] == [
        {1, 'bar', '2017-01-01', 'Event 1', 'a'},
        {1, 'bar', '2017-01-01', 'Event 1', 'b'},
        {1, 'bar', '2017-01-02', 'Event 2', 'a'},
        {2, 'baz', '2017-01-03', 'Event 3', 'x'},
        {2, 'baz', '2017-01-03', 'Event 3', 'y'},
        {2, 'baz', '2017-01-04', 'Event 4', 'z'},
    ]

    assert [{v for k, v in x} for x in flatten_nested_lists(rows)] == [
        {1, 'bar', '2017-01-01', 'Event 1', 'a'},
        {1, 'bar', '2017-01-01', 'Event 1', 'b'},
        {1, 'bar', '2017-01-02', 'Event 2', 'a'},
        {2, 'baz', '2017-01-03', 'Event 3', 'x'},
        {2, 'baz', '2017-01-03', 'Event 3', 'y'},
        {2, 'baz', '2017-01-04', 'Event 4', 'z'},
    ]


def test_flatten_nested_dicts():
    assert set(flatten_nested_dicts({'a': 1, 'b': 2, 'c': 3})) == {
        (('a',), 1),
        (('b',), 2),
        (('c',), 3),
    }


def test_flatten_nested_dicts_include():
    assert set(flatten_nested_dicts({'a': 1, 'b': 2, 'c': 3}, include=[('b',), ('a',), ('c',)])) == {
        (('b',), 2),
        (('a',), 1),
        (('c',), 3),
    }


def test_get_level_keys():
    assert list(get_level_keys(keys=['c', 'b', 'a'], field=(), include=())) == ['a', 'b', 'c']
    assert list(get_level_keys(keys=['c', 'b', 'a'], field=(), include=[('b',), ('a',), ('c',)])) == ['b', 'a', 'c']
    assert list(get_level_keys(keys=['c', 'b', 'a'], field=('x',), include=())) == ['a', 'b', 'c']
    assert list(get_level_keys(keys=['c', 'b', 'a'], field=('x',), include=[('x', 'b',), ('x', 'c',)])) == ['b', 'c']
    assert list(get_level_keys(keys=['c', 'b', 'a'], field=(), include=[('b',), ('x',)])) == ['b']
    assert list(get_level_keys(keys=['c', 'b', 'a'], field=('x', 'y'), include=[('x',)])) == ['a', 'b', 'c']


def test_flatten():
    rows = [
        Row(key=1, value={'foo': 'bar', 'events': [
            {'name': 'Event 1', 'date': '2017-01-01', 'people': ['a', 'b']},
            {'name': 'Event 2', 'date': '2017-01-02', 'people': ['a']},
        ]}),
        Row(key=2, value={'foo': 'baz', 'events': [
            {'name': 'Event 3', 'date': '2017-01-03', 'people': ['x', 'y']},
            {'name': 'Event 4', 'date': '2017-01-04', 'people': ['z']},
        ]}),
    ]
    assert list(flatten(rows)) == [
        ('events.date', 'events.name', 'events.people', 'foo', 'key'),
        ('2017-01-01', 'Event 1', 'a', 'bar', 1),
        ('2017-01-01', 'Event 1', 'b', 'bar', 1),
        ('2017-01-02', 'Event 2', 'a', 'bar', 1),
        ('2017-01-03', 'Event 3', 'x', 'baz', 2),
        ('2017-01-03', 'Event 3', 'y', 'baz', 2),
        ('2017-01-04', 'Event 4', 'z', 'baz', 2),
    ]

    assert list(flatten(rows, include=('key', 'foo', 'events.people'))) == [
        ('key', 'foo', 'events.people'),
        (1, 'bar', 'a'),
        (1, 'bar', 'b'),
        (1, 'bar', 'a'),
        (2, 'baz', 'x'),
        (2, 'baz', 'y'),
        (2, 'baz', 'z'),
    ]

    assert list(flatten(rows, include=('key', 'foo'))) == [
        ('key', 'foo'),
        (1, 'bar'),
        (2, 'baz'),
    ]


def test_sort_fields():
    def _(fields, include):
        fields = [tuple(x.split('.')) for x in fields]
        include = [tuple(x.split('.')) for x in include]
        return ['.'.join(x) for x in sort_fields(fields, include)]

    assert _(['c', 'b', 'a'], []) == ['a', 'b', 'c']
    assert _(['c', 'b', 'a'], ['a', 'c']) == ['a', 'c']
    assert _(['x.c', 'x.b', 'x.a'], ['x']) == ['x.a', 'x.b', 'x.c']
    assert _(['z', 'x.b', 'x.a'], ['x', 'z']) == ['x.a', 'x.b', 'z']


def test_flatten_rows_update_without_include(data):
    rows = [
        Row(key=1, value={'text': 'abc'}),
        Row(key=1, value={'text': 'abcde'}),
    ]
    update = {'size': databot.this.value.text.apply(len)}
    assert list(flatten(rows, update=update)) == [
        ('key', 'size', 'text'),
        (1, 3, 'abc'),
        (1, 5, 'abcde'),
    ]


def test_flatten_rows_callable_update(data):
    rows = [
        Row(key=1, value={'text': 'abc'}),
        Row(key=1, value={'text': 'abcde'}),
    ]

    def update(row):
        return {'size': len(row.value['text'])}

    assert list(flatten(rows, update=update)) == [
        ('size',),
        (3,),
        (5,),
    ]


def test_flatten_rows_include(data):
    rows = [
        Row(key=1, value={'a': 1}),
        Row(key=2, value={'b': 2}),
    ]
    assert list(flatten(rows, include=['a', 'b'])) == [
        ('a', 'b'),
        (1, None),
        (None, 2),
    ]


def test_flatten_rows_include_value(data):
    rows = [
        Row(key=1, value='a'),
        Row(key=2, value='b'),
    ]
    assert list(flatten(rows, include=['key', 'value'])) == [
        ('key', 'value'),
        (1, 'a'),
        (2, 'b'),
    ]


def test_flatten_rows_value(data):
    rows = [
        Row(key=1, value='a'),
        Row(key=2, value='b'),
    ]
    assert list(flatten(rows)) == [
        ('key', 'value'),
        (1, 'a'),
        (2, 'b'),
    ]


def test_flatten_int_key(data):
    rows = [
        Row(key=1, value={'year': {2000: 1, 2001: 2}}),
        Row(key=2, value={'year': {2000: 3, 2001: 4}}),
    ]
    assert list(flatten(rows)) == [
        ('key', 'year.2000', 'year.2001'),
        (1, 1, 2),
        (2, 3, 4),
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
    assert list(flatten(rows)) == [
        ('events.date', 'events.name', 'key'),
        ('2017-01-01', 'Event 1', 1),
        ('2017-02-01', 'Event 2', 1),
        ('2017-03-01', 'Event 3', 2),
        ('2017-04-01', 'Event 4', 2),
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
