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
