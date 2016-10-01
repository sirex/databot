import pytest
import databot

from databot.db.utils import Row
from databot.exporters.csv import get_fields, get_values, flatten_rows


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


def test_update(data):
    rows = [
        Row(key=1, value={'text': 'abc'}),
        Row(key=1, value={'text': 'abcde'}),
    ]
    update = {'size': databot.row.value.text(len)}
    assert list(flatten_rows(rows, include=['key', 'size'], update=update)) == [
        ['key', 'size'],
        [1, 3],
        [1, 5],
    ]


def test_update_without_include(data):
    rows = [
        Row(key=1, value={'text': 'abc'}),
        Row(key=1, value={'text': 'abcde'}),
    ]
    update = {'size': databot.row.value.text(len)}
    assert list(flatten_rows(rows, update=update)) == [
        ['key', 'size', 'text'],
        [1, 3, 'abc'],
        [1, 5, 'abcde'],
    ]


def test_callable_update(data):
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


def test_include(data):
    rows = [
        Row(key=1, value={'a': 1}),
        Row(key=2, value={'b': 2}),
    ]
    assert list(flatten_rows(rows, include=['a', 'b'])) == [
        ['a', 'b'],
        [1, None],
        [None, 2],
    ]


def test_include_value(data):
    rows = [
        Row(key=1, value='a'),
        Row(key=2, value='b'),
    ]
    assert list(flatten_rows(rows, include=['key', 'value'])) == [
        ['key', 'value'],
        [1, 'a'],
        [2, 'b'],
    ]


def test_value(data):
    rows = [
        Row(key=1, value='a'),
        Row(key=2, value='b'),
    ]
    assert list(flatten_rows(rows)) == [
        ['key', 'value'],
        [1, 'a'],
        [2, 'b'],
    ]
