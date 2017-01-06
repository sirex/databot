import databot
from databot.db.utils import Row


def test_row():
    row = Row(key=1, value=2)
    assert databot.row(row) == {'key': 1, 'value': 2}


def test_key():
    row = Row(key=1, value=2)
    assert databot.row.key(row) == 1


def test_value():
    row = Row(key=1, value=2)
    assert databot.row.value(row) == 2


def test_value_item():
    row = Row(key=1, value={'x': 3, 'y': 4})
    assert databot.row.value['x'](row) == 3


def test_value_multiple_items():
    row = Row(key=1, value={'x': {'y': {'z': 42}}})
    assert databot.row.value['x']['y']['z'](row) == 42


def test_value_function():
    row = Row(key=1, value={'x': 'abc'})
    assert databot.row.value.x(len)(row) == 3
    assert databot.row.value.x(str.upper)(row) == 'ABC'
    assert databot.row.value(list)(row) == ['x']
    assert databot.row.value(list)[0](str.upper)(row) == 'X'


def test_value_function_arguments():
    def getitem(value, key, default):
        return value.get(key, default)

    row = Row(key=1, value={'x': 'abc'})
    assert databot.row.value(getitem, 'y', 'zz')(row) == 'zz'


def test_value_attr():
    row = Row(key=1, value={'x': 3, 'y': 4})
    assert databot.row.value.x(row) == 3

    row = Row(key=1, value={'x': {'y': {'z': 42}}})
    assert databot.row.value.x.y.z(row) == 42


def test_url():
    row = Row(key=1, value='http://example.com/path?key=42')
    assert databot.url(databot.row.value).query.key(int)(row) == 42
    assert databot.url(databot.row.value).path(row) == '/path'
    assert databot.url(databot.row.value).hostname(row) == 'example.com'
    assert databot.url(databot.row.value)(row) == 'http://example.com/path?key=42'
