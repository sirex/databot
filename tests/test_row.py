from databot import this
from databot.db.utils import Row


def test_row():
    row = Row(key=1, value=2)
    assert this._eval(row) == {'key': 1, 'value': 2}


def test_key():
    row = Row(key=1, value=2)
    assert this.key._eval(row) == 1


def test_value():
    row = Row(key=1, value=2)
    assert this.value._eval(row) == 2


def test_value_item():
    row = Row(key=1, value={'x': 3, 'y': 4})
    assert this.value['x']._eval(row) == 3


def test_value_multiple_items():
    row = Row(key=1, value={'x': {'y': {'z': 42}}})
    assert this.value['x']['y']['z']._eval(row) == 42


def test_value_function():
    row = Row(key=1, value={'x': 'abc'})
    assert this.value.x.apply(len)._eval(row) == 3
    assert this.value.x.upper()._eval(row) == 'ABC'
    assert this.value.apply(list)._eval(row) == ['x']
    assert this.value.apply(list)[0].upper()._eval(row) == 'X'


def test_value_function_arguments():
    def getitem(value, key, default):
        return value.get(key, default)

    row = Row(key=1, value={'x': 'abc'})
    assert this.value.apply(getitem, 'y', 'zz')._eval(row) == 'zz'


def test_value_attr():
    row = Row(key=1, value={'x': 3, 'y': 4})
    assert this.value.x._eval(row) == 3

    row = Row(key=1, value={'x': {'y': {'z': 42}}})
    assert this.value.x.y.z._eval(row) == 42


def test_url():
    row = Row(key=1, value='http://example.com/path?key=42')
    assert this.value.urlparse().query.key.cast(int)._eval(row) == 42
    assert this.value.urlparse().path._eval(row) == '/path'
    assert this.value.urlparse().hostname._eval(row) == 'example.com'
    assert this.value.url()._eval(row) == 'http://example.com/path?key=42'
