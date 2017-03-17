from databot import this
from databot.db.utils import Row
from databot import recursive


def test_nested_dict():
    source = {'value': {'number': this.value}}
    target = {'value': {'number': 42}}
    assert recursive.call(source, Row(value=42)) == target


def test_list():
    source = {'value': [this.value, this.value]}
    target = {'value': [42, 42]}
    assert recursive.call(source, Row(value=42)) == target


def test_update():
    source = {'a': {'b': {'c': 24, 'd': 3}}}
    target = {'a': {'b': {'c': 42, 'd': 3}}, 'x': 2}
    assert recursive.update(source, {'a.b.c': 42, 'x': 2}) == target
    assert recursive.update({'a': 1, 'b': 2}, {'a': 2}) == {'a': 2, 'b': 2}


def test_update_scalar():
    assert recursive.update('a', 'b') == 'b'
