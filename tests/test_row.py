from databot import row
from databot.db.utils import Row


def test_row():
    assert row(Row(key=1, value=2)) == {'key': 1, 'value': 2}


def test_key():
    assert row.key(Row(key=1, value=2)) == 1


def test_value():
    assert row.value(Row(key=1, value=2)) == 2


def test_value_item():
    assert row.value['x'](Row(key=1, value={'x': 3, 'y': 4})) == 3


def test_value_multiple_items():
    assert row.value['x']['y']['z'](Row(key=1, value={'x': {'y': {'z': 42}}})) == 42


def test_value_length():
    assert row.value['x'].length(Row(key=1, value={'x': 'abc'})) == 3
