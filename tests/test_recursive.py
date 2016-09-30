from databot import row
from databot.db.utils import Row
from databot.recursive import call


def test_nested_dict():
    source = {'value': {'number': row.value}}
    target = {'value': {'number': 42}}
    assert call(source, Row(value=42)) == target


def test_list():
    source = {'value': [row.value, row.value]}
    target = {'value': [42, 42]}
    assert call(source, Row(value=42)) == target
