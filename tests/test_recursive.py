from databot import this
from databot.db.utils import Row
from databot.recursive import call


def test_nested_dict():
    source = {'value': {'number': this.value}}
    target = {'value': {'number': 42}}
    assert call(source, Row(value=42)) == target


def test_list():
    source = {'value': [this.value, this.value]}
    target = {'value': [42, 42]}
    assert call(source, Row(value=42)) == target
