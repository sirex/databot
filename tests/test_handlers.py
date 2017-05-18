import pytest

from databot import this
from databot.expressions.base import ExpressionError


def test_re():
    assert this.re('\d+').cast(int)._eval('42 comments') == 42
    assert this.re('(\d+)').cast(int)._eval('42 comments') == 42
    with pytest.raises(ExpressionError):
        this.re('\d+').cast(int)._eval('1 2 3')


def test_header():
    assert this.header().value._eval('attachment; filename="doc.odt"') == 'attachment'
    assert this.header().filename._eval('attachment; filename="doc.odt"') == 'doc.odt'


def test_header_content_type():
    value = {'Content-Type': 'text/plain;charset=UTF-8'}
    assert this['Content-Type'].header().value._eval(value) == 'text/plain'
    assert this['Content-Type'].header().type._eval(value) == 'text'
    assert this['Content-Type'].header().subtype._eval(value) == 'plain'
    assert this['Content-Type'].header().charset._eval(value) == 'UTF-8'


def test_bypass():
    data = {'key': 1, 'value': 'a'}

    assert this.value.bypass({'a': 'b'})._eval(data) == 'b'
    assert this.value.bypass({'x': 'b'})._eval(data) == 'a'

    assert this.value.bypass({'a': 'b'}).upper()._eval(data) == 'b'
    assert this.value.bypass({'x': 'b'}).upper()._eval(data) == 'A'

    assert this.value.bypass(this.key, {1: 'b'})._eval(data) == 'b'
    assert this.value.bypass(this.key, {2: 'b'})._eval(data) == 'a'

    assert this.value.bypass(this.key, {1: 'b'}).upper()._eval(data) == 'b'
    assert this.value.bypass(this.key, {2: 'b'}).upper()._eval(data) == 'A'


def test_notnull():
    with pytest.raises(ExpressionError):
        this.notnull()._eval(None)

    assert this.notnull()._eval(1) == 1


def test_list_get():
    assert this.get(1)._eval([1]) is None
