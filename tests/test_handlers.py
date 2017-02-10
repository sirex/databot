import pytest

from databot import this


def test_re():
    assert this.re('\d+').cast(int)._eval('42 comments') == 42
    assert this.re('(\d+)').cast(int)._eval('42 comments') == 42
    with pytest.raises(ValueError):
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
