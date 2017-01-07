import pytest

from databot import this


def test_re():
    assert this.re('\d+').cast(int)._eval('42 comments') == 42
    assert this.re('(\d+)').cast(int)._eval('42 comments') == 42
    with pytest.raises(ValueError):
        this.re('\d+').cast(int)._eval('1 2 3')
