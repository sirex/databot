import pytest

from textwrap import dedent

from databot import task, select
from databot.expressions.base import Expression, ExpressionError, Attr, Item, Func, Method

this = Expression()


def test_expression():
    assert Expression().a['b'].c(1, x=2)._stack == (
        Attr(key='a'),
        Item(key='b'),
        Method(name='c', args=(1,), kwargs={'x': 2}),
    )


def test_function_expression():
    func = Expression(func='func')
    assert func('args').a['b'].c(1, x=2)._stack == (
        Func(name='func', args=('args',), kwargs={}),
        Attr(key='a'),
        Item(key='b'),
        Method(name='c', args=(1,), kwargs={'x': 2}),
    )


def test_eval():
    assert Expression().cast(int)._eval('42') == 42
    assert Expression().cast(int)._eval('42') == 42
    assert Expression().urlparse().query.key.cast(int)._eval('http://example.com/?key=42') == 42


def test_eval_args():
    def proxy(*args, **kwargs):
        return args, kwargs

    assert this.apply(proxy, this.upper(), kw=this.upper())._eval('a') == (('a', 'A'), {'kw': 'A'})
    assert this.a.b.apply(proxy, this.a.b.upper(), kw=this.a.b.upper())._eval({'a': {'b': 'c'}}) == (
        ('c', 'C'),
        {'kw': 'C'},
    )


def test_op_eq():
    assert (this == 42)._eval(42) is True
    assert (this == 42)._eval(0) is False
    assert (this == this)._eval(42) is True
    assert (this.a == this.b)._eval({'a': 1, 'b': 2}) is False
    assert (this.a == this.b)._eval({'a': 2, 'b': 2}) is True


def test_expresion_str():
    expr = (
        task('a', 'b').
        select(['query', ('key', {
            'foo': select('query'),
        })]).
        download(this.key, check=select('query')).
        urlparse().
        query.key.cast(int)
    )
    assert str(expr) == dedent('''
        task('a', 'b').
        select(['query', ('key', {'foo': select('query')})]).
        download(this.key, check=select('query')).
        urlparse().query.key.
        cast('int')
    ''').strip()


def test_expresion_exception_handling(caplog):
    with pytest.raises(ExpressionError) as e:
        this.cast('int')._eval('foobar')
    assert str(e.value) == dedent("""
        error while processing expression:
          this.
          cast('int')
        evaluated with:
          'foobar'
    """).strip()
