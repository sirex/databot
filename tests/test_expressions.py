from databot.expressions.base import Expression, Attr, Item, Func, Method


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
    this = Expression()

    def proxy(*args, **kwargs):
        return args, kwargs

    assert this.apply(proxy, this.upper(), kw=this.upper())._eval('a') == (('a', 'A'), {'kw': 'A'})
