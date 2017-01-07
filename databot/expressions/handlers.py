import urllib.parse

from databot.expressions.utils import handler


@handler(item=('func', 'method'))
def cast(value, func):
    return func(value)


@handler(item=('func', 'method'))
def apply(value, func, *args, **kwargs):
    return func(value, *args, **kwargs)


@handler(str, 'method')
def urlparse(value):
    return urllib.parse.urlparse(value)


@handler(str, 'method')
def select(selector, row, node, many=False, single=True, query=None):
    return selector.render(row, node, query, many, single)


@handler(urllib.parse.ParseResult, 'attr')
def query(url):
    return dict(urllib.parse.parse_qsl(url.query))
