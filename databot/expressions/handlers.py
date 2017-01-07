import urllib.parse

import databot.utils.urls

from databot.expressions.utils import handler


@handler(item='func')
def value(_, value):
    return value


@handler(item='method')
def cast(value, func):
    return func(value)


@handler(item='method')
def apply(value, func, *args, **kwargs):
    return func(value, *args, **kwargs)


@handler(str, 'method')
def urlparse(value):
    return urllib.parse.urlparse(value)


@handler(str, 'method')
def url(value, *args, **kwargs):
    return databot.utils.urls.url(value, *args, **kwargs)


@handler(urllib.parse.ParseResult, 'attr')
def query(url):
    return dict(urllib.parse.parse_qsl(url.query))


@handler(str, 'method')
def strip(value):
    return value.strip()


@handler(str, 'method')
def lower(value):
    return value.lower()


@handler(str, 'method')
def upper(value):
    return value.upper()


@handler(str, 'method')
def normspace(value):
    return ' '.join([x for x in value.strip().split() if x])
