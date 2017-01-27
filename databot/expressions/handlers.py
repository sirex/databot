import re as re_
import urllib.parse

import databot.utils.urls

from databot.tasks import Task
from databot.expressions.utils import StopEval
from databot.expressions.utils import handler


@handler(item='func')
def value(expr, _, value):
    return value


@handler(item='method')
def cast(expr, value, func):
    return func(value)


@handler(item='method')
def apply(expr, value, func, *args, **kwargs):
    return func(value, *args, **kwargs)


@handler(str, 'method')
def urlparse(expr, value):
    return urllib.parse.urlparse(value)


@handler(str, 'method')
def url(expr, value, *args, **kwargs):
    return databot.utils.urls.url(value, *args, **kwargs)


@handler(urllib.parse.ParseResult, 'attr')
def query(url):
    return dict(urllib.parse.parse_qsl(url.query))


@handler(str, 'method')
def strip(expr, value):
    return value.strip()


@handler(str, 'method')
def lower(expr, value):
    return value.lower()


@handler(str, 'method')
def upper(expr, value):
    return value.upper()


@handler(str, 'method')
def replace(expr, value, old, new, count=None):
    if count is None:
        return value.replace(old, new)
    else:
        return value.replace(old, new, count)


@handler(list, 'method')
def join(expr, value, sep=' '):
    return sep.join(map(str, value))


@handler(str, 'method')
def normspace(expr, value):
    return ' '.join([x for x in value.strip().split() if x])


@handler(str, 'method')
def re(expr, value, pattern):
    matches = re_.findall(pattern, value)
    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        raise ValueError("More than one match found for pattern: %r in value: %r." % (pattern, value))


@handler(Task, 'method')
def once(expr, value):
    if expr._evals > 1:
        raise StopEval()
    else:
        return value
