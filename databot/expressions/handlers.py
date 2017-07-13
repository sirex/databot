import re as re_
import urllib.parse
import datetime
import cgi
import typing
import builtins

import databot.utils.urls

from databot.expressions.utils import StopEval
from databot.expressions.utils import handler
from databot.pipes import TaskPipe, Pipe
from databot.tasks import Task


@handler(item='func')
def value(expr, pos, _, value):
    return value


@handler(item='method')
def cast(expr, pos, value, func):
    return func(value)


@handler(item='method')
def apply(expr, pos, value, func, *args, **kwargs):
    return func(value, *args, **kwargs)


@handler(str, 'method')
def urlparse(expr, pos, value):
    return urllib.parse.urlparse(value)


@handler(str, 'method')
def url(expr, pos, value, *args, **kwargs):
    return databot.utils.urls.url(value, *args, **kwargs)


@handler(str, 'method')
def header(expr, pos, value):
    value, params = cgi.parse_header(value)
    assert 'value' not in params
    params['value'] = value
    item = expr._stack[pos - 1] if len(expr._stack) > 1 else None
    if hasattr(item, 'key') and item.key.lower() == 'content-type':
        type, subtype = value.split('/')
        assert 'type' not in params
        params['type'] = type
        assert 'subtype' not in params
        params['subtype'] = subtype
    return params


@handler(urllib.parse.ParseResult, 'attr')
def query(url):
    return dict(urllib.parse.parse_qsl(url.query))


@handler(str, 'method')
def strip(expr, pos, value, chars=None):
    return value.strip(chars)


@handler(str, 'method')
def lower(expr, pos, value):
    return value.lower()


@handler(str, 'method')
def upper(expr, pos, value):
    return value.upper()


@handler(str, 'method')
def replace(expr, pos, value, old, new, count=None):
    if count is None:
        return value.replace(old, new)
    else:
        return value.replace(old, new, count)


@handler(list, 'method')
def join(expr, pos, value, sep=' '):
    return sep.join(map(str, value))


@handler(str, 'method')
def normspace(expr, pos, value):
    return ' '.join([x for x in value.strip().split() if x])


@handler(str, 'method')
def re(expr, pos, value, pattern):
    matches = re_.findall(pattern, value)
    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        raise ValueError("More than one match found for pattern: %r in value: %r." % (pattern, value))


@handler(str, 'method')
def rall(expr, pos, value, pattern):
    return re_.findall(pattern, value)


@handler(str, 'method')
def sub(expr, pos, value, pattern, substitution):
    return re_.sub(pattern, substitution, value)


@handler(Task, 'method')
def once(expr, pos, value):
    if expr._evals > 1:
        raise StopEval(value)
    else:
        return value


@handler(Task, 'method')
def freq(expr, pos, value, timedelta=None, **kwargs):
    if isinstance(value, (Pipe, TaskPipe)):
        if timedelta is None:
            timedelta = datetime.timedelta(**kwargs)
        if value.age() < timedelta:
            raise StopEval(value)
    else:
        raise TypeError("Unsupported task type: %r." % type(value))

    return value


@handler(Task, 'method')
def daily(expr, pos, value):
    if isinstance(value, (Pipe, TaskPipe)):
        if value.age() < datetime.timedelta(days=1):
            raise StopEval(value)
    else:
        raise TypeError("Unsupported task type: %r." % type(value))

    return value


@handler(Task, 'method')
def weekly(expr, pos, value):
    if isinstance(value, (Pipe, TaskPipe)):
        if value.age() < datetime.timedelta(days=7):
            raise StopEval(value)
    else:
        raise TypeError("Unsupported task type: %r." % type(value))

    return value


@handler(Task, 'method')
def monthly(expr, pos, value):
    if isinstance(value, (Pipe, TaskPipe)):
        if value.age() < datetime.timedelta(days=30):
            raise StopEval(value)
    else:
        raise TypeError("Unsupported task type: %r." % type(value))

    return value


@handler(item='method')
def null(expr, pos, value):
    """Stop expression execution if value is None."""
    if value is None:
        raise StopEval(value)
    else:
        return value


@handler(item='method')
def notnull(expr, pos, value):
    """Raises ValueError if value is None."""
    if value is None:
        raise ValueError('value should not be null')
    else:
        return value


@handler(item='method')
def bypass(expr, pos, value, *args, **kwargs):
    """Stop expression execution if key is found in given mapping.

    bypass(mapping) - in this case key is value
    bypass(key, mapping) - in this case key is another expression

    If key is found in given mapping, expression will stop execution and returns with a mapping[key] value.
    """
    if len(args) == 1:
        key = value
        mapping, = args
    elif len(args) == 2:
        key, mapping = args
    else:
        raise TypeError('bypass() takes only one or tow arguments, got: %d' % len(args))

    if key in mapping:
        raise StopEval(mapping[key])
    elif 'default' in kwargs:
        raise StopEval(kwargs['default'])
    else:
        return value


@handler(Task, item='method', eval_args=False)
def download(expr, pos, pipe, *args, **kwargs):
    return pipe.download(*args, **kwargs)


@handler((list, tuple), item='method')
def get(expr, pos, value, key, default=None):
    if len(value) - 1 >= key:
        return value[key]
    else:
        return default


@handler(item='func')
def utcnow(expr, pos, value):
    return datetime.datetime.utcnow()


@handler(str, item='method')
def strptime(expr, pos, value, format):
    return datetime.datetime.strptime(value, format)


@handler(typing.Iterable, item='method')
def sort(expr, pos, value, **kwargs):
    return sorted(value, **kwargs)


@handler(typing.Iterable, item='method')
def min(expr, pos, value, **kwargs):
    return builtins.min(value, **kwargs)


@handler(typing.Iterable, item='method')
def max(expr, pos, value, **kwargs):
    return builtins.max(value, **kwargs)


@handler(item='func')
def strformat(expr, pos, value, s, *args, **kwargs):
    return s.format(*args, **kwargs)
