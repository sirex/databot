import copy
import collections

from databot.expressions.base import Expression


def call(value, *args, **kwargs):
    if value is None:
        return None
    elif isinstance(value, Expression):
        return value._eval(*args, **kwargs)
    elif callable(value):
        return value(*args, **kwargs)
    elif isinstance(value, dict):
        return {k: call(v, *args, **kwargs) for k, v in sorted(value.items())}
    elif isinstance(value, list):
        return [call(v, *args, **kwargs) for v in value]
    elif isinstance(value, tuple):
        return tuple([call(v, *args, **kwargs) for v in value])
    else:
        return value


def update(source, data):
    if isinstance(data, dict):
        target = copy.deepcopy(source) if isinstance(source, dict) else {}
        for key, new in data.items():
            old = target
            keys = key.split('.')
            for k in keys[:-1]:
                old = old[k] if k in old else {}
            old[keys[-1]] = new
        return target
    else:
        return data


def merge(source, data):
    for key, value in data.items():
        if isinstance(value, collections.Mapping):
            source[key] = merge(source.get(key, {}), value)
        else:
            source[key] = data[key]
    return source
