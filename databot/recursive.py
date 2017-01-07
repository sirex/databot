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
