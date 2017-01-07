from collections import namedtuple, defaultdict

HANDLERS = defaultdict(list)

Handler = namedtuple('Handler', ('handler', 'types', 'items'))


def handler(types=None, item=None):
    items = item if item is None or isinstance(item, tuple) else (item,)

    def decorator(func):
        if func.__name__ in HANDLERS:
            raise RuntimeError("Handler %r is already registered." % func.__name__)

        HANDLERS[func.__name__].append(Handler(func, types, items))

        def wrapper(*args, **kwargs):
            func(*args, **kwargs)

        return func

    return decorator
