from collections import namedtuple, defaultdict

HANDLERS = defaultdict(list)

Handler = namedtuple('Handler', ('handler', 'types', 'items', 'eval_args'))


class StopEval(Exception):

    def __init__(self, value):
        self.value = value


def handler(types=None, item=None, eval_args=True):
    items = item if item is None or isinstance(item, tuple) else (item,)

    for item in items:
        if item not in ('func', 'method', 'attr'):
            raise ValueError('Unknonw handler item: %r' % item)

    def decorator(func):
        if func.__name__ in HANDLERS:
            raise RuntimeError("Handler %r is already registered." % func.__name__)

        HANDLERS[func.__name__].append(Handler(func, types, items, eval_args))

        return func

    return decorator
