import logging

from collections import namedtuple

from databot.expressions.utils import HANDLERS

logger = logging.getLogger(__name__)


Attr = namedtuple('Attr', ('key',))
Item = namedtuple('Item', ('key',))
Method = namedtuple('Method', ('name', 'args', 'kwargs'))
Func = namedtuple('Func', ('name', 'args', 'kwargs'))


class ExpressionError(Exception):
    pass


class Expression:

    def __init__(self, stack=(), func=None):
        self._stack = stack
        self._func = func

    def __getitem__(self, key):
        return self._add(Item(key))

    def __getattr__(self, key):
        return self._add(Attr(key))

    def __call__(self, *args, **kwargs):
        if self._func:
            return self._add(Func(self._func, args, kwargs))
        elif self._stack and isinstance(self._stack[-1], Attr):
            attr = self._stack[-1]
            self._stack = self._stack[:-1]
            return self._add(Method(attr.key, args, kwargs))
        else:
            raise ExpressionError('Invalid expression. You can call only function expressions or attributes.')

    def _add(self, *items):
        return Expression(self._stack + items)

    def _eval(self, value):
        for item in self._stack:
            if isinstance(item, Func):
                for handler in HANDLERS[item.name]:
                    conditions = (
                        (handler.items is None or 'func' in handler.items) and
                        (handler.types is None or isinstance(value, handler.types))
                    )
                    if conditions:
                        logger.debug('eval: %s.%s', handler.handler.__module__, handler.handler.__name__)
                        value = handler.handler(value, *item.args, **item.kwargs)
                        break
                else:
                    raise ExpressionError("Unknown function %r for value %r." % (item.name, value))

            elif isinstance(item, Method):
                for handler in HANDLERS[item.name]:
                    conditions = (
                        (handler.items is None or 'method' in handler.items) and
                        (handler.types is None or isinstance(value, handler.types))
                    )
                    if conditions:
                        value = handler.handler(value, *item.args, **item.kwargs)
                        break
                else:
                    value = getattr(value, item.name)(*item.args, **item.kwargs)

            elif isinstance(item, Attr):
                for handler in HANDLERS[item.key]:
                    conditions = (
                        (handler.items is None or 'attr' in handler.items) and
                        (handler.types is None or isinstance(value, handler.types))
                    )
                    if conditions:
                        value = handler.handler(value)
                        break
                else:
                    if isinstance(value, dict):
                        value = value[item.key]
                    else:
                        value = getattr(value, item.key)

            elif isinstance(item, Item):
                value = value[item.key]

            else:
                raise ExpressionError("Unknown item type %r." % item)

        return value
