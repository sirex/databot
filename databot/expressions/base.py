import logging

from collections import namedtuple

from databot.expressions.utils import StopEval
from databot.expressions.utils import HANDLERS

logger = logging.getLogger(__name__)


Attr = namedtuple('Attr', ('key',))
Item = namedtuple('Item', ('key',))
Method = namedtuple('Method', ('name', 'args', 'kwargs'))
Func = namedtuple('Func', ('name', 'args', 'kwargs'))


class _HandlerRepr:

    def __init__(self, handler, args, kwargs):
        self.handler = handler
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        args = ', '.join(
            [repr(x) for x in self.args] +
            ['%s=%r' % (k, v) for k, v in self.kwargs.items()]
        )
        return '%s.%s(%s)' % (self.handler.__module__, self.handler.__name__, args)


class ExpressionError(Exception):
    pass


class Expression:

    def __init__(self, stack=(), func=None):
        self._stack = stack
        self._func = func
        self._reset()

    def __eq__(self, other):
        return self._add(Method('__eq__', (other,), {}))

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

    def _reset(self):
        self._evals = 0

    def _eval_args(self, value, item, eval_args=True):
        if eval_args:
            args = tuple(v._eval(value) if isinstance(v, Expression) else v for v in item.args)
            kwargs = {k: v._eval(value) if isinstance(v, Expression) else v for k, v in item.kwargs.items()}
            return args, kwargs
        else:
            return item.args, item.kwargs

    def _eval(self, value, base=None):
        """Evaluate expression with given value and base.

        Parameters
        ----------
        value : object
            Value used to evaluate this expression.
        base : object
            Base value used as value for expressions passed as arguments. If not given, base will be equal to value.

        """
        self._evals += 1
        base = value if base is None else base
        for i, item in enumerate(self._stack):
            if isinstance(item, Func):
                for handler in HANDLERS[item.name]:
                    conditions = (
                        (handler.items is None or 'func' in handler.items) and
                        (handler.types is None or isinstance(value, handler.types))
                    )
                    if conditions:
                        args, kwargs = self._eval_args(base, item, handler.eval_args)
                        logger.debug('eval: %s', _HandlerRepr(handler.handler, (value,) + args, kwargs))
                        try:
                            value = handler.handler(self, i, value, *args, **kwargs)
                        except StopEval:
                            logger.debug('eval: StopEval')
                            return value
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
                        args, kwargs = self._eval_args(base, item, handler.eval_args)
                        logger.debug('eval: %s', _HandlerRepr(handler.handler, (value,) + args, kwargs))
                        try:
                            value = handler.handler(self, i, value, *args, **kwargs)
                        except StopEval:
                            logger.debug('eval: StopEval')
                            return value
                        break
                else:
                    method = getattr(value, item.name)
                    args, kwargs = self._eval_args(base, item)
                    logger.debug('eval: %s', _HandlerRepr(method, args, kwargs))
                    value = method(*args, **kwargs)

            elif isinstance(item, Attr):
                for handler in HANDLERS[item.key]:
                    conditions = (
                        (handler.items is None or 'attr' in handler.items) and
                        (handler.types is None or isinstance(value, handler.types))
                    )
                    if conditions:
                        logger.debug('eval: %s', _HandlerRepr(handler.handler, (value,), {}))
                        try:
                            value = handler.handler(value)
                        except StopEval:
                            logger.debug('eval: StopEval')
                            return value
                        break
                else:
                    if isinstance(value, dict):
                        logger.debug('eval: [%r]', item.key)
                        value = value[item.key]
                    else:
                        logger.debug('eval: %r.%s', value, item.key)
                        value = getattr(value, item.key)

            elif isinstance(item, Item):
                logger.debug('eval: [%r]', item.key)
                value = value[item.key]

            else:
                raise ExpressionError("Unknown item type %r." % item)

        return value
