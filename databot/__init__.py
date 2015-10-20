from databot.bot import Bot  # noqa
from databot.rowvalue import RowAttr
from databot.handlers.html import Call as call  # noqa
from databot.handlers.html import Join as join  # noqa
from databot.handlers.html import First as first  # noqa
from databot.handlers.html import Value as value  # noqa

row = RowAttr()


def strip(query):
    def func(v):
        return None if v is None else v.strip()
    return call(func, query)


def lower(query):
    def func(v):
        return None if v is None else v.lower()
    return call(func, query)


def nspace(query):
    def func(v):
        if v is not None:
            return ' '.join(filter(None, map(str.strip, v.split())))
    return call(func, query)
