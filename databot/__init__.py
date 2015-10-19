from databot.bot import Bot  # noqa
from databot.rowvalue import RowAttr
from databot.handlers.html import Call as call  # noqa
from databot.handlers.html import Join as join  # noqa
from databot.handlers.html import First as first  # noqa
from databot.handlers.html import Value as value  # noqa

row = RowAttr()


def strip(query):
    return call(str.strip, query)
