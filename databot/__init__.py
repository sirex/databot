from databot.bot import Bot  # noqa
from databot.rowvalue import RowTop
from databot.handlers.html import func, url, text  # noqa
from databot.handlers.html import Call as call  # noqa
from databot.handlers.html import Join as join  # noqa
from databot.handlers.html import First as first  # noqa
from databot.handlers.html import Subst as subst  # noqa
from databot.handlers.html import Value as value  # noqa

row = RowTop()

strip = func(skipna=True)(str.strip)  # noqa
lower = func(skipna=True)(str.lower)
nspace = func(skipna=True)(lambda v: ' '.join(filter(None, map(str.strip, v.split()))))
