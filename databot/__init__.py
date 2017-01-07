from databot.bot import Bot  # noqa
from databot.expressions.base import Expression
from databot.handlers.html import func  # noqa
from databot.handlers.html import Join as join  # noqa
from databot.handlers.html import First as first  # noqa
from databot.handlers.html import Subst as subst  # noqa

import databot.expressions.handlers  # noqa

this = Expression()
select = Expression(func='select')  # noqa
value = Expression(func='value')
