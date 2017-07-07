import logging

from databot.expressions.utils import handler

logger = logging.getLogger(__name__)


@handler(item='func')
def define(expr, pos, bot, *args, **kwargs):
    return bot.define(*args, **kwargs)


@handler(item='func')
def task(expr, pos, bot, source=None, target=None, watch=False):
    if source and target:
        source = bot.pipe(source)
        target = bot.pipe(target)
        return target(source)
    elif source:
        return bot.pipe(source)
    else:
        return bot


def run_single_task(bot, expr, source, target):
    task = expr._stack[0]

    logger.debug('run_single_task: %s', ', '.join(task.args))

    if (
        (source, target) == (None, None) or
        (source and target and (source.name, target.name) == task.args) or
        (source and target is None and (source.name,) == task.args) or
        (source and target is None and len(task.args) == 2 and (source.name,) == task.args[1:])
    ):
        expr._eval(bot, bot=bot)


def get_watching_tasks(bot, tasks):
    for expr in tasks:
        task = expr._stack[0]
        if task.kwargs.get('watch', False):
            source = bot.pipe(task.args[0])
            target = bot.pipe(task.args[1])
            yield target(source), expr


def run_watching_tasks(bot, tasks, source, target):
    n = 0
    has_changes = True
    while has_changes:
        n += 1
        has_changes = False
        for task, expr in tasks:
            if task.is_filled():
                has_changes = True
                logger.debug('run watching task: %r', task)
                run_single_task(bot, expr, source, target)
        if bot.limit and n >= bot.limit:
            break


def run_all_tasks(bot, tasks, source=None, target=None):
    logger.debug('run_all_tasks: limit=%r', bot.limit)

    watching_tasks = list(get_watching_tasks(bot, tasks))

    for expr in tasks:
        run_single_task(bot, expr, source, target)
        run_watching_tasks(bot, watching_tasks, source, target)
