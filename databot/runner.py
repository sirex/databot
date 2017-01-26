from databot.expressions.utils import handler


@handler(item='func')
def define(bot, *args, **kwargs):
    return bot.define(*args, **kwargs)


@handler(item='func')
def task(bot, source=None, target=None, watch=False):
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

    if (
        (source, target) == (None, None) or
        (source and target and (source.name, target.name) == task.args) or
        (source and target is None and (source.name,) == task.args) or
        (source and target is None and len(task.args) == 2 and (source.name,) == task.args[1:])
    ):
        expr._eval(bot)


def get_watching_tasks(bot, tasks):
    for expr in tasks:
        task = expr._stack[0]
        if task.kwargs.get('watch', False):
            source = bot.pipe(task.args[0])
            target = bot.pipe(task.args[1])
            yield target(source), expr


def run_watching_tasks(bot, tasks, source, target):
    has_changes = True
    while has_changes:
        has_changes = False
        for task, expr in tasks:
            if task.is_filled():
                has_changes = True
                run_single_task(bot, expr, source, target)


def run_all_tasks(bot, tasks, source=None, target=None):
    watching_tasks = list(get_watching_tasks(bot, tasks))

    for expr in tasks:
        run_single_task(bot, expr, source, target)
        run_watching_tasks(bot, watching_tasks, source, target)
