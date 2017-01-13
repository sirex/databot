from databot.expressions.utils import handler


@handler(item='func')
def define(bot, *args, **kwargs):
    return bot.define(*args, **kwargs)


@handler(item='func')
def task(bot, source=None, target=None):
    if source and target:
        bot.stack.append(bot.pipe(source))
        return bot.pipe(target)
    elif source:
        return bot.pipe(source)
    else:
        return bot
