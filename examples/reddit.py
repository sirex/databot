#!/usr/bin/env python3

import databot


def define(bot):
    bot.define('index')
    bot.define('news')


def run(bot):
    index = bot.pipe('index')
    news = bot.pipe('news')

    with index.download('https://www.reddit.com/'):
        news.select([
            '.thing.link', (
                '.entry .title > a@href', {
                    'title': '.entry .title > a:text',
                    'score': '.midcol .score.likes@title',
                    'time': databot.first(['.tagline time@datetime']),
                    'comments': '.entry a.comments:text',
                }
            )
        ])

    news.export('/tmp/reddit.jsonl')


if __name__ == '__main__':
    databot.Bot('/tmp/reddit.db').main(define, run)
