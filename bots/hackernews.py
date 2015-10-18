#!/usr/bin/env python3

from databot import Bot, first, strip, call


def clean_number(value):
    value = [int(v) for v in value.split() if v.isnumeric()] if value else None
    return value[0] if value else None


def define(bot):
    bot.define('start urls')
    bot.define('index')
    bot.define('news')


def run(bot):
    start_url = 'https://news.ycombinator.com/'
    with bot.pipe('start urls').append(start_url):
        with bot.pipe('index').download():
            bot.pipe('news').select([
                '.athing', (
                    'td[3] > a@href', {
                        'title': 'td[3] > a:text',
                        'score': call(clean_number, 'xpath:./following-sibling::tr[1]/td[2] css:.score:text?'),
                        'time': first(
                            'xpath:./following-sibling::tr[1]/td[2]/a[2]/text()?',
                            strip('xpath:./following-sibling::tr[1]/td[2]/text()'),
                        ),
                        'comments': call(clean_number, 'xpath:./following-sibling::tr[1]/td[2]/a[3]/text()?'),
                    }
                )
            ])

    bot.compact()


if __name__ == '__main__':
    Bot('sqlite:///data/hackernews.db').main(define, run)
