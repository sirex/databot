#!/usr/bin/env python3

import databot

from databot import row


def filter_declaration_pages_with_error(row):
    if row.value['text'] != 'Klaida.':
        yield row.key, row.value


def define(bot):
    bot.define('sąrašas')
    bot.define('puslapiai')
    bot.define('nuorodos')
    bot.define('deklaracijų puslapiai')
    bot.define('deklaracijų puslapiai be klaidos')
    bot.define('deklaracijos')
    bot.define('žmonės')


def run(bot):
    start_url = (
        'http://www.vtek.lt/paieska/id001/paieska.php?dekl_jkodas=188605295&dekl_vardas=&dekl_pavarde=&rasti=Surasti'
    )

    with bot.pipe('sąrašas').append(start_url).dedup():
        with bot.pipe('puslapiai').download():
            with bot.pipe('sąrašas').select([('.panel-body > a@href', row.value['cookies'])]).dedup():
                with bot.pipe('puslapiai').download(cookies=row.value):
                    # We don't want to select page links from each page, they are the same on each page.
                    bot.pipe('sąrašas').skip()

    with bot.pipe('puslapiai'):
        with bot.pipe('nuorodos').select(
            ['.panel-body xpath:div[normalize-space(following-sibling::div/text())="101"]/a/@href']
        ).dedup():
            bot.pipe('deklaracijų puslapiai').download()

    with bot.pipe('puslapiai'):
        bot.pipe('žmonės').select([
            'xpath://div[contains(@class,"panel-body") and count(div)=3]', (
                'div[1] > a@href', {
                    'name': 'div[1] > a:text',  # Person's full name
                    'position': 'div[2]:text',  # Position code in an institution where this person work
                }
            )
        ])

    with bot.pipe('deklaracijų puslapiai'):
        bot.pipe('deklaracijų puslapiai be klaidos').call(filter_declaration_pages_with_error)

    # TODO: export data from declaration page.

    bot.compact()


if __name__ == '__main__':
    databot.Bot('sqlite:///data/vtek.db').main(define, run)
