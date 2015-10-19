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
    bot.download_delay = 7

    start_url = (
        'http://www.vtek.lt/paieska/id001/paieska.php?dekl_jkodas=188605295&dekl_vardas=&dekl_pavarde=&rasti=Surasti'
    )

    with bot.pipe('sąrašas').append(start_url).dedup():
        with bot.pipe('puslapiai').download():
            with bot.pipe('sąrašas').select(['.panel-body > a@href']).dedup():
                with bot.pipe('puslapiai').download():
                    # We don't want to select page links from each page, they are the same on each page.
                    bot.pipe('sąrašas').skip()

    with bot.pipe('puslapiai'):
        bot.pipe('nuorodos').select([
            '.panel-body xpath:div[normalize-space(following-sibling::div/text())="101"]/a/@href',
        ])
        with bot.pipe('nuorodos'):
            bot.pipe('deklaracijų puslapiai').download(headers={'Referer': row.key})

    # with bot.pipe('puslapiai'):
    #     bot.pipe('žmonės').select([
    #         'xpath://div[contains(@class,"panel-body") and count(div)=3]', (
    #             'div[1] > a@href', {
    #                 'name': 'div[1] > a:text',  # Person's full name
    #                 'position': 'div[2]:text',  # Position code in an institution where this person work
    #             }
    #         )
    #     ])

    # with bot.pipe('deklaracijų puslapiai'):
    #     bot.pipe('deklaracijų puslapiai be klaidos').call(filter_declaration_pages_with_error)

    with bot.pipe('deklaracijų puslapiai'):
        bot.pipe('deklaracijos').select({
            'deklaruojantis asmuo': '#asmens_duomenys xpath:./tr[contains(td/text(),"DEKLARUOJANTIS ASMUO")]/following-sibling::tr[1]/td/text()',
            'darbovietė': '#asmens_duomenys xpath:./tr[contains(td/text(),"DARBOVIETĖ")]/following-sibling::tr[1]/td/text()',
            'pareigos': '#asmens_duomenys xpath:./tr[td/contains(text(),"PAREIGOS")][1]/following-sibling::tr[1]/td/text()',
            'sutuoktinis': '#asmens_duomenys xpath:./tr[contains(td/text(),"SUTUOKTINIS, SUGYVENTINIS, PARTNERIS")]/following-sibling::tr[2]/td/text()',
            'sutuoktinio darbovietė': '#asmens_duomenys xpath:./tr[contains(td/text(),"SUTUOKTINIO, SUGYVENTINIO, PARTNERIO DARBOVIETĖ")]/following-sibling::tr[1]/td/text()',
            'sutuoktinio pareigos': '#asmens_duomenys xpath:./tr[contains(td/text(),"PAREIGOS")][2]/following-sibling::tr[1]/td/text()',
        })

    bot.compact()


def runx(bot):

    # > bots/vtek.py select deklaracijų-puslapiai "['#pagrindine_priedai #p_virsus', ('tr[2] > td:text', ['xpath:./../../following-sibling::tr[1]/td/table[@id=\"priedas\"]/tr/td/table/tr[1]/td[1]/text()'])]"
    with bot.pipe('deklaracijų puslapiai'):
        bot.pipe('deklaracijos').select({
            'deklaruojantis asmuo': '#asmens_duomenys xpath:./tr[contains(td/text(),"DEKLARUOJANTIS ASMUO")]/following-sibling::tr[1]/td/text()',
            'darbovietė': '#asmens_duomenys xpath:./tr[contains(td/text(),"DARBOVIETĖ")][1]/following-sibling::tr[1]/td/text()',
            'pareigos': '#asmens_duomenys xpath:./tr[contains(td/text(),"PAREIGOS")][1]/following-sibling::tr[1]/td/text()',
            'sutuoktinis': '#asmens_duomenys xpath:./tr[contains(td/text(),"SUTUOKTINIS, SUGYVENTINIS, PARTNERIS")]/following-sibling::tr[2]/td/text()',
            'sutuoktinio darbovietė': '#asmens_duomenys xpath:./tr[contains(td/text(),"SUTUOKTINIO, SUGYVENTINIO, PARTNERIO DARBOVIETĖ")]/following-sibling::tr[1]/td/text()',
            'sutuoktinio pareigos': '#asmens_duomenys xpath:./tr[contains(td/text(),"PAREIGOS")][2]/following-sibling::tr[1]/td/text()',
        })


if __name__ == '__main__':
    databot.Bot('sqlite:///data/vtek.db').main(define, runx)
