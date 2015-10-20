#!/usr/bin/env python3

from datetime import timedelta
from databot import Bot, row, strip, first, value, lower, nspace


def define(bot):
    bot.define('sąrašas')
    bot.define('puslapiai')
    bot.define('nuorodos')
    bot.define('deklaracijų puslapiai')
    bot.define('deklaracijų puslapiai be klaidos')
    bot.define('deklaracijos')
    bot.define('žmonės')
    bot.define('seimo nariai')
    bot.define('seimo narių deklaracijų puslapiai')
    bot.define('seimo narių deklaracijos')
    bot.define('sandoriai')


def run(bot):
    bot.download_delay = 7  # seconds, vtek.lt denies access if more frequent request are detected

    start_url = (
        # List of declarations for Lithuanian Seimas (code: 188605295)
        'http://www.vtek.lt/paieska/id001/paieska.php?dekl_jkodas=188605295&dekl_vardas=&dekl_pavarde=&rasti=Surasti'
    )

    # Download all pagination pages, redownload after each 7 days
    with bot.pipe('sąrašas').clean(timedelta(days=7)).append(start_url).dedup():
        with bot.pipe('puslapiai').download():
            with bot.pipe('sąrašas').select(['.panel-body > a@href']).dedup():
                with bot.pipe('puslapiai').download():
                    # We don't want to select page links from each page, they are the same on each page.
                    bot.pipe('sąrašas').skip()

    # Extract just person name and position code for data analysis
    with bot.pipe('puslapiai'):
        bot.pipe('žmonės').select([
            'xpath://div[contains(@class,"panel-body") and count(div)=3]', (
                'div[1] > a@href', {
                    'name': 'div[1] > a:text',  # Person's full name
                    'position': 'div[2]:text',  # Position code in an institution where this person work
                }
            )
        ])

    # Download just members of parlament (101 position code) and group by full name, since URL's are changing
    with bot.pipe('puslapiai'):
        with bot.pipe('seimo nariai').clean(timedelta(days=30)).select([
            '.panel-body xpath:div[normalize-space(following-sibling::div/text())="101"]/a', (':text', '@href')
        ]).dedup():
            bot.pipe('seimo narių deklaracijų puslapiai').download(row.value, headers={'Referer': row.value})

    # Extract row data for members of parlament
    with bot.pipe('seimo narių deklaracijų puslapiai'):
        bot.pipe('seimo narių deklaracijos').select(
            nspace(lower('#asmens_duomenys xpath:./tr[contains(td/text(),"DEKLARUOJANTIS ASMUO")]/following-sibling::tr[1]/td/text()')), {  # noqa
                'vtek link': row.key,
                'deklaruojantis asmuo': '#asmens_duomenys xpath:./tr[contains(td/text(),"DEKLARUOJANTIS ASMUO")]/following-sibling::tr[1]/td/text()',  # noqa
                'darbovietė': '#asmens_duomenys xpath:./tr[contains(td/text(),"DARBOVIETĖ")][1]/following-sibling::tr[1]/td/text()',  # noqa
                'pareigos': '#asmens_duomenys xpath:./tr[contains(td/text(),"PAREIGOS")][1]/following-sibling::tr[1]/td/text()',  # noqa
                'sutuoktinis': '#asmens_duomenys xpath:./tr[contains(td/text(),"SUTUOKTINIS, SUGYVENTINIS, PARTNERIS")]/following-sibling::tr[2]/td/text()?',  # noqa
                'sutuoktinio darbovietė': '#asmens_duomenys xpath:./tr[contains(td/text(),"SUTUOKTINIO, SUGYVENTINIO, PARTNERIO DARBOVIETĖ")]/following-sibling::tr[1]/td/text()?',  # noqa
                'sutuoktinio pareigos': '#asmens_duomenys xpath:./tr[contains(td/text(),"PAREIGOS")][2]/following-sibling::tr[1]/td/text()?',  # noqa
                'deklaracijos': [
                    '#pagrindine_priedai #p_virsus', (
                        strip('tr[2] > td:text'), [
                            'xpath:./../../following-sibling::tr[1]/td/table[@id="priedas"]/tr/td/table/tr', (
                                first(strip('td[1]:text'), value(None)),   # Field name
                                first(strip('td[2]:text?'), value(None)),  # Field value
                            )
                        ]
                    )
                ]
            }
        )

    bot.compact()


def sandoriai(data):
    print(data)


def runx(bot):
    with bot.pipe('seimo narių deklaracijos'):
        bot.pipe('sandoriai').call(sandoriai)


if __name__ == '__main__':
    Bot('sqlite:///data/vtek.db').main(define, runx)
