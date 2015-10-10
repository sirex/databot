#!/usr/bin/env python3

import sys
import databot


def define(bot):
    bot.define('savivaldybių rinkimai')
    bot.define('savivaldybių sąrašo puslapiai')
    bot.define('savivaldybių rezultatų nuorodos')
    bot.define('savivaldybių rezultatų puslapiai')
    bot.define('tarybos nariai')


def run(bot):
    bot.compact()

    rinkimai = [
        (
            'http://www.2013.vrk.lt/2015_savivaldybiu_tarybu_rinkimai/output_lt/savivaldybiu_tarybu_sudetis/savivaldybes.html',
            {
                'date': '2015-03-01',
            }
        )
    ]

    with bot.pipe('savivaldybių rinkimai').append(rinkimai).dedup():
        with bot.pipe('savivaldybių sąrašo puslapiai').download():
            with bot.pipe('savivaldybių rezultatų nuorodos').select(['table.partydata tr td b > a@href']).dedup():
                with bot.pipe('savivaldybių rezultatų puslapiai').download():
                    bot.pipe('tarybos nariai').select([
                        'xpath://table[contains(@class,"partydata3")][1]/tr[count(td)>0]', (
                            'tr td[2] > a@href', {
                                'sąrašas': 'tr td[1]:text',
                                'pavardė vardas': 'tr td[2] > a:text',
                                'įgaliojimai pripažinti': 'tr td[3]:text',
                            }
                        )
                    ])


if __name__ == '__main__':
    databot.Bot('sqlite:///data/savivaldybiu-rinkimai.db').argparse(sys.argv[1:], define, run)
