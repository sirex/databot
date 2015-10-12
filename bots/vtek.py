#!/usr/bin/env python3

import sys
import databot


def define(bot):
    bot.define('sąrašas')
    bot.define('puslapiai')
    bot.define('nuorodos')


def run(bot):
    bot.compact()

    start_url = (
        'http://www.vtek.lt/paieska/id001/paieska.php?dekl_jkodas=188605295&dekl_vardas=&dekl_pavarde=&rasti=Surasti'
    )
    with bot.pipe('sąrašas').append(start_url).dedup():
        with bot.pipe('puslapiai').download():
            with bot.pipe('sąrašas').select(['.panel-body > a@href']).dedup():
                bot.pipe('puslapiai').download()


if __name__ == '__main__':
    databot.Bot('sqlite:///data/savivaldybiu-rinkimai.db').argparse(sys.argv[1:], define, run)
