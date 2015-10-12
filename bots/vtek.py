#!/usr/bin/env python3

import sys
import databot


def define(bot):
    bot.define('sąrašas')
    bot.define('puslapiai')
    bot.define('nuorodos')
    bot.define('deklaracijų puslapiai')
    bot.define('deklaracijos')


def run(bot):
    start_url = (
        'http://www.vtek.lt/paieska/id001/paieska.php?dekl_jkodas=188605295&dekl_vardas=&dekl_pavarde=&rasti=Surasti'
    )
    with bot.pipe('sąrašas').append(start_url).dedup():
        with bot.pipe('puslapiai').download():
            with bot.pipe('sąrašas').select(['.panel-body > a@href']).dedup():
                with bot.pipe('puslapiai').download():
                    with bot.pipe('nuorodos').select(
                        ['.panel-body xpath:div[normalize-space(following-sibling::div/text())="101"]/a/@href']
                    ).dedup():
                        bot.pipe('deklaracijų puslapiai').download()

    bot.compact()


if __name__ == '__main__':
    databot.Bot('sqlite:///data/vtek.db').argparse(sys.argv[1:], define, run)
