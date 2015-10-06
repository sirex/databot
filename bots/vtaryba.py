#!/usr/bin/env python3

import sys
import databot


def define(bot):
    bot.define('start urls')
    bot.define('archive index pages')
    bot.define('session links')
    bot.define('session pages')
    bot.define('session data')
    bot.define('question links')
    bot.define('question pages')
    bot.define('question data')


def run(bot):
    bot.compact()

    start_url = 'http://www.vilnius.lt/lit/Posedziu_archyvas/7/1724278'
    with bot.pipe('start urls').append(start_url):
        with bot.pipe('archive index pages').download():
            with bot.pipe('session links'):
                bot.select(['#wp2sw_content table.info_table tr td[1] a@href']).dedup()
                bot.pipe('session pages').download()

    with bot.pipe('session pages'):
        with bot.pipe('session data'):
            bot.select(databot.row.key(), {
                'numeris': 'form > table.info_table xpath:tr[contains(th/text(), "Posėdžio numeris")] css:td.long:text?',
                'pavadinimas': 'form > table.info_table xpath:tr[contains(th/text(), "Posėdžio pavadinimas")] css:td.long:text',
                'data': 'form > table.info_table xpath:tr[contains(th/text(), "Posėdžio data")] css:td.long:text',
                'pirmininkas': 'form > table.info_table xpath:tr[contains(th/text(), "Pirmininkas")] css:td.long div[2] a:text',
                'dalyviai': ['form > table.info_table xpath:tr[contains(th/text(), "Dalyviai")] css:td.long div[1] *:tail'],
                'kurejas': 'form > table.info_table xpath:tr[contains(th/text(), "Kūrėjas")] css:td.long div[2] a:text',
                'busena': 'form > table.info_table xpath:tr[contains(th/text(), "Būsena")] css:td.long:content',
                'klausimai': ['td > .info_table tr td[2] a@href'],
            })
        with bot.pipe('question links').select(['td > .info_table tr td[2] a@href']):
            bot.pipe('question pages').download()

    with bot.pipe('question pages'):
        bot.pipe('question data').select(databot.row.key(), {
            'posedzio_numeris': (
                'form > table.info_table xpath:tr[contains(th/text(), "Posėdžio numeris")] css:td.long:text?'
            ),
            'klausimo_numeris': (
                'form > table.info_table xpath:tr[contains(th/text(), "Klausimo nr.")] '
                'css:td.long select option[selected]@title'
            ),
            'pavadinimas': (
                'form > table.info_table xpath:tr[contains(th/text(), "Posėdžio pavadinimas")] css:td.long:text'
            ),
            'data': 'form > table.info_table xpath:tr[contains(th/text(), "Posėdžio data")] css:td.long:text',
            'klausimas': 'form > table.info_table xpath:tr[contains(th/text(), "klausimas")] css:td.long:text',
            'pastabos': 'form > table.info_table xpath:tr[contains(th/text(), "Pastabos")] css:td.long:content',
            'busena': 'form > table.info_table xpath:tr[contains(th/text(), "Būsena")] css:td.long:content',
            'rezultatai': {
                'pries': (
                    'form > table.info_table xpath:tr[contains(th/text(), "Narių balsavimo rezultatai")] '
                    'css:td.long table.info_table xpath:tr[contains(td/text(), "Prieš")] css:td[3]:text?'
                ),
                'uz': (
                    'form > table.info_table xpath:tr[contains(th/text(), "Narių balsavimo rezultatai")] '
                    'css:td.long table.info_table xpath:tr[contains(td/text(), "Už")] css:td[3]:text?'
                ),
                'susilaike': (
                    'form > table.info_table xpath:tr[contains(th/text(), "Narių balsavimo rezultatai")] '
                    'css:td.long table.info_table xpath:tr[contains(td/text(), "Susilaikė")] css:td[3]:text?'
                )
            },
            'bendru_sutarimu': (
                'form > table.info_table xpath:tr[contains(th/text(), "Narių balsavimo rezultatai")] '
                'css:td.long table[2] xpath:tr[contains(td/text(), "Bendru sutarimu")] css:td:text?'
            ),
            'balsai': ['#voteprotocol xpath:tr[count(td)>0]', {
                'person': 'td[1]:text',
                'date': 'td[2]:content',
                'value': 'td[3]:text',
            }],
            'nedalyvavo': (
                'form > table.info_table xpath:tr[contains(th/text(), "Nedalyvavo balsavime")] css:td.long:content?'
            ),
        })


if __name__ == '__main__':
    databot.Bot('sqlite:///data/vtaryba.db').argparse(sys.argv[1:], define, run)
