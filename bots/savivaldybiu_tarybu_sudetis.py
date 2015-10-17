#!/usr/bin/env python3

import sys
import databot
import functools


@functools.lru_cache()
def get_first_names():
    return {v['name'].lower() for v in databot.Bot('sqlite:///data/vardai.db').define('vardų sąrašas').data.values()}


def split_first_last_name(names):
    first_name = []
    last_name = []
    first_names = get_first_names()
    for name in names:
        if name.lower() in first_names:
            first_name.append(name)
        else:
            last_name.append(name)

    if not first_name:
        raise ValueError('Could not detect first name for "%s".' % ' '.join(names))

    return ' '.join(first_name), ' '.join(last_name)


def split_name(data):
    names = data.pop('pavardė vardas').strip().split()

    if names[-1] in ('(MERAS)', '(MERĖ)'):
        meras = True
        names = names[:-1]
    else:
        meras = False

    assert len(names) > 1

    if len(names) > 2:
        first_name, last_name = split_first_last_name(names)
    else:
        last_name, first_name = names

    data['meras'] = meras
    data['vardas'] = first_name
    data['pavardė'] = last_name

    return data


def define(bot):
    bot.define('savivaldybių rinkimai')
    bot.define('savivaldybių sąrašo puslapiai')
    bot.define('savivaldybių rezultatų nuorodos')
    bot.define('savivaldybių rezultatų puslapiai')
    bot.define('tarybos nariai')


def run(bot):

    if bot.run('2015'):
        start_url = 'http://www.2013.vrk.lt/2015_savivaldybiu_tarybu_rinkimai/output_lt/savivaldybiu_tarybu_sudetis/savivaldybes.html'
        with bot.pipe('savivaldybių rinkimai').append(start_url).dedup():
            with bot.pipe('savivaldybių sąrašo puslapiai').download():
                with bot.pipe('savivaldybių rezultatų nuorodos').select(['table.partydata tr td b > a@href']).dedup():
                    with bot.pipe('savivaldybių rezultatų puslapiai').download():
                        bot.pipe('tarybos nariai').select([
                            'xpath://table[contains(@class,"partydata3")][1]/tr[count(td)>0]', (
                                'tr td[2] > a@href', databot.call(split_name, {
                                    'sąrašas': 'tr td[1]:text',
                                    'pavardė vardas': 'tr td[2] > a:text',
                                    'įgaliojimai pripažinti': 'tr td[3]:text',
                                    'savivaldybė': '/font[size="5"] > b:text',
                                    'kadencija': databot.value(2015),
                                })
                            )
                        ])

    bot.compact()


if __name__ == '__main__':
    databot.Bot('sqlite:///data/savivaldybiu-rinkimai.db').argparse(sys.argv[1:], define, run)
