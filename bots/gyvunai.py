#!/usr/bin/env python3

import sys
import requests
import string
import databot
import hashlib

from urllib.parse import urlencode
from databot.db.serializers import dumps
from databot.handlers.download import dump_response
from databot.handlers.html import create_bs4_parser


def extract_streets(row):
    for item in row.value.split('|'):
        if item.strip():
            street_id, street_name = item.split('###')
            street_name = street_name.replace('<b>', '').replace('</b>', '')
            yield street_id, street_name


def download_page(row):
    url = 'http://www.manogyvunai.lt/m/m_animalproblems/files/ajax_workaround.php'
    resp = requests.post(url, data={
        'do_search': '1',
        'street_id': row.key,
        'street_name': row.value,
        'house_num': '',
        'flat_num': '',
    })
    yield row.key, dump_response(resp)


def extract_search_results(row):
    html = create_bs4_parser(row)
    for tr in html.select('#ttt > tr'):
        td = tr('td')
        if len(td) == 5:
            data = {
                'gatve': td[1].string,
                'butas': td[2].string,
                'rusis': td[3].string,
                'veisle': td[4].string,
            }
            yield hashlib.sha1(dumps(data)).hexdigest(), data


def define(bot):
    bot.define('street suggestions')
    bot.define('streets')
    bot.define('pages')
    bot.define('extract search results')
    bot.define('osm addresses')
    bot.define('extract osm addresses')


def run(bot):
    bot.compact()

    start_url = 'http://www.manogyvunai.lt/m/m_animalproblems/files/ajax_workaround.php'

    if bot.pipe('street suggestions').data.count() == 0:
        for letter in list(string.ascii_lowercase):
            query = urlencode({'getStreetsByLetters': '1', 'letters': letter})
            resp = requests.post('%s?%s' % (start_url, query))
            bot.pipe('street suggestions').append(letter, resp.text)

    with bot.pipe('street suggestions'):
        with bot.pipe('streets').call(extract_streets).dedup():
            with bot.pipe('pages').call(download_page):
                bot.pipe('extract search results').call(extract_search_results)

    bot.pipe('extract search results').export('data/gyvunai.csv')


if __name__ == '__main__':
    databot.Bot('sqlite:///data/gyvunai.db').argparse(sys.argv[1:], define, run)
