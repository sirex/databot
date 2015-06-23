#!/usr/bin/env python3

import requests
import string
import databot
import hashlib
from urllib.parse import urlencode

from databot.db.serializers import dumps


class Bot(databot.Bot):

    def task_extract_streets(self, row):
        for item in row.value.split('|'):
            if item.strip():
                street_id, street_name = item.split('###')
                street_name = street_name.replace('<b>', '').replace('</b>', '')
                yield street_id, street_name

    def task_download_pages(self, row):
        url = 'http://www.manogyvunai.lt/m/m_animalproblems/files/ajax_workaround.php'
        resp = requests.post(url, data={
            'do_search': '1',
            'street_id': row.key,
            'street_name': row.value,
            'house_num': '',
            'flat_num': '',
        })
        assert resp.status_code == 200
        yield row.key, resp.text

    def task_extract_search_results(self, row, html):
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

    def init(self):
        self.define('download streets suggestions', None)
        self.define('extract streets')
        self.define('download pages')
        self.define('extract search results', wrap=databot.html)

    def run(self):
        start_url = 'http://www.manogyvunai.lt/m/m_animalproblems/files/ajax_workaround.php'

        if self.task('download streets suggestions').data.count() == 0:
            for letter in list(string.ascii_lowercase):
                query = urlencode({'getStreetsByLetters': '1', 'letters': letter})
                resp = requests.post('%s?%s' % (start_url, query))
                self.task('download streets suggestions').append(letter, resp.text)

        with self.task('download streets suggestions'):
            with self.task('extract streets').run().dedup():
                with self.task('download pages').run():
                    self.task('extract search results').run()

        self.compact()


if __name__ == '__main__':
    Bot('sqlite:///{path}/gyvunai.sqlite3').main()
