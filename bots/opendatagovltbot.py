#!/usr/bin/env python3

import databot


def define(self):
    self.define('index urls')
    self.define('index pages')
    self.define('dataset urls')
    self.define('dataset pages')
    self.define('dataset data')


def run(self):
    self.compact()

    start_url = 'http://opendata.gov.lt/index.php?vars=/public/public/search'
    with self.pipe('index urls').append(start_url):
        while self.pipe('index pages').is_filled():
            with self.pipe('index pages').download():
                self.pipe('index urls').select(['td > a.path@href']).dedup()

    with self.pipe('index pages'):
        with self.pipe('dataset urls').select(['form[name=frm] > table > tr > td[3] > a@href']).dedup():
            with self.pipe('dataset pages').download():
                self.pipe('dataset data').select(databot.row.key(), [
                    'table xpath:tr[count(td)=2]', ('td[1]:content', 'td[2]:content')
                ])


if __name__ == '__main__':
    databot.Bot('sqlite:///data/opendatagovlt.db').argparse(define, run)
