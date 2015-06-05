#!/usr/bin/env python3

import databot


class OpenDataGovLtBot(databot.Bot):
    db = 'sqlite:///{path}/db.sqlite3'

    def task_extract_page_links(self, item, html):
        raise NotImplementedError

    def task_extract_dataset_links(self, item, html):
        raise NotImplementedError

    def task_extract_dataset_details(self, item, html):
        raise NotImplementedError

    def init(self):
        self.define('download page links', databot.download)
        self.define('download dataset page', databot.download)
        self.define('extract page links', wrap=databot.html)
        self.define('extract dataset links', wrap=databot.html)
        self.define('extract dataset details', wrap=databot.html)
        self.define('export datasets', databot.export('datasets.csv'))

    def run(self):
        url = 'http://opendata.gov.lt/index.php?vars=/public/public/search'
        with self.task('extract page links').append(url):
            while self.task('download page links').is_filled():
                with self.task('download page links').run():
                    self.task('extract page links').run().dedup()

        with self.task('download page links'):
            with self.task('extract dataset links').run():
                with self.task('download dataset page').run():
                    self.task('extract dataset details').run()

        with self.task('extract dataset details'):
            self.task('export datasets').run()

        self.compact()


if __name__ == '__main__':
    OpenDataGovLtBot().main()
