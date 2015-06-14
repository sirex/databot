#!/usr/bin/env python3

import databot
import schematics


class Dataset(schematics.Model):
    kodas = schematics.StringType(serialized_name='Kodas', required=True)
    pavadinimas = schematics.StringType(serialized_name='Pavadinimas', required=True)
    alternatyvus_pavadinimas = schematics.StringType(serialized_name='Alternatyvus pavadinimas')
    apibudinimas = schematics.StringType(serialized_name='Apibūdinimas')
    kategorija = schematics.StringType(serialized_name='Kategorija (informacijos sritis)')
    reiksminiai_zodziai = schematics.StringType(serialized_name='Reikšminiai žodžiai')
    tvarkytojas = schematics.StringType(serialized_name='Rinkmenos tvarkytojas')
    kontaktai = schematics.StringType(serialized_name='Kontaktiniai duomenys')
    rusis = schematics.StringType(serialized_name='Rinkmenos rūšis')
    formatas = schematics.StringType(serialized_name='Duomenų formatas')
    pradzios_data = schematics.DateTimeType(('%Y', '%Y-%m-%d'), serialized_name='Rinkmenos pradžios data')
    pabaigos_data = schematics.DateTimeType(('%Y', '%Y-%m-%d'), serialized_name='Rinkmenos pabaigos data')
    atnaujinimo_daznumas = schematics.StringType(serialized_name='Atnaujinimo dažnumas')
    nuoroda = schematics.URLType(serialized_name='Internetinis adresas')
    licencija = schematics.StringType(serialized_name='Rinkmenos duomenų teikimo sąlygos')
    patikimumas = schematics.StringType(serialized_name='Duomenų patikimumas')
    issamumas = schematics.StringType(serialized_name='Duomenų išsamumas')
    publikavimo_data = schematics.DateTimeType('%Y-%m-%d %H:%M:%S', serialized_name='Rinkmenos aprašymo publikavimo duomenys')


class OpenDataGovLtBot(databot.Bot):

    def task_extract_page_links(self, row, html):
        for link in html.select('td > a.path'):
            if link.string.strip().isalnum():
                yield databot.normurl(link['href'])

    def task_extract_dataset_links(self, row, html):
        for link in html.select('form[name=frm] > table > tbody > tr > td:nth-child(3) > a'):
            yield databot.normurl(link['href'])

    def task_extract_dataset_details(self, row, html):
        data = Dataset({td[0].string: td[1].string for td in html.select('table > tr > td')})
        yield data.validate() and (data.kodas, data.to_primitive())

    def init(self):
        self.define('download page links', databot.download)
        self.define('download dataset page', databot.download)
        self.define('extract page links', wrap=databot.html)
        self.define('extract dataset links', wrap=databot.html)
        self.define('extract dataset details', wrap=databot.html)

    def run(self):
        start_url = 'http://opendata.gov.lt/index.php?vars=/public/public/search'

        with self.task('extract page links').append(start_url):
            while self.task('download page links').is_filled():
                with self.task('download page links').run():
                    self.task('extract page links').run().dedup()

        with self.task('download page links'):
            with self.task('extract dataset links').run():
                with self.task('download dataset page').run():
                    self.task('extract dataset details').run()

        self.compact()

        self.task('extract dataset details').export('datasets.csv')



if __name__ == '__main__':
    OpenDataGovLtBot('sqlite:///{path}/db.sqlite3').main()
