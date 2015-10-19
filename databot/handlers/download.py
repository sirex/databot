import time
import bs4
import requests

from databot.recursive import call


class DownloadErrror(Exception):
    pass


def dump_response(response):
    if response.headers['Content-Type'] == 'text/html':
        soup = bs4.BeautifulSoup(response.content)
        text = response.content.decode(soup.original_encoding)
    else:
        text = response.text

    return {
        'headers': dict(response.headers),
        'cookies': dict(response.cookies),
        'status_code': response.status_code,
        'encoding': response.encoding,
        'text': text,
    }


def download(delay=None, **kwargs):
    def func(row):
        if delay is not None:
            time.sleep(delay)
        kw = call(kwargs, row)
        print(row.key, repr(kw))
        response = requests.get(row.key, **kw)
        if response.status_code == 200:
            yield row.key, dump_response(response)
        else:
            raise DownloadErrror('Error while downloading %s, returned status code was %s, response content:\n\n%s' % (
                row.key, response.status_code, response.text,
            ))
    return func
