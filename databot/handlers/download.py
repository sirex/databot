import time
import requests
import bs4
import cgi

from databot.recursive import call


class DownloadErrror(Exception):
    pass


def dump_response(response):
    return {
        'headers': dict(response.headers),
        'cookies': response.cookies.get_dict(),
        'status_code': response.status_code,
        'encoding': response.encoding,
        'content': response.content,
    }


def download(url, delay=None, update=None, **kwargs):
    update = update or {}

    def func(row):
        if delay is not None:
            time.sleep(delay)
        kw = call(kwargs, row)
        _url = url(row)
        response = requests.get(_url, **kw)
        if response.status_code == 200:
            value = dump_response(response)
            for k, fn in update.items():
                value[k] = fn(row)
            yield _url, value
        else:
            raise DownloadErrror('Error while downloading %s, returned status code was %s, response content:\n\n%s' % (
                _url, response.status_code, response.content,
            ))

    return func


def get_content(data):
    content_type_header = data.get('headers', {}).get('Content-Type', '')
    content_type, params = cgi.parse_header(content_type_header)
    if content_type in ('text/html', 'text/xml'):
        soup = bs4.BeautifulSoup(data['content'], 'lxml')
        return data['content'].decode(soup.original_encoding)
    elif content_type.startswith('text/'):
        return data['content'].decode(data['encoding'])
    else:
        return data['content']
