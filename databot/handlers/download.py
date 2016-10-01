import time
import bs4
import cgi

from databot.recursive import call
from databot.db.utils import Row


class DownloadErrror(Exception):
    pass


def dump_response(response):
    return {
        'headers': dict(response.headers),
        'cookies': response.cookies if isinstance(response.cookies, dict) else response.cookies.get_dict(),
        'status_code': response.status_code,
        'encoding': response.encoding,
        'content': response.content,
    }


def download(session, urlexpr, delay=None, update=None, **kwargs):
    update = update or {}

    def func(row):
        nonlocal kwargs

        if delay is not None:
            time.sleep(delay)

        if isinstance(row, Row):
            kwargs = call(kwargs, row)
            url = urlexpr(row)
        else:
            url = row

        response = session.get(url, **kwargs)

        if response.status_code == 200:
            value = dump_response(response)
            for k, fn in update.items():
                value[k] = fn(row)
            yield url, value
        else:
            raise DownloadErrror('Error while downloading %s, returned status code was %s, response content:\n\n%s' % (
                url, response.status_code, response.content,
            ))

    return func


def get_content(data, errors='strict'):
    headers = {k.lower(): v for k, v in data.get('headers', {}).items()}
    content_type_header = headers.get('content-type', '')
    content_type, params = cgi.parse_header(content_type_header)
    if content_type.lower() in ('text/html', 'text/xml'):
        soup = bs4.BeautifulSoup(data['content'], 'lxml')
        return data['content'].decode(soup.original_encoding, errors)
    elif content_type.startswith('text/'):
        return data['content'].decode(data['encoding'], errors)
    else:
        return data['content']
