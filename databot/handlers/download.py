import time

from databot import recursive
from databot.db.utils import Row
from databot.handlers.html import Select
from databot.expressions.base import Expression


class DownloadErrror(Exception):
    pass


def get_final_url(response, url):
    for resp in response.history:
        if resp.status_code in range(300, 400) and 'Location' in resp.headers:
            url = resp.headers['Location']
    return url


def dump_response(response, url, request):
    dump = {
        'headers': dict(response.headers),
        'cookies': response.cookies if isinstance(response.cookies, dict) else response.cookies.get_dict(),
        'status_code': response.status_code,
        'encoding': response.encoding,
        'request': request,
    }

    if url:
        dump['content'] = response.content
        dump['history'] = [dump_response(r, None) for r in response.history]
        dump['url'] = get_final_url(response, url)

    return dump


def check_download(url, response, check):
    row = Row({'key': url, 'value': response})
    select = Select(check)
    select.set_row(row)
    select.check_render(row, select.html, check, many=True)


def download(session, urlexpr, delay=None, update=None, check=None, method='GET', **kwargs):
    update = update or {}

    def func(row):
        nonlocal kwargs

        if delay is not None:
            time.sleep(delay)

        if isinstance(row, Row):
            _kwargs = recursive.call(kwargs, row)
            url = urlexpr._eval(row) if isinstance(urlexpr, Expression) else urlexpr
        else:
            _kwargs = kwargs
            url = row

        response = session.request(method, url, **_kwargs)

        if response.status_code == 200:
            value = dump_response(response, url, dict(method=method, **_kwargs))
            value = recursive.call(value, row)
            if check:
                check_download(url, value, check)
            yield url, value
        else:
            raise DownloadErrror('Error while downloading %s, returned status code was %s, response content:\n\n%s' % (
                url, response.status_code, response.content,
            ))

    return func
