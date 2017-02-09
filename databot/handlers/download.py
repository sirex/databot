import time

from databot.recursive import call
from databot.db.utils import Row
from databot.handlers.html import Select


class DownloadErrror(Exception):
    pass


def get_final_url(response, url):
    for resp in response.history:
        if resp.status_code in range(300, 400) and 'Location' in resp.headers:
            url = resp.headers['Location']
    return url


def dump_response(response, url):
    dump = {
        'headers': dict(response.headers),
        'cookies': response.cookies if isinstance(response.cookies, dict) else response.cookies.get_dict(),
        'status_code': response.status_code,
        'encoding': response.encoding,
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


def download(session, urlexpr, delay=None, update=None, check=None, **kwargs):
    update = update or {}

    def func(row):
        nonlocal kwargs

        if delay is not None:
            time.sleep(delay)

        if isinstance(row, Row):
            kwargs = call(kwargs, row)
            url = urlexpr._eval(row)
        else:
            url = row

        response = session.get(url, **kwargs)

        if response.status_code == 200:
            value = dump_response(response, url)
            for k, fn in update.items():
                value[k] = fn(row)
            if check:
                check_download(url, value, check)
            yield url, value
        else:
            raise DownloadErrror('Error while downloading %s, returned status code was %s, response content:\n\n%s' % (
                url, response.status_code, response.content,
            ))

    return func
