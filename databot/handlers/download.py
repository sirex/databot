import requests


class DownloadErrror(Exception):
    pass


def dump_response(response):
    return {
        'headers': dict(response.headers),
        'status_code': response.status_code,
        'encoding': response.encoding,
        'text': response.text,
    }


def download(row):
    response = requests.get(row.key)
    if response.status_code == 200:
        yield row.key, dump_response(response)
    else:
        raise DownloadErrror('Error while downloading %s, returned status code was %s, response content:\n\n%s' % (
            row.key, response.status_code, response.text,
        ))
