import requests


class DownloadErrror(Exception):
    pass


def handler(row):
    response = requests.get(row.key)
    if response.status_code == 200:
        yield row.key, response.text
    else:
        raise DownloadErrror('Error while downloading %s, returned status code was %s, response content:\n\n%s' % (
            row.key, response.status_code, response.text,
        ))
