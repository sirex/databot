import urllib.parse
import databot.utils.urls


class Row(object):
    pass


class RowTop(Row):

    def __call__(self, row):
        return {'key': row.key, 'value': row.value}

    @property
    def key(self):
        return RowItem(('key',))

    @property
    def value(self):
        return RowItem(('value',))


class RowItem(Row):

    def __init__(self, keys=()):
        self._row_keys = keys

    def __call__(self, row, *args, **kwargs):
        if callable(row):
            def func(value):
                return row(value, *args, **kwargs)
            return RowItem(self._row_keys + (func,))
        else:
            return self._get_value(row)

    def __getitem__(self, key):
        return RowItem(self._row_keys + (key,))

    def __getattr__(self, key):
        return RowItem(self._row_keys + (key,))

    def _get_value(self, value):
        for key in self._row_keys:
            if callable(key):
                value = key(value)
            elif isinstance(value, dict) or isinstance(key, int):
                value = value[key]
            else:
                value = getattr(value, key)
        return value


def get_one(value):
    if len(value) > 0:
        return value[0]


class UrlQueryRowItem(RowItem):

    def __getitem__(self, key):
        return RowItem(self._row_keys + (key, get_one))

    def __getattr__(self, key):
        return RowItem(self._row_keys + (key, get_one))


class UrlRowItem(RowItem):

    def __init__(self, url_args, url_kwargs, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._url_args = url_args
        self._url_kwargs = url_kwargs

    @property
    def query(self):
        return UrlQueryRowItem(self._row_keys + ('query', urllib.parse.parse_qs))

    def _get_value(self, value):
        value = super()._get_value(value)
        return databot.utils.urls.url(value, *self._url_args, **self._url_kwargs)
