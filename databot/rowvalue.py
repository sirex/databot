import functools


class Row(object):
    pass


class RowAttr(Row):

    def __call__(self, row):
        return {'key': row.key, 'value': row.value}

    @property
    def key(self):
        return RowItem('key')

    @property
    def value(self):
        return RowItem('value')


class RowItem(Row):

    def __init__(self, attr, keys=()):
        self.attr = attr
        self.keys = keys

    def __call__(self, row, *args, **kwargs):
        if callable(row):
            def func(value):
                return row(value, *args, **kwargs)
            return RowItem(self.attr, self.keys + (func,))
        else:
            value = getattr(row, self.attr)
            for key in self.keys:
                if callable(key):
                    value = key(value)
                else:
                    value = value[key]
            return value

    def __getitem__(self, key):
        return RowItem(self.attr, self.keys + (key,))

    def __getattr__(self, key):
        return RowItem(self.attr, self.keys + (key,))
