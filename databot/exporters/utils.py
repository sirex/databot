from itertools import islice, chain

from databot.db.utils import Row
from databot.expressions.base import Expression


def _force_dict(value):
    if isinstance(value, dict):
        return value
    else:
        return {'value': value}


def updated_rows(rows, update=None):
    update = update or {}
    for row in rows:
        if callable(update):
            yield Row(key=None, value=_force_dict(update(row)))
        else:
            value = _force_dict(row.value)
            for k, call in update.items():
                if isinstance(call, Expression):
                    value[k] = call._eval(row)
                else:
                    value[k] = call(row)
            row.value = value
            yield row


def commonstart(a, b):
    for a, b in zip(a, b):
        if a == b:
            yield a
        else:
            break


def includes(items, value):
    for item in items:
        common = tuple(commonstart(value, item))
        if len(common) == len(value) or len(common) >= len(item):
            return True
    return False


def flatten_nested_dicts(nested, field=(), include=None, exclude=None):
    skip = False

    if field and include:
        if includes(include, field):
            if field in include:
                include = None
        else:
            skip = True

    if field and exclude:
        if exclude is True:
            skip = True
        elif field in exclude:
            skip = True
            exclude = True

    if skip:
        pass
    elif isinstance(nested, dict):
        for k, v in nested.items():
            yield from flatten_nested_dicts(v, field + (k,), include, exclude)
    else:
        yield (field, nested)


def separate_dicts_from_lists(nested, field=(), include=None, exclude=None):
    data = []
    lists = []
    for key, value in flatten_nested_dicts(nested, field, include, exclude):
        if isinstance(value, (tuple, list)):
            lists.append((key, value))
        else:
            data.append((key, value))
    return data, lists


def flatten_nested_lists(nested, include=None, exclude=None, field=(), context=None):
    data, lists = separate_dicts_from_lists(nested, field, include, exclude)
    data += (context or [])
    if lists:
        for key, values in lists:
            for value in values:
                yield from flatten_nested_lists(value, include, exclude, key, data)
    else:
        yield data


def detect_fields(rows, scan):
    fields = set()
    scanrows = list(islice(rows, scan))
    for row in scanrows:
        fields.update(k for k, v in row)
    return chain(scanrows, rows), fields


def get_level_keys(keys, field, include=()):
    include_all = True
    include_some = False
    if include:
        include_all = False
        for item in include:
            common = tuple(commonstart(field, item))
            if len(field) == len(common) and len(item) > len(field):
                include_some = True
                if item[len(field)] in keys:
                    yield item[len(field)]
            elif len(item) <= len(common):
                include_all = True

    if include_some is False and include_all:
        yield from sorted(keys)


def sort_fields(fields, include):
    if include:
        fields = set(fields)
        sorted_fields = []
        for item in include:
            if item in fields:
                sorted_fields.append(item)
                fields.remove(item)
            else:
                unsorted_fields = []
                for field in fields:
                    common = tuple(commonstart(field, item))
                    if len(item) == len(common) and len(field) >= len(common):
                        unsorted_fields.append(field)
                for field in sorted(unsorted_fields):
                    sorted_fields.append(field)
                    fields.remove(field)
        return sorted_fields
    else:
        return sorted(fields)


def row_to_dict(row, key_name):
    if row.key is None or key_name is None:
        return row.value
    else:
        return dict(row.value, **{key_name: row.key})


def flatten(rows, exclude=None, include: list=None, update=None, sep='.', scan=100, key_name='key'):
    include = [tuple(x.split(sep)) for x in include] if include else []
    exclude = [tuple(x.split(sep)) for x in exclude] if exclude else []

    rows = (
        x
        for row in updated_rows(rows, update)
        for x in flatten_nested_lists(row_to_dict(row, key_name), include, exclude)
    )

    rows, fields = detect_fields(rows, scan)

    fields = sort_fields(fields, include)
    yield tuple(sep.join(map(str, x)) for x in fields)

    for row in map(dict, rows):
        yield tuple(row.get(x) for x in fields)
