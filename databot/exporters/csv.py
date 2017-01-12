import csv
import json
import itertools
import functools
import pathlib
import funcy

from databot.db.utils import Row
from databot.expressions.base import Expression


def get_fields(data, field=()):
    fields = []
    if isinstance(data, dict):
        for k, v in sorted(data.items()):
            fields.extend(get_fields(v, field + (k,)))
    else:
        fields.append(field)
    return fields


def get_values(fields, data):
    values = []
    for field in fields:
        node = data
        for key in field:
            try:
                node = node[key]
            except (KeyError, TypeError):
                node = None
                break
        values.append(node)
    return tuple(values)


def values_to_csv(values):
    row = []
    for value in values:
        if isinstance(value, (list, dict)):
            value = json.dumps(value).encode('utf-8')
        row.append(value)
    return row


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


def detect_fields(rows):
    fields = set()
    for row in rows:
        fields.update(get_fields(row.value))
    return fields


def flatten_rows(rows, exclude=None, include: list=None, update=None, scan_fields=1):
    if callable(update) and include is None and exclude is None:
        exclude = {'key'}
    else:
        exclude = exclude or set()

    if include:
        cols = ['key'] + [c for c in include if c != 'key']
        fields = [tuple(field.split('.')) for field in cols[1:]]
        yield include
    else:
        rows = iter(rows)
        sample = funcy.take(scan_fields, rows)
        rows = itertools.chain(sample, rows)
        fields = sorted(detect_fields(updated_rows(sample, update)))
        cols = ['key'] + (list(filter(None, ['.'.join(field) for field in fields])) or ['value'])
        yield [c for c in cols if c not in exclude]

    for row in updated_rows(rows, update):
        values = [row.key] + list(get_values(fields, row.value))
        if include:
            values = dict(zip(cols, values))
            yield [values.get(k) for k in include]
        else:
            yield [v for k, v in zip(cols, values) if k not in exclude]


class BaseWriter(object):

    def __init__(self, stream):
        self.stream = stream

    def writeheader(self, row):
        self.writerow(row)


class TxtWriter(BaseWriter):

    def writerow(self, row):
        self.stream.write('%s\n' % '\t'.join(map(str, values_to_csv(row))))


class CsvWriter(BaseWriter):

    def __init__(self, stream, *args, **kwargs):
        self.writer = csv.writer(stream, *args, **kwargs)

    def writerow(self, row):
        self.writer.writerow(values_to_csv(row))


def export(path, rows, exclude=None, include=None, update=None, append=False, header=True):
    path = pathlib.Path(path)

    if path.suffix == '.txt':
        Writer = TxtWriter
    elif path.suffix == '.tsv':
        Writer = functools.partial(CsvWriter, dialect='excel-tab', lineterminator='\n')
    elif path.suffix == '.csv':
        Writer = functools.partial(CsvWriter, dialect='excel', lineterminator='\n')
    else:
        raise ValueError("Unknown file format '%s'." % path.suffix)

    mode = 'a' if append else 'w'

    with path.open(mode) as f:
        writer = Writer(f)
        rows = flatten_rows(rows, exclude, include, update)

        for row in rows:
            if header:
                writer.writeheader(row)
            break

        for row in rows:
            writer.writerow(row)
