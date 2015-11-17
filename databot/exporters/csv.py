import csv
import json
import itertools
import functools
import pathlib


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


def flatten_rows(rows, exclude=None, include=None, update=None, scan_fields=1):
    _rows = []
    exclude = exclude or set()
    update = update or {}

    rows = iter(rows)

    fields = set()
    for i, row in enumerate(rows, 1):
        for k, call in update.items():
            row.value[k] = call(row)
        fields.update(get_fields(row.value))
        _rows.append(row)
        if i >= scan_fields:
            break

    fields = sorted(fields)
    cols = ['key'] + (list(filter(None, ['.'.join(field) for field in fields])) or ['value'])

    if include:
        yield include
    else:
        yield [c for c in cols if c not in exclude]

    for row in itertools.chain(_rows, rows):
        for k, call in update.items():
            row.value[k] = call(row)
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


def export(path, pipe, exclude=None, include=None, update=None, append=False, header=True):
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
        rows = flatten_rows(pipe.data.rows(), exclude, include, update)

        for row in rows:
            if header:
                writer.writeheader(row)
            break

        for row in rows:
            writer.writerow(row)
