import csv
import json
import itertools


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


def flatten_rows(rows, scan_fields=1):
    _rows = []

    rows = iter(rows)

    fields = set()
    for i, row in enumerate(rows, 1):
        fields.update(get_fields(row.value))
        _rows.append(row)
        if i >= scan_fields:
            break

    fields = sorted(fields)
    _fields = list(filter(None, ['.'.join(field) for field in fields]))

    yield ['key'] + (_fields or ['value'])

    for row in itertools.chain(_rows, rows):
        yield [row.key] + list(get_values(fields, row.value))


def export(path, pipe):
    with open(path, 'w') as f:
        writer = csv.writer(f)
        for row in flatten_rows(pipe.data.rows()):
            writer.writerow(values_to_csv(row))
