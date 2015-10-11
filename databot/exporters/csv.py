import csv
import json


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
            except KeyError:
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


def export(path, pipe):
    with open(path, 'w') as f:
        writer = csv.writer(f)

        fields = []
        for row in pipe.data.rows():
            fields = get_fields(row.value)
            break

        writer.writerow(['key'] + ['.'.join(field) for field in fields])

        for row in pipe.data.rows():
            values = values_to_csv(get_values(fields, row.value))
            writer.writerow([row.key] + values)
