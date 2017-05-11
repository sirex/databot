import csv
import json
import functools
import pathlib

from databot.exporters.utils import flatten


def values_to_csv(values):
    row = []
    for value in values:
        if isinstance(value, (list, dict)):
            value = json.dumps(value).encode('utf-8')
        row.append(value)
    return row


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
        rows = flatten(rows, exclude, include, update)

        for row in rows:
            if header:
                writer.writeheader(row)
            break

        for row in rows:
            writer.writerow(row)
