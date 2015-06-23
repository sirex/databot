import csv
import itertools


class Exporter(object):
    def __init__(self, file, columns=None, delimiter=','):
        self.file = file
        self.columns = columns
        self.delimiter = delimiter

    def export(self, rows):
        writer = csv.writer(self.file, delimiter=self.delimiter)
        writer.writerows(self.rows(rows))

    def rows(self, rows):
        row = next(rows)

        if isinstance(row.value, (tuple, list)):
            mode = 'tuple'
        elif isinstance(row.value, str):
            mode = 'string'
        else:
            mode = 'mapping'

        if self.columns is not None:
            cols = self.columns
        elif mode == 'tuple':
            cols = [str(i) for i in range(1, len(row.value)+1)]
        elif mode == 'string':
            cols = ['value']
        else:
            cols = sorted(row.value.keys())

        yield ['key'] + cols

        for row in itertools.chain([row], rows):
            if mode == 'tuple':
                yield [row.key] + list(row.value)
            elif mode == 'string':
                yield [row.key, row.value]
            else:
                yield [row.key] + [row.value.get(col) for col in cols]
