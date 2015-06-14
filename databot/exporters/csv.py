import csv
import pathlib


class Exporter(ExporterBase):
    def __init__(self, path, columns=None, delimiter=','):
        self.path = pathlib.Path(path)
        self.columns = columns
        self.delimiter = delimiter

    def __call__(self, rows):
        if self.columns and self.path.exists():
            self.columns = self.get_columns_from_file()
        with self.path.open('a', newline='') as f:
            writer = csv.writer(f, delimiter=self.delimiter)
            writer.writerows(self.rows(rows))

    def get_columns_from_file(self):
        with self.path.open() as f:
            line = f.readline().strip()
            if line:
                return line.split(self.delimiter)

    def rows(self, rows):
        for row in rows:
            if self.columns is None:
                self.columns = sorted(row.value.keys())
                yield ['key'] + self.columns
            yield [row.key] + [row.value.get(col) for col in self.columns]
