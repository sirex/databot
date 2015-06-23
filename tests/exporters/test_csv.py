import io
import unittest

import databot.exporters.csv


class StringIOClosless(io.StringIO):
    def close(self, really=False):
        if really:
            super().close()


class FakeCsvExporter(databot.exporters.csv.Exporter):
    def __init__(self, content, *args, **kwargs):
        self.content = content
        super().__init__(*args, **kwargs)

    def open(self):
        return self.content


class CsvExporterTests(unittest.TestCase):
    def assertExport(self, value, result):
        content = StringIOClosless()
        exporter = FakeCsvExporter(content, path='')

        t1 = databot.Bot('sqlite:///:memory:').define('t1', None)
        t1.append(value)
        exporter.export(t1.data.rows())

        self.assertEqual(content.getvalue(), result)

    def test_string_value(self):
        self.assertExport([('1', 'a')], 'key,value\r\n1,a\r\n')

    def test_tuple_value(self):
        self.assertExport([('1', ('a', 'b', 'c'))], 'key,1,2,3\r\n1,a,b,c\r\n')
