import os
import io
import unittest
import mock
import tempfile
import sqlalchemy as sa
import freezegun

from databot import Bot
from databot.db import migrations
from databot.db.models import Models

import tests.db


class StatusTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)

    def test_status(self):
        self.bot.define('p1')
        self.bot.define('p2')
        self.bot.main(argv=['status'])
        self.assertEqual(self.output.getvalue(), (
            '   id              rows  source\n'
            '       errors      left    target\n'
            '=================================\n'
            '    1                 0  p1\n'
            '---------------------------------\n'
            '    2                 0  p2\n'
            '---------------------------------\n'
        ))


class SelectTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)

    def test_select(self):
        self.bot.define('p1').append('http://example.com/', {'text': '<div>value</div>'})
        self.bot.main(argv=['select', 'p1', 'div:text'])
        self.assertEqual(self.output.getvalue(), (
            '- key: \'value\'\n'
            '  value: None\n'
        ))

    def test_not_found(self):
        self.bot.define('p1')
        self.bot.main(argv=['select', 'p1', '-k', 'missing', 'div:text'])
        self.assertEqual(self.output.getvalue(), (
            'Not found.\n'
        ))


class DownloadTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)

    @mock.patch('requests.get')
    def test_download(self, get):
        resp = mock.Mock()
        resp.status_code = 200
        resp.encoding = 'utf-8'
        resp.headers = {'Content-Type': 'text/html'}
        resp.content = b'<div>It works!</div>'
        resp.cookies = {}

        get.return_value = resp

        self.bot.main(argv=['download', 'http://example.com/'])
        self.assertEqual(self.output.getvalue(), '\n'.join([
            "- key: 'http://example.com/'",
            "  headers:",
            "    {'Content-Type': 'text/html'}",
            "  cookies:",
            "    {}",
            "  status_code: 200",
            "  encoding: 'utf-8'",
            "  text:",
            "    <div>It works!</div>",
            "",
        ]))


class SkipTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)

    def test_skip(self):
        self.bot.define('p1').append([1, 2, 3, 4])
        self.bot.define('p2')
        self.bot.main(argv=['skip', 'p1', 'p2'])
        self.assertEqual(self.output.getvalue(), '\n'.join(map(str.rstrip, [
            "   id              rows  source  ",
            "       errors      left    target",
            "=================================",
            "    1                 4  p1      ",
            "            0         0    p2    ",
            "---------------------------------",
            "    2                 0  p2      ",
            "---------------------------------",
            "",
        ])))


class ResetTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)

    def test_reset(self):
        self.bot.define('p1').append([1, 2, 3, 4])
        self.bot.define('p2')
        self.bot.main(argv=['reset', 'p1', 'p2'])
        self.assertEqual(self.output.getvalue(), '\n'.join(map(str.rstrip, [
            "   id              rows  source  ",
            "       errors      left    target",
            "=================================",
            "    1                 4  p1      ",
            "            0         4    p2    ",
            "---------------------------------",
            "    2                 0  p2      ",
            "---------------------------------",
            "",
        ])))


class OffsetTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)

    def test_reset(self):
        with self.bot.define('p1').append([1, 2, 3, 4]):
            self.bot.define('p2').skip()
        self.bot.main(argv=['offset', 'p1', 'p2', '-1'])
        self.assertEqual(self.output.getvalue(), '\n'.join(map(str.rstrip, [
            "   id              rows  source  ",
            "       errors      left    target",
            "=================================",
            "    1                 4  p1      ",
            "            0         1    p2    ",
            "---------------------------------",
            "    2                 0  p2      ",
            "---------------------------------",
            "",
        ])))


class CleanTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)

    def test_clean(self):
        self.bot.define('p1').append([1, 2, 3, 4])
        self.bot.main(argv=['clean', 'p1'])
        self.assertEqual(self.output.getvalue(), '\n'.join(map(str.rstrip, [
            "   id              rows  source  ",
            "       errors      left    target",
            "=================================",
            "    1                 0  p1      ",
            "---------------------------------",
            "",
        ])))


class CompactTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)

    def test_compact(self):
        self.bot.define('p1').append([1, 1, 2, 1, 1])
        self.bot.main(argv=['compact', 'p1'])
        self.assertEqual(self.output.getvalue(), '\n'.join(map(str.rstrip, [
            "   id              rows  source  ",
            "       errors      left    target",
            "=================================",
            "    1                 2  p1      ",
            "---------------------------------",
            "",
        ])))


class ShowTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)

    def test_show(self):
        self.bot.define('p1').append([(1, 'a'), (2, 'b')])
        self.bot.main(argv=['show', 'p1'])
        self.assertEqual(self.output.getvalue(), '\n'.join(map(str.rstrip, [
            "- key: 2  ",
            "  value: 'b'",
            "",
        ])))

    def test_show_key(self):
        self.bot.define('p1').append([(1, 'a'), (2, 'b')])
        self.bot.main(argv=['show', 'p1', '1'])
        self.assertEqual(self.output.getvalue(), '\n'.join(map(str.rstrip, [
            "- key: 1  ",
            "  value: 'a'",
            "",
        ])))


class TailTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)

    def test_tail(self):
        self.bot.define('p1').append([(1, 'a'), (2, 'b')])
        self.bot.main(argv=['tail', 'p1', '-t'])
        self.assertEqual(self.output.getvalue(), '\n'.join([
            "key   value ",
            "===========",
            "1     a     ",
            "2     b     ",
            "",
        ]))

    def test_include(self):
        self.bot.define('p1').append([(1, {'a': 1, 'b': 2}), (2, {'b': 3})])
        self.bot.main(argv=['tail', 'p1', '-t', '-i', 'a,b'])
        self.assertEqual(self.output.getvalue(), '\n'.join([
            " a     b ",
            "========",
            "1      2 ",
            "None   3 ",
            "",
        ]))


class ExportTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)

    def test_export_csv(self):
        self.bot.define('p1').append([(1, 'a'), (2, 'b')])
        temp = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
        self.bot.main(argv=['export', 'p1', temp.name])
        with open(temp.name) as f:
            self.assertEqual(f.read(), '\n'.join([
                "key,value",
                "1,a",
                "2,b",
                "",
            ]))
        os.unlink(temp.name)

    def test_include(self):
        self.bot.define('p1').append([(1, {'a': 1, 'b': 2}), (2, {'b': 3})])
        temp = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
        self.bot.main(argv=['export', 'p1', temp.name, '-i', 'a,b'])
        with open(temp.name) as f:
            self.assertEqual(f.read(), '\n'.join([
                "a,b",
                "1,2",
                ",3",
                "",
            ]))
        os.unlink(temp.name)

    def test_export_tsv(self):
        self.bot.define('p1').append([(1, 'a'), (2, 'b')])
        temp = tempfile.NamedTemporaryFile(suffix='.tsv', delete=False)
        self.bot.main(argv=['export', 'p1', temp.name])
        with open(temp.name) as f:
            self.assertEqual(f.read(), '\n'.join([
                "key\tvalue",
                "1\ta",
                "2\tb",
                "",
            ]))
        os.unlink(temp.name)

    def test_no_header_append(self):
        self.bot.define('p1').append([(1, 'a'), (2, 'b')])
        temp = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
        self.bot.main(argv=['export', 'p1', temp.name, '--no-header'])
        self.bot.main(argv=['export', 'p1', temp.name, '--no-header', '--append'])
        with open(temp.name) as f:
            self.assertEqual(f.read(), '\n'.join([
                "1,a",
                "2,b",
                "1,a",
                "2,b",
                "",
            ]))

        os.unlink(temp.name)


class ResolveTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)
        self.t1 = self.bot.define('p1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])
        self.t2 = self.bot.define('p2')

        rows = list(self.t1.data.rows())
        with self.t1:
            self.t2.errors.report(rows[0], 'Error 1')
            self.t2.errors.report(rows[2], 'Error 2')

    def test_resolve_all(self):
        self.bot.main(argv=['resolve', 'p1', 'p2'])
        self.assertEqual(self.output.getvalue(), '\n'.join(map(str.rstrip, [
            "   id              rows  source  ",
            "       errors      left    target",
            "=================================",
            "    1                 3  p1      ",
            "            0         3    p2    ",
            "---------------------------------",
            "    2                 0  p2      ",
            "---------------------------------",
            "",
        ])))

    def test_resolve_key(self):
        self.bot.main(argv=['resolve', 'p1', 'p2', '"3"'])
        self.assertEqual(self.output.getvalue(), '\n'.join(map(str.rstrip, [
            "   id              rows  source  ",
            "       errors      left    target",
            "=================================",
            "    1                 3  p1      ",
            "            1         3    p2    ",
            "---------------------------------",
            "    2                 0  p2      ",
            "---------------------------------",
            "",
        ])))


@tests.db.usedb()
class MigrateTests(object):

    def setUp(self):
        super().setUp()

        # Create tables, but do not apply any migrations
        self.db.meta.create_all(self.db.engine, checkfirst=True)

        self.output = io.StringIO()
        self.bot = Bot(self.db.engine, output=self.output, models=self.db.models)
        self.p1 = self.bot.define('p1')

        # Add a value, that should be migrated
        self.db.engine.execute(self.p1.table.insert().values(key='1', value=b'"a"'))

    @mock.patch('sys.exit', mock.Mock())
    @mock.patch('databot.db.migrations.Migrations.migrations', {migrations.ValueToMsgpack: set()})
    def test_error_when_migrations_not_applied(self):
        self.bot.main(argv=['status'])
        self.assertEqual(self.output.getvalue(), '\n'.join(map(str.rstrip, [
            "You need to run database migrations:                   ",
            "                                                       ",
            "    /home/sirex/.venvs/databot/bin/nosetests migrate   ",
            "                                                       ",
            "List of unapplied migrations:                          ",
            "                                                       ",
            "  - ValueToMsgpack                                     ",
            "                                                       ",
            "   id              rows  source                        ",
            "       errors      left    target                      ",
            "=================================                      ",
            "    1                 1  p1                            ",
            "---------------------------------                      ",
            "                                                       ",
        ])))

    @mock.patch('databot.db.migrations.Migrations.migrations', {migrations.ValueToMsgpack: set()})
    def test_migrate(self):
        self.bot.main(argv=['migrate'])
        result = '\n'.join(map(str.rstrip, [
            "- value to msgpack...   ",
            "  p1                    ",
        ]))
        self.assertIn(result, self.output.getvalue())


@tests.db.usedb()
class MigrateExternalDBTests(object):

    def setUp(self):
        super().setUp()

        self.output = io.StringIO()

        engine = sa.create_engine('sqlite:///:memory:')
        models = Models(sa.MetaData())

        # Create tables, but do not apply any migrations
        models.metadata.create_all(engine, checkfirst=True)

        self.bot1 = Bot(engine, output=self.output, models=models)
        self.bot1.define('p1')

    @mock.patch('sys.exit', mock.Mock())
    @mock.patch('databot.db.migrations.Migrations.migrations', {migrations.ValueToMsgpack: set()})
    def test_external_db_error_when_migrations_not_applied(self):
        bot2 = Bot(self.db.engine, output=self.output, models=self.db.models)
        bot2.define('p1', self.bot1.engine)
        bot2.define('p2')
        bot2.main(argv=['status'])
        self.assertEqual(self.output.getvalue(), '\n'.join(map(str.rstrip, [
            "External database 'sqlite:///:memory:' from 'p1' pipe has unapplied migrations.",
            "                                                                               ",
            "List of unapplied migrations:                                                  ",
            "                                                                               ",
            "  - ValueToMsgpack                                                             ",
            "                                                                               ",
            "   id              rows  source                                                ",
            "       errors      left    target                                              ",
            "=================================                                              ",
            "    1                 0  p1                                                    ",
            "---------------------------------                                              ",
            "    2                 0  p2                                                    ",
            "---------------------------------                                              ",
            "                                                                               ",
        ])))


class ErrorsTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = Bot('sqlite:///:memory:', output=self.output)
        self.t1 = self.bot.define('p1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])
        self.t2 = self.bot.define('p2')
        self.maxDiff = None

    @freezegun.freeze_time('2015-12-20 15:33:05')
    def test_resolve_all(self):
        traceback = [
            '  Traceback (most recent call last):                          ',
            '    File "databot/databot/pipes.py", line 498, in call        ',
            '      self.append(handler(row), bulk=pipe)                    ',
            '    File "databot/databot/pipes.py", line 296, in append      ',
            '      rows = keyvalueitems(key, value)                        ',
            '    File "databot/databot/pipes.py", line 34, in keyvalueitems',
            '      item = next(items)                                      ',
            '    File "databot/databot/testing.py", line 9, in __call__    ',
            '      raise ValueError(\'Error.\')                            ',
            '  ValueError: Error.                                          ',
        ]

        self.bot.main(argv=['-v0', 'run'])
        with self.t1:
            self.t2.errors.report(self.t1.last(), '\n'.join(map(str.rstrip, traceback)))

        self.bot.main(argv=['errors', 'p1', 'p2'])
        self.assertEqual(self.output.getvalue(), '\n'.join(map(str.rstrip, [
            "- key: '3'                    ",
            "  value: 'c'                  ",
        ] + ['  ' + line for line in traceback] + [
            "  Created: 2015-12-20 15:33:05",
            "  Updated: 2015-12-20 15:33:05",
            "  Retries: 0                  ",
            "                              ",
        ])))
