import os
import io
import sys
import mock
import tempfile
import sqlalchemy as sa
import freezegun
import pytest

from databot import Bot
from databot.db import migrations
from databot.db.models import Models
from databot.commands import Command
from databot.exceptions import PipeNameError


def test_pipe_name_error(bot):
    command = Command(bot)
    with pytest.raises(PipeNameError):
        command.pipe('42')
    with pytest.raises(PipeNameError):
        command.pipe('zz')


def test_status(bot):
    bot.define('p1')
    bot.define('p2')
    bot.main(argv=['status'])
    assert bot.output.output.getvalue() == (
        '   id              rows  source\n'
        '       errors      left    target\n'
        '=================================\n'
        '    1                 0  p1\n'
        '---------------------------------\n'
        '    2                 0  p2\n'
        '---------------------------------\n'
    )


def test_select(bot):
    bot.define('p1').append('http://example.com/', {'content': b'<div>value</div>'})
    bot.main(argv=['select', 'p1', 'div:text'])
    assert bot.output.output.getvalue() == (
        '- key: \'value\'\n'
        '  value: None\n'
    )


def test_select_not_found(bot):
    bot.define('p1')
    bot.main(argv=['select', 'p1', '-k', 'missing', 'div:text'])
    assert bot.output.output.getvalue() == (
        'Not found.\n'
    )


def test_download(mocker, bot):
    resp = mock.Mock()
    resp.status_code = 200
    resp.encoding = 'utf-8'
    resp.headers = {'Content-Type': 'text/html'}
    resp.content = b'<div>It works!</div>'
    resp.cookies = {}
    mocker.patch('requests.get').return_value = resp

    bot.main(argv=['download', 'http://example.com/'])
    assert bot.output.output.getvalue() == '\n'.join([
        "- key: 'http://example.com/'",
        "  headers:",
        "    {'Content-Type': 'text/html'}",
        "  cookies:",
        "    {}",
        "  status_code: 200",
        "  encoding: 'utf-8'",
        "  content:",
        "    <div>It works!</div>",
        "",
    ])


def test_skip(bot):
    bot.define('p1').append([1, 2, 3, 4])
    bot.define('p2')
    bot.main(argv=['skip', 'p1', 'p2'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "   id              rows  source  ",
        "       errors      left    target",
        "=================================",
        "    1                 4  p1      ",
        "            0         0    p2    ",
        "---------------------------------",
        "    2                 0  p2      ",
        "---------------------------------",
        "",
    ]))


def test_reset(bot):
    bot.define('p1').append([1, 2, 3, 4])
    bot.define('p2')
    bot.main(argv=['reset', 'p1', 'p2'])
    bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "   id              rows  source  ",
        "       errors      left    target",
        "=================================",
        "    1                 4  p1      ",
        "            0         4    p2    ",
        "---------------------------------",
        "    2                 0  p2      ",
        "---------------------------------",
        "",
    ]))


def test_offset(bot):
    with bot.define('p1').append([1, 2, 3, 4]):
        bot.define('p2').skip()
    bot.main(argv=['offset', 'p1', 'p2', '-1'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "   id              rows  source  ",
        "       errors      left    target",
        "=================================",
        "    1                 4  p1      ",
        "            0         1    p2    ",
        "---------------------------------",
        "    2                 0  p2      ",
        "---------------------------------",
        "",
    ]))


def test_clean(bot):
    bot.define('p1').append([1, 2, 3, 4])
    bot.main(argv=['clean', 'p1'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "   id              rows  source  ",
        "       errors      left    target",
        "=================================",
        "    1                 0  p1      ",
        "---------------------------------",
        "",
    ]))


def test_compact(bot):
    bot.define('p1').append([1, 1, 2, 1, 1])
    bot.main(argv=['compact', 'p1'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "   id              rows  source  ",
        "       errors      left    target",
        "=================================",
        "    1                 2  p1      ",
        "---------------------------------",
        "",
    ]))


def test_show(bot):
    bot.define('p1').append([(1, 'a'), (2, 'b')])
    bot.main(argv=['show', 'p1'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "- key: 2  ",
        "  value: 'b'",
        "",
    ]))


def test_show_key(bot):
    bot.define('p1').append([(1, 'a'), (2, 'b')])
    bot.main(argv=['show', 'p1', '1'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "- key: 1  ",
        "  value: 'a'",
        "",
    ]))


def test_tail(bot):
    bot.define('p1').append([(1, 'a'), (2, 'b')])
    bot.main(argv=['tail', 'p1', '-t'])
    assert bot.output.output.getvalue() == '\n'.join([
        "key   value ",
        "===========",
        "1     a     ",
        "2     b     ",
        "",
    ])


def test_tail_include(bot):
    bot.define('p1').append([(1, {'a': 1, 'b': 2}), (2, {'b': 3})])
    bot.main(argv=['tail', 'p1', '-t', '-i', 'a,b'])
    assert bot.output.output.getvalue() == '\n'.join([
        " a     b ",
        "========",
        "1      2 ",
        "None   3 ",
        "",
    ])


def test_export_csv(bot):
    bot.define('p1').append([(1, 'a'), (2, 'b')])
    temp = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
    bot.main(argv=['export', 'p1', temp.name])
    with open(temp.name) as f:
        assert f.read() == '\n'.join([
            "key,value",
            "1,a",
            "2,b",
            "",
        ])
    os.unlink(temp.name)


def test_export_include(bot):
    bot.define('p1').append([(1, {'a': 1, 'b': 2}), (2, {'b': 3})])
    temp = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
    bot.main(argv=['export', 'p1', temp.name, '-i', 'a,b'])
    with open(temp.name) as f:
        assert f.read() == '\n'.join([
            "a,b",
            "1,2",
            ",3",
            "",
        ])
    os.unlink(temp.name)


def test_export_tsv(bot):
    bot.define('p1').append([(1, 'a'), (2, 'b')])
    temp = tempfile.NamedTemporaryFile(suffix='.tsv', delete=False)
    bot.main(argv=['export', 'p1', temp.name])
    with open(temp.name) as f:
        assert f.read() == '\n'.join([
            "key\tvalue",
            "1\ta",
            "2\tb",
            "",
        ])
    os.unlink(temp.name)


def test_export_no_header_append(bot):
    bot.define('p1').append([(1, 'a'), (2, 'b')])
    temp = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
    bot.main(argv=['export', 'p1', temp.name, '--no-header'])
    bot.main(argv=['export', 'p1', temp.name, '--no-header', '--append'])
    with open(temp.name) as f:
        assert f.read() == '\n'.join([
            "1,a",
            "2,b",
            "1,a",
            "2,b",
            "",
        ])

    os.unlink(temp.name)


def test_resolve_all(bot):
    t1 = bot.define('p1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])
    t2 = bot.define('p2')

    rows = list(t1.data.rows())
    with t1:
        t2.errors.report(rows[0], 'Error 1')
        t2.errors.report(rows[2], 'Error 2')

    bot.main(argv=['resolve', 'p1', 'p2'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "   id              rows  source  ",
        "       errors      left    target",
        "=================================",
        "    1                 3  p1      ",
        "            0         3    p2    ",
        "---------------------------------",
        "    2                 0  p2      ",
        "---------------------------------",
        "",
    ]))


def test_resolve_key(bot):
    t1 = bot.define('p1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])
    t2 = bot.define('p2')

    rows = list(t1.data.rows())
    with t1:
        t2.errors.report(rows[0], 'Error 1')
        t2.errors.report(rows[2], 'Error 2')

    bot.main(argv=['resolve', 'p1', 'p2', '"3"'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "   id              rows  source  ",
        "       errors      left    target",
        "=================================",
        "    1                 3  p1      ",
        "            1         3    p2    ",
        "---------------------------------",
        "    2                 0  p2      ",
        "---------------------------------",
        "",
    ]))


def test_error_when_migrations_not_applied(mocker, db):
    mocker.patch('sys.exit', mock.Mock())
    mocker.patch('databot.db.migrations.Migrations.migrations', {migrations.ValueToMsgpack: set()})

    # Create tables, but do not apply any migrations
    db.meta.create_all(db.engine, checkfirst=True)
    bot = db.Bot()
    p1 = bot.define('p1')

    # Add a value, that should be migrated
    db.engine.execute(p1.table.insert().values(key='1', value=b'"a"'))

    bot.main(argv=['status'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "You need to run database migrations:                   ",
        "                                                       ",
        "    %s migrate   " % sys.argv[0],
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
    ]))


def test_migrate(mocker, db):
    mocker.patch('databot.db.migrations.Migrations.migrations', {migrations.ValueToMsgpack: set()})

    # Create tables, but do not apply any migrations
    db.meta.create_all(db.engine, checkfirst=True)
    bot = db.Bot()
    p1 = bot.define('p1')

    # Add a value, that should be migrated
    db.engine.execute(p1.table.insert().values(key='1', value=b'"a"'))

    bot.main(argv=['migrate'])
    result = '\n'.join(map(str.rstrip, [
        "- value to msgpack...   ",
        "  p1                    ",
    ]))
    assert result in bot.output.output.getvalue()


def test_external_db_error_when_migrations_not_applied(mocker, db):
    mocker.patch('sys.exit')
    mocker.patch('databot.db.migrations.Migrations.migrations', {migrations.ValueToMsgpack: set()})

    # Create tables, but do not apply any migrations
    engine = sa.create_engine('sqlite:///:memory:')
    models = Models(sa.MetaData())
    models.metadata.create_all(engine, checkfirst=True)
    bot1 = Bot(engine, output=io.StringIO(), models=models)
    bot1.define('p1')

    bot2 = db.Bot()
    bot2.define('p1', bot1.engine)
    bot2.define('p2')
    bot2.main(argv=['status'])
    assert bot2.output.output.getvalue() == '\n'.join(map(str.rstrip, [
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
    ]))


def test_errors(bot):
    t1 = bot.define('p1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])
    t2 = bot.define('p2')

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

    bot.main(argv=['-v0', 'run'])
    with t1:
        with freezegun.freeze_time('2015-12-20 15:33:05'):
            t2.errors.report(t1.last(), '\n'.join(map(str.rstrip, traceback)))

    bot.main(argv=['errors', 'p1', 'p2'])
    bot.output.output.getvalue(), '\n'.join(map(str.rstrip, [
        "- key: '3'                    ",
        "  value: 'c'                  ",
    ] + ['  ' + line for line in traceback] + [
        "  Created: 2015-12-20 15:33:05",
        "  Updated: 2015-12-20 15:33:05",
        "  Retries: 0                  ",
        "                              ",
    ]))


def test_rename(bot):
    bot.define('p1')
    bot.define('p2')
    bot.main(argv=['rename', 'p1', 'pp'])

    bot = Bot('sqlite:///:memory:', output=io.StringIO())
    bot.define('pp')
    bot.define('p2')
    bot.main(argv=['status'])

    assert bot.output.output.getvalue() == (
        '   id              rows  source\n'
        '       errors      left    target\n'
        '=================================\n'
        '    1                 0  pp\n'
        '---------------------------------\n'
        '    2                 0  p2\n'
        '---------------------------------\n'
    )
