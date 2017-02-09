import os
import io
import json
import mock
import tempfile
import sqlalchemy as sa
import freezegun
import pytest
import textwrap

from databot import Bot, define, task, this
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
    bot.main(argv=['select', 'p1', '-q', 'div:text'])
    assert bot.output.output.getvalue() == (
        '- key: \'value\'\n'
        '  value: None\n'
    )


def test_select_table(bot):
    bot.define('p1').append('http://example.com/', {'content': b'<div>value</div>'})
    bot.main(argv=['select', 'p1', '-q', 'div:text', '-t'])
    assert bot.output.output.getvalue() == (
        ' key    value \n'
        '=============\n'
        'value   None  \n'
    )


def test_select_export(bot, tmpdir):
    bot.define('p1').append([
        ('http://example.com/', {'content': b'<div id="1">a</div>'}),
        ('http://example.com/', {'content': b'<div id="2">b</div>'}),
        ('http://example.com/', {'content': b'<div id="3">c</div>'}),
    ])

    bot.main(argv=['select', 'p1', '-x', str(tmpdir / 'export.csv'), '-eq', '("div@id", "div:text")'])
    assert bot.output.output.getvalue() == ''
    assert tmpdir.join('export.csv').read() == (
        'key,value\n'
        '1,a\n'
        '2,b\n'
        '3,c\n'
    )

    bot.main(argv=['select', 'p1', '-x', str(tmpdir / 'export.jsonl'), '-eq', '("div@id", "div:text")'])
    assert bot.output.output.getvalue() == ''
    assert list(map(json.loads, tmpdir.join('export.jsonl').read().splitlines())) == [
        {'key': '1', 'value': 'a'},
        {'key': '2', 'value': 'b'},
        {'key': '3', 'value': 'c'},
    ]


def test_select_export_non_verbose(bot, tmpdir):
    bot.define('p1').append([
        ('http://example.com/', {'content': b'<div id="1">a</div>'}),
        ('http://example.com/', {'content': b'<div id="2">b</div>'}),
        ('http://example.com/', {'content': b'<div id="3">c</div>'}),
    ])

    bot.main(argv=['-v', '0', 'select', 'p1', '-x', str(tmpdir / 'export.csv'), '-eq', '("div@id", "div:text")'])
    assert bot.output.output.getvalue() == ''
    assert tmpdir.join('export.csv').read() == (
        'key,value\n'
        '1,a\n'
        '2,b\n'
        '3,c\n'
    )


def test_select_not_found(bot):
    bot.define('p1')
    bot.main(argv=['select', 'p1', '-k', 'missing', '-q', 'div:text'])
    assert bot.output.output.getvalue() == (
        'Not found.\n'
    )


def test_select_raw(bot):
    p1 = bot.define('p1').append([1, 2, 3])
    assert bot.commands.select(p1, raw=True, query=this.key) == [{'key': 3, 'value': None}]


def test_select_raw_empty(bot):
    p1 = bot.define('p1')
    assert bot.commands.select(p1, raw=True, query=this.key) == []


def test_select_errors(bot):
    p1 = bot.define('p1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])
    p2 = bot.define('p2')
    p2(p1).errors.report(p1.get('2'), 'Error.')
    assert bot.commands.select(p1, p2, raw=True, query=this.key, errors=True) == [{'key': '2', 'value': None}]


def test_select_errors_export(bot, tmpdir):
    p1 = bot.define('p1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])
    p2 = bot.define('p2')
    p2(p1).errors.report(p1.get('1'), 'Error.')
    p2(p1).errors.report(p1.get('3'), 'Error.')
    bot.commands.select(p1, p2, raw=True, query=this.value, errors=True, export=str(tmpdir.join('export.csv')))
    assert tmpdir.join('export.csv').read() == (
        'key,value\n'
        'a,\n'
        'c,\n'
    )


def test_select_check(bot):
    p1 = bot.define('p1').append('http://example.com/', {
        'content': b'<div><span class="check">1</span></div>',
    })
    assert bot.commands.select(p1, query=['span.missing:text'], check=False) is None
    assert bot.commands.select(p1, query=['span.missing:text'], check='span.check') is None

    with pytest.raises(ValueError) as e:
        bot.commands.select(p1, query=['span.missing:text'])
    assert str(e.value) == 'Select query did not returned any results.'


def test_download(bot, requests):
    requests.get('http://example.com/', text='<div>It works!</div>', headers={'Content-Type': 'text/html'})
    bot.main(argv=['download', 'http://example.com/'])
    assert bot.output.output.getvalue() == '\n'.join([
        "- key: 'http://example.com/'",
        "  history:",
        "    []",
        "  headers:",
        "    {'Content-Type': 'text/html'}",
        "  cookies:",
        "    {}",
        "  status_code: 200",
        "  encoding: 'utf-8'",
        "  content:",
        "    <div>It works!</div>",
        "  url: 'http://example.com/'",
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
    p1 = bot.define('p1').append([1, 2, 3, 4])
    p2 = bot.define('p2')
    p2(p1).skip()
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


def test_clean_key(bot):
    bot.define('p1').append([1, 2, 2, 3])
    bot.main(argv=['clean', 'p1', '2', '-e'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "   id              rows  source  ",
        "       errors      left    target",
        "=================================",
        "    1                 3  p1      ",
        "---------------------------------",
        "",
    ]))


def test_clean_key_not_found(bot):
    bot.define('p1').append([1, 2, 2, 3])
    bot.main(argv=['clean', 'p1', '42'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "Item with key='42' not found.",
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
    bot.main(argv=['show', 'p1', '-k', '1'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "- key: 1  ",
        "  value: 'a'",
        "",
    ]))


def test_show_target_errors(bot):
    p1 = bot.define('p1').append([('1', 'a'), ('2', 'b'), ('3', 'c')])
    p2 = bot.define('p2')
    p2(p1).errors.report(p1.get('2'), 'Error.')
    bot.main(argv=['show', 'p1', 'p2', '-e'])
    assert bot.output.output.getvalue() == '\n'.join(map(str.rstrip, [
        "- key: '2'  ",
        "  value: 'b'",
        "",
    ]))


def test_head(bot):
    bot.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')])
    bot.main(argv=['head', 'p1', '-t', '-n', '2'])
    assert bot.output.output.getvalue() == '\n'.join([
        "key   value ",
        "===========",
        "1     a     ",
        "2     b     ",
        "",
    ])


def test_tail(bot):
    bot.define('p1').append([(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')])
    bot.main(argv=['tail', 'p1', '-t', '-n', '2'])
    assert bot.output.output.getvalue() == '\n'.join([
        "key   value ",
        "===========",
        "4     d     ",
        "5     e     ",
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

    rows = list(t1.rows())
    t2(t1).errors.report(rows[0], 'Error 1')
    t2(t1).errors.report(rows[2], 'Error 2')

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

    rows = list(t1.rows())
    t2(t1).errors.report(rows[0], 'Error 1')
    t2(t1).errors.report(rows[2], 'Error 2')

    bot.main(argv=['resolve', 'p1', 'p2', '3'])
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
        "    databot sqlite:///:memory: migrate                 ",
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
        "You need to run database migrations:                                           ",
        "                                                                               ",
        "    databot sqlite:///:memory: migrate                                         ",
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
    with freezegun.freeze_time('2015-12-20 15:33:05'):
        t2(t1).errors.report(t1.last(), '\n'.join(map(str.rstrip, traceback)))

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


def test_compress(bot):
    bot.define('p1').append([(1, 'a'), (2, 'b')])
    bot.main(argv=['compress', 'p1'])
    assert bot.output.output.getvalue().startswith('\rcompress p1:   0%|          | 0/2')


def test_decompress(bot):
    bot.define('p1').append([(1, 'a'), (2, 'b')]).compress()
    bot.main(argv=['decompress', 'p1'])
    assert bot.output.output.getvalue().startswith('\rdecompress p1:   0%|          | 0/2')


def test_run(bot):
    pipeline = {
        'pipes': [
            define('p1'),
            define('p2'),
        ],
        'tasks': [
            task('p1').once().append([(1, 'a'), (2, 'b')]),
            task('p1', 'p2').select(this.key, this.value.upper()),
        ]
    }

    bot.main(pipeline, argv=['run', '-f'])
    assert bot.output.output.getvalue() == textwrap.dedent('''\
    Validating pipeline.

    Run pipeline (limit=1).

    Run pipeline (limit=0).
    ''')
    assert list(bot.pipe('p2').items()) == [(1, 'A'), (2, 'B')]


def test_run_error_limit(bot, capsys):

    def handler(row):
        if row.key > 1:
            raise ValueError('Error.')
        else:
            yield row.key, row.value.upper()

    pipeline = {
        'pipes': [
            define('p1'),
            define('p2'),
        ],
        'tasks': [
            task('p1').append([(1, 'a'), (2, 'b')]),
            task('p1', 'p2').call(handler),
        ]
    }

    with pytest.raises(ValueError):
        bot.main(pipeline, argv=['run', '-f'])

    assert bot.output.output.getvalue() == textwrap.dedent('''\
    Validating pipeline.

    Run pipeline (limit=1).

    Run pipeline (limit=0).
    - key: 2
      value: 'b'
    ''')
    assert list(bot.pipe('p2').items()) == [(1, 'A')]
    assert capsys.readouterr()[0] == 'Interrupting bot because error limit of 0 was reached.\n'
    assert task('p1', 'p2').errors.count()._eval(bot) == 0


def test_run_error_limit_n(bot, capsys):

    def handler(row):
        if row.key > 1:
            raise ValueError('Error.')
        else:
            yield row.key, row.value.upper()

    pipeline = {
        'pipes': [
            define('p1'),
            define('p2'),
        ],
        'tasks': [
            task('p1').once().append([(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')]),
            task('p1', 'p2').call(handler),
        ]
    }

    with pytest.raises(ValueError):
        bot.main(pipeline, argv=['run', '-f', '2', '-l', '0'])

    assert bot.output.output.getvalue() == textwrap.dedent('''\
    Validating pipeline.

    Run pipeline (limit=0).
    - key: 3
      value: 'c'
    ''')
    assert list(bot.pipe('p2').items()) == [(1, 'A')]
    assert capsys.readouterr()[0] == 'Interrupting bot because error limit of 2 was reached.\n'
    assert task('p1', 'p2').errors.count()._eval(bot) == 2
