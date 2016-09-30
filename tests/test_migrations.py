import io
import json
import msgpack
import pytest

import databot.db
from databot.printing import Printer


def create_pipe(engine, models, name, data):
    result = engine.execute(models.pipes.insert().values(bot='test', pipe=name))
    pipe = models.get_data_table('t%d' % tuple(result.inserted_primary_key))
    pipe.create(engine, checkfirst=True)
    for key, value in data:
        value = msgpack.dumps(value, use_bin_type=True)
        engine.execute(pipe.insert().values(key=str(key), value=value))
    return pipe


class MigrationA(databot.db.migrations.Migration):

    name = 'a'
    data_tables = True


class MigrationB(databot.db.migrations.Migration):

    name = 'b'
    data_tables = True

    def migrate_data_item(self, row):
        return dict(value=row['value'])


class Migrations(databot.db.migrations.Migrations):

    migrations = {
        MigrationA: set(),
        MigrationB: {MigrationA},
    }


@pytest.fixture
def migrations(db):
    output = io.StringIO()
    return Migrations(db.models, db.engine, Printer(db.models, output), verbosity=2)


def test_applied(db, migrations):
    db.models.migrations.create(db.engine)
    migrations.mark_applied(MigrationA.name)
    assert migrations.applied() == {MigrationA}


def test_applied_no_migrations_table(migrations):
    assert migrations.applied() == set()


def test_unapplied(db, migrations):
    db.models.migrations.create(db.engine)
    migrations.mark_applied(MigrationA.name)
    assert migrations.unapplied() == {MigrationB}


def test_initialize(migrations):
    migrations.initialize()
    assert migrations.applied() == {MigrationA, MigrationB}


def test_migrate(db, migrations):
    db.meta.create_all(db.engine, checkfirst=True)

    create_pipe(db.engine, db.models, 'p1', [(1, 'a'), (2, 'b')])
    create_pipe(db.engine, db.models, 'p2', [(1, 'a'), (2, 'b')])

    assert migrations.applied() == set()

    migrations.migrate()

    assert migrations.applied() == {MigrationA, MigrationB}
    assert migrations.output.output.getvalue() == (
        '- a...\n'
        '  p1\n'
        '  p2\n'
        '- b...\n'
        '  p1\n'
        '  p2\n'
        'done.\n'
    )


def test_migrate_applied(db, migrations):
    table = db.models.get_data_table('t1')
    table.create(db.engine, checkfirst=True)

    migrations.initialize()

    db.engine.execute(table.insert().values(key='1', value=b'a'))
    db.engine.execute(db.models.pipes.insert().values(bot='x', pipe='p1'))

    migrations.migrate()

    assert migrations.output.output.getvalue() == (
        '- a... (already applied)\n'
        '- b... (already applied)\n'
        'done.\n'
    )


def test_migrate_without_migrations_table(db, migrations):
    db.meta.create_all(db.engine, checkfirst=True)
    db.models.migrations.drop(db.engine)
    db.models.get_data_table('t1').create(db.engine, checkfirst=True)

    db.engine.execute(db.models.pipes.insert().values(bot='x', pipe='p1'))

    migrations.migrations = {
        databot.db.migrations.MigrationsTable: set(),
        MigrationA: {databot.db.migrations.MigrationsTable},
        MigrationB: {MigrationA},
    }

    migrations.migrate()

    assert migrations.output.output.getvalue() == (
        '- migrations table...\n'
        '  creating migrations table...\n'
        '- a...\n'
        '  p1\n'
        '- b...\n'
        '  p1\n'
        'done.\n'
    )


def test_migrate_with_progress_bar(db, migrations):
    db.meta.create_all(db.engine, checkfirst=True)

    table = db.models.get_data_table('t1')
    table.create(db.engine, checkfirst=True)

    db.engine.execute(table.insert().values(key='1', value=b'a'))
    db.engine.execute(db.models.pipes.insert().values(bot='x', pipe='p1'))

    migrations.verbosity = 1
    migrations.migrate()

    output = (
        '- a...\n'
        '  p1\n'
        '- b...\n'
        '  p1\n'
    )
    assert output in migrations.output.output.getvalue()


def test_has_initial_state(db, migrations):
    assert migrations.has_initial_state() is True
    db.models.migrations.create(db.engine)
    assert migrations.has_initial_state() is False


@pytest.fixture
def Migration(db):
    def factory(MigrationClass):
        output = io.StringIO()
        printer = Printer(db.models, output)
        migration = MigrationClass(db.models, db.engine, printer, verbosity=2)
        table = db.models.get_data_table('t1')
        table.create(db.engine)
        return migration, table
    return factory


def test_value_to_msgpack_download_handler_value(db, Migration):
    migration, table = Migration(databot.db.migrations.ValueToMsgpack)

    before = {
        'headers': {},
        'cookies': {},
        'status_code': 200,
        'encoding': 'utf-8',
        'text': '<html></html>',
    }

    after = {
        'headers': {},
        'cookies': {},
        'status_code': 200,
        'encoding': 'utf-8',
        'content': b'<html></html>',
    }

    db.engine.execute(table.insert(), key='http://example.com/', value=json.dumps(before).encode())
    migration.migrate_data(table, 'p1')

    (key, value), = [(row['key'], row['value']) for row in db.engine.execute(table.select())]
    assert key == 'http://example.com/'
    assert msgpack.loads(value, encoding='utf-8') == after


def test_value_to_msgpack_other_value(db, Migration):
    migration, table = Migration(databot.db.migrations.ValueToMsgpack)

    before = {
        'a': 1,
    }

    after = {
        'a': 1,
    }

    db.engine.execute(table.insert(), key='http://example.com/', value=json.dumps(before).encode())
    migration.migrate_data(table, 'p1')

    (key, value), = [(row['key'], row['value']) for row in db.engine.execute(table.select())]
    assert key == 'http://example.com/'
    assert msgpack.loads(value, encoding='utf-8') == after


def test_key_to_sha1_other_value(db, Migration):
    migration, table = Migration(databot.db.migrations.KeyToSha1)

    before = {
        'key': '42',
        'value': {'a': 1},
    }

    after = {
        'key': 'a927369e6d62033fcd22c37b52b55451cd5e548a',
        'value': ['42', {'a': 1}],
    }

    db.engine.execute(
        table.insert(),
        key=before['key'],
        value=msgpack.dumps(before['value'], use_bin_type=True)
    )
    migration.migrate_data(table, 'p1')

    (key, value), = [(row['key'], row['value']) for row in db.engine.execute(table.select())]
    assert key == after['key']
    assert msgpack.loads(value, encoding='utf-8') == after['value']


def test_text_to_content_migration(db, Migration):
    migration, table = Migration(databot.db.migrations.TextToContent)

    before = [
        'http://example.com/',
        {
            'headers': {},
            'cookies': {},
            'status_code': 200,
            'encoding': 'utf-8',
            'text': b'<html></html>',
        }
    ]

    after = [
        'http://example.com/',
        {
            'headers': {},
            'cookies': {},
            'status_code': 200,
            'encoding': 'utf-8',
            'content': b'<html></html>',
        }
    ]

    db.engine.execute(
        table.insert(), value=msgpack.dumps(before, use_bin_type=True)
    )
    migration.migrate_data(table, 'p1')

    value, = [row['value'] for row in db.engine.execute(table.select())]
    assert msgpack.loads(value, encoding='utf-8') == after
