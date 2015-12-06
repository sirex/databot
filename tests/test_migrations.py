import io
import json
import msgpack

from databot.db import migrations
from databot.printing import Printer

import tests.db


def create_pipe(engine, models, name, data):
    result = engine.execute(models.pipes.insert().values(bot='test', pipe=name))
    pipe = models.get_data_table('t%d' % tuple(result.inserted_primary_key))
    pipe.create(engine, checkfirst=True)
    for key, value in data:
        value = msgpack.dumps(value, use_bin_type=True)
        engine.execute(pipe.insert().values(key=str(key), value=value))
    return pipe


class MigrationA(migrations.Migration):

    name = 'a'
    data_tables = True


class MigrationB(migrations.Migration):

    name = 'b'
    data_tables = True

    def migrate_data_item(self, row):
        return dict(value=row['value'])


class Migrations(migrations.Migrations):

    migrations = {
        MigrationA: set(),
        MigrationB: {MigrationA},
    }


@tests.db.usedb()
class MigrationsTests(object):

    def setUp(self):
        super().setUp()
        self.output = io.StringIO()
        self.migrations = Migrations(self.db.models, self.db.engine, Printer(self.db.models, self.output), verbosity=2)

    def test_applied(self):
        self.db.models.migrations.create(self.db.engine)
        self.migrations.mark_applied(MigrationA.name)
        self.assertEqual(self.migrations.applied(), {MigrationA})

    def test_applied_no_migrations_table(self):
        self.assertEqual(self.migrations.applied(), set())

    def test_unapplied(self):
        self.db.models.migrations.create(self.db.engine)
        self.migrations.mark_applied(MigrationA.name)
        self.assertEqual(self.migrations.unapplied(), {MigrationB})

    def test_initialize(self):
        self.migrations.initialize()
        self.assertEqual(self.migrations.applied(), {MigrationA, MigrationB})

    def test_migrate(self):
        self.db.meta.create_all(self.db.engine, checkfirst=True)

        create_pipe(self.db.engine, self.db.models, 'p1', [(1, 'a'), (2, 'b')])
        create_pipe(self.db.engine, self.db.models, 'p2', [(1, 'a'), (2, 'b')])

        self.assertEqual(self.migrations.applied(), set())

        self.migrations.migrate()

        self.assertEqual(self.migrations.applied(), {MigrationA, MigrationB})
        self.assertEqual(self.output.getvalue(), (
            '- a...\n'
            '  p1\n'
            '  p2\n'
            '- b...\n'
            '  p1\n'
            '  p2\n'
            'done.\n'
        ))

    def test_migrate_applied(self):
        table = self.db.models.get_data_table('t1')
        table.create(self.db.engine, checkfirst=True)

        self.migrations.initialize()

        self.db.engine.execute(table.insert().values(key='1', value=b'a'))
        self.db.engine.execute(self.db.models.pipes.insert().values(bot='x', pipe='p1'))

        self.migrations.migrate()

        self.assertEqual(self.output.getvalue(), (
            '- a... (already applied)\n'
            '- b... (already applied)\n'
            'done.\n'
        ))

    def test_migrate_without_migrations_table(self):
        self.db.meta.create_all(self.db.engine, checkfirst=True)
        self.db.models.migrations.drop(self.db.engine)
        self.db.models.get_data_table('t1').create(self.db.engine, checkfirst=True)

        self.db.engine.execute(self.db.models.pipes.insert().values(bot='x', pipe='p1'))

        self.migrations.migrations = {
            migrations.MigrationsTable: set(),
            MigrationA: {migrations.MigrationsTable},
            MigrationB: {MigrationA},
        }

        self.migrations.migrate()

        self.assertEqual(self.output.getvalue(), (
            '- migrations table...\n'
            '  creating migrations table...\n'
            '- a...\n'
            '  p1\n'
            '- b...\n'
            '  p1\n'
            'done.\n'
        ))

    def test_migrate_with_progress_bar(self):
        self.db.meta.create_all(self.db.engine, checkfirst=True)

        table = self.db.models.get_data_table('t1')
        table.create(self.db.engine, checkfirst=True)

        self.db.engine.execute(table.insert().values(key='1', value=b'a'))
        self.db.engine.execute(self.db.models.pipes.insert().values(bot='x', pipe='p1'))

        self.migrations.verbosity = 1
        self.migrations.migrate()

        output = (
            '- a...\n'
            '  p1\n'
            '- b...\n'
            '  p1\n'
        )
        self.assertIn(output, self.output.getvalue())

    def test_has_initial_state(self):
        self.assertTrue(self.migrations.has_initial_state())
        self.db.models.migrations.create(self.db.engine)
        self.assertFalse(self.migrations.has_initial_state())


@tests.db.usedb()
class ValueToMsgpackTests(object):

    def setUp(self):
        super().setUp()
        self.output = io.StringIO()
        printer = Printer(self.db.models, self.output)
        self.migration = migrations.ValueToMsgpack(self.db.models, self.db.engine, printer, verbosity=2)
        self.table = self.db.models.get_data_table('t1')
        self.table.create(self.db.engine)

    def test_download_handler_value(self):
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

        self.db.engine.execute(self.table.insert(), key='http://example.com/', value=json.dumps(before).encode())
        self.migration.migrate_data(self.table, 'p1')

        (key, value), = [(row['key'], row['value']) for row in self.db.engine.execute(self.table.select())]
        self.assertEqual(key, 'http://example.com/')
        self.assertEqual(msgpack.loads(value, encoding='utf-8'), after)

    def test_other_value(self):
        before = {
            'a': 1,
        }

        after = {
            'a': 1,
        }

        self.db.engine.execute(self.table.insert(), key='http://example.com/', value=json.dumps(before).encode())
        self.migration.migrate_data(self.table, 'p1')

        (key, value), = [(row['key'], row['value']) for row in self.db.engine.execute(self.table.select())]
        self.assertEqual(key, 'http://example.com/')
        self.assertEqual(msgpack.loads(value, encoding='utf-8'), after)


@tests.db.usedb()
class KeyToSha1Tests(object):

    def setUp(self):
        super().setUp()
        self.output = io.StringIO()
        printer = Printer(self.db.models, self.output)
        self.migration = migrations.KeyToSha1(self.db.models, self.db.engine, printer, verbosity=2)
        self.table = self.db.models.get_data_table('t1')
        self.table.create(self.db.engine)

    def test_other_value(self):
        before = {
            'key': '42',
            'value': {'a': 1},
        }

        after = {
            'key': 'a927369e6d62033fcd22c37b52b55451cd5e548a',
            'value': ['42', {'a': 1}],
        }

        self.db.engine.execute(
            self.table.insert(),
            key=before['key'],
            value=msgpack.dumps(before['value'], use_bin_type=True)
        )
        self.migration.migrate_data(self.table, 'p1')

        (key, value), = [(row['key'], row['value']) for row in self.db.engine.execute(self.table.select())]
        self.assertEqual(key, after['key'])
        self.assertEqual(msgpack.loads(value, encoding='utf-8'), after['value'])
