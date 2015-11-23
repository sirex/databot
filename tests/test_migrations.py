import io
import json
import msgpack

from databot.db import models
from databot.db import migrations
from databot.printing import Printer

import tests.db


def create_pipe(meta, engine, name, data):
    result = engine.execute(models.pipes.insert().values(bot='test', pipe=name))
    pipe = models.get_data_table('t%d' % tuple(result.inserted_primary_key), meta)
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
        self.migrations = Migrations(self.db.meta, self.db.engine, Printer(self.output), verbosity=2)

    def test_applied(self):
        models.migrations.create(self.db.engine)
        self.migrations.mark_applied(MigrationA.name)
        self.assertEqual(self.migrations.applied(), {MigrationA})

    def test_applied_no_migrations_table(self):
        self.assertEqual(self.migrations.applied(), set())

    def test_unapplied(self):
        models.migrations.create(self.db.engine)
        self.migrations.mark_applied(MigrationA.name)
        self.assertEqual(self.migrations.unapplied(), {MigrationB})

    def test_initialize(self):
        self.migrations.metadata = models.metadata
        self.migrations.initialize()
        self.assertEqual(self.migrations.applied(), {MigrationA, MigrationB})

    def test_migrate(self):
        models.metadata.create_all(self.db.engine, checkfirst=True)

        create_pipe(self.db.meta, self.db.engine, 'p1', [(1, 'a'), (2, 'b')])
        create_pipe(self.db.meta, self.db.engine, 'p2', [(1, 'a'), (2, 'b')])

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
        table = models.get_data_table('t1', self.db.meta)
        table.create(self.db.engine, checkfirst=True)

        self.migrations.metadata = models.metadata
        self.migrations.initialize()

        self.db.engine.execute(table.insert().values(key='1', value=b'a'))
        self.db.engine.execute(models.pipes.insert().values(bot='x', pipe='p1'))

        self.migrations.migrate()

        self.assertEqual(self.output.getvalue(), (
            '- a... (already applied)\n'
            '- b... (already applied)\n'
            'done.\n'
        ))

    def test_migrate_without_migrations_table(self):
        models.metadata.create_all(self.db.engine, checkfirst=True)
        models.migrations.drop(self.db.engine)
        models.get_data_table('t1', self.db.meta).create(self.db.engine, checkfirst=True)

        self.db.engine.execute(models.pipes.insert().values(bot='x', pipe='p1'))

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
        models.metadata.create_all(self.db.engine, checkfirst=True)

        table = models.get_data_table('t1', self.db.meta)
        table.create(self.db.engine, checkfirst=True)

        self.db.engine.execute(table.insert().values(key='1', value=b'a'))
        self.db.engine.execute(models.pipes.insert().values(bot='x', pipe='p1'))

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
        models.migrations.create(self.db.engine)
        self.assertFalse(self.migrations.has_initial_state())


@tests.db.usedb()
class ValueToMsgpackTests(object):

    def setUp(self):
        super().setUp()
        self.output = io.StringIO()
        self.migration = migrations.ValueToMsgpack(self.db.engine, Printer(self.output), verbosity=2)
        self.table = models.get_data_table('t1', self.db.meta)
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
