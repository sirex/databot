import io
import json
import unittest
import sqlalchemy as sa

import databot
import databot.pipes

from databot.db.migrations import get_applied_migrations, add_migration, run_migrations, iter_data_tables
from databot.db.migrations import value_to_msgpack


def migration_a(engine, table, output, verbosity):
    pass


def migration_b(engine, table, output, verbosity):
    pass


class MigrateTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        bot = databot.Bot('sqlite:///:memory:')
        bot.define('p1')
        self.engine = bot.engine

    def test_get_applied_migrations(self):
        add_migration(self.engine, 'a')
        self.assertEqual(get_applied_migrations(self.engine), {'a'})

    def test_run_migrations(self):
        applied = set()
        migrations = {
            migration_b: {migration_a},
            migration_a: set(),
        }
        metadata = sa.MetaData()
        metadata.reflect(bind=self.engine)
        tables = list(iter_data_tables(self.engine, metadata))
        self.assertEqual(get_applied_migrations(self.engine), set())
        run_migrations(self.engine, tables, migrations, applied, self.output, verbosity=2)
        self.assertEqual(get_applied_migrations(self.engine), {'migration_a', 'migration_b'})
        self.assertEqual(self.output.getvalue(), (
            'Migrate migration_b...\n'
            '  p1\n'
            'Migrate migration_a...\n'
            '  p1\n'
        ))


class ValueToMsgpack(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = databot.Bot('sqlite:///:memory:')
        self.pipe = self.bot.define('p1')

    def test_value_to_msgpack(self):
        value = {
            'headers': {},
            'cookies': {},
            'status_code': 200,
            'encoding': 'utf-8',
            'text': '<html></html>',
        }
        self.bot.engine.execute(
            self.pipe.table.insert(),
            key='http://example.com/',
            value=json.dumps(value).encode(),
        )

        value_to_msgpack(self.bot.engine, self.pipe.table, self.output, verbosity=2)
        self.assertEqual(list(self.pipe.data.items()), [
            ('http://example.com/', {
                'headers': {},
                'cookies': {},
                'status_code': 200,
                'encoding': 'utf-8',
                'content': b'<html></html>',
            }),
        ])
