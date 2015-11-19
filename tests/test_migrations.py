import io
import json
import unittest

import databot
import databot.pipes

from databot.db.migrations import get_applied_migrations, add_migration, run_migrations
from databot.db.migrations import value_to_msgpack


def migration_a(engine):
    pass


def migration_b(engine):
    pass


class MigrateTests(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.engine = databot.Bot('sqlite:///:memory:').engine

    def test_get_applied_migrations(self):
        add_migration(self.engine, 'a')
        self.assertEqual(get_applied_migrations(self.engine), {'a'})

    def test_run_migrations(self):
        applied = set()
        migrations = {
            migration_b: {migration_a},
            migration_a: set(),
        }
        self.assertEqual(get_applied_migrations(self.engine), set())
        run_migrations(self.engine, migrations, applied, self.output)
        self.assertEqual(get_applied_migrations(self.engine), {'migration_a', 'migration_b'})
        self.assertEqual(self.output.getvalue(), (
            'Migrate migration_b...\n'
            'Migrate migration_a...\n'
        ))


class ValueToMsgpack(unittest.TestCase):

    def setUp(self):
        self.output = io.StringIO()
        self.bot = databot.Bot('sqlite:///:memory:')
        self.pipe = self.bot.pipe('p1')

    def test_value_to_msgpack(self):
        value = {
            'headers': {},
            'cookies': {},
            'status_code': 200,
            'encoding': 'utf-8',
            'text': b'<html></html>',
        }
        self.bot.engine.execute(
            self.pipe.table.insert(),
            key='http://example.com/',
            value=json.dumps(value).encode('utf-8'),
        )

        value_to_msgpack(self.bot.engine, self.pipe.table, self.output)
        self.assertEqual(self.pipe.data.items(), [
            ('http://example.com/', {
                'headers': {},
                'cookies': {},
                'status_code': 200,
                'encoding': 'utf-8',
                'content': b'<html></html>',
            }),
        ])
