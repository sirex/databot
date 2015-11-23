import sys
import datetime
import toposort
import json
import msgpack
import tqdm
import sqlalchemy as sa

from sqlalchemy.engine import reflection

from databot.db import models
from databot.db.windowedquery import windowed_query


class Migration(object):

    data_tables = False

    def __init__(self, engine, output, verbosity):
        self.engine = engine
        self.output = output
        self.verbosity = verbosity

    def migrate(self):
        pass

    def migrate_data_table(self, table):
        pass

    def migrate_data(self, table, pipe):
        if hasattr(self, 'migrate_data_item'):
            total = self.engine.execute(table.count()).scalar()
            rows = windowed_query(self.engine, table.select(), table.c.id)
            if self.verbosity == 1:
                rows = tqdm.tqdm(rows, total=total, file=self.output.output)
            for row in rows:
                self.engine.execute(table.update().values(**self.migrate_data_item(row)))


class MigrationsTable(Migration):

    name = "migrations table"

    def migrate(self):
        self.output.info('  creating migrations table...')
        models.migrations.create(self.engine)


class ValueToMsgpack(Migration):

    name = "value to msgpack"
    data_tables = True

    def migrate_data_item(self, row):
        value = json.loads(row['value'].decode())
        if isinstance(value, dict) and 'text' in value and 'status_code' in value:
            value['content'] = value.pop('text').encode()
        value = msgpack.dumps(value, use_bin_type=True)
        return dict(value=value)


class Migrations(object):

    migrations = {
        MigrationsTable: set(),
        ValueToMsgpack: {MigrationsTable},
    }

    def __init__(self, metadata, engine, output=sys.stdout, verbosity=1):
        """
        Args:
            metadata: sqlalchemy metadata for data tables
        """
        self.metadata = metadata
        self.engine = engine
        self.output = output
        self.verbosity = verbosity

    def available(self):
        return toposort.toposort_flatten(self.migrations, sort=True)

    def applied(self):
        if 'databotmigrations' in self.table_names():
            available = {x.name: x for x in self.available()}
            return {available[row['name']] for row in self.engine.execute(models.migrations.select())}
        else:
            return set()

    def unapplied(self):
        return set(self.available()) - self.applied()

    def mark_applied(self, name):
        return self.engine.execute(
            models.migrations.insert(),
            name=name,
            created=datetime.datetime.utcnow(),
        )

    def table_names(self):
        inspector = reflection.Inspector.from_engine(self.engine)
        return sorted(inspector.get_table_names())

    def data_tables(self):
        tables = set(reflection.Inspector.from_engine(self.engine).get_table_names())
        for pipe in self.engine.execute(models.pipes.select()):
            table = 't%s' % pipe['id']
            if table in tables:
                yield sa.Table(table, self.metadata, autoload=True, autoload_with=self.engine), pipe['pipe']

    def has_initial_state(self):
        """Check if we need to run initial migrations or not."""
        return len(self.table_names()) == 0

    def initialize(self):
        models.metadata.create_all(self.engine, checkfirst=True)
        for fn in self.available():
            self.mark_applied(fn.name)

    def migrate(self):
        applied = self.applied()
        for Migration in self.available():
            migration = Migration(self.engine, self.output, self.verbosity)
            if Migration not in applied:
                self.output.info('- %s...' % migration.name)
                migration.migrate()
                if migration.data_tables:
                    for table, pipe in self.data_tables():
                        self.output.info('  %s' % pipe)
                        migration.migrate_data_table(table)
                        migration.migrate_data(table, pipe)
                        applied.add(Migration)
                self.mark_applied(migration.name)
            else:
                self.output.info('- %s... (already applied)' % migration.name)
        self.output.info('done.')
