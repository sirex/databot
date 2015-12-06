import sys
import datetime
import toposort
import json
import msgpack
import tqdm
import sqlalchemy as sa
import hashlib

from alembic.migration import MigrationContext
from alembic.operations import Operations

from sqlalchemy.engine import reflection

from databot.db.windowedquery import windowed_query


class Migration(object):

    data_tables = False

    def __init__(self, models, engine, output, verbosity):
        self.models = models
        self.engine = engine
        self.output = output
        self.verbosity = verbosity
        self.op = Operations(MigrationContext.configure(engine))

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
                self.engine.execute(table.update().where(table.c.id == row['id']).values(**self.migrate_data_item(row)))


class MigrationsTable(Migration):

    name = "migrations table"

    def migrate(self):
        self.output.info('  creating migrations table...')
        self.models.migrations.create(self.engine)


class ValueToMsgpack(Migration):

    name = "value to msgpack"
    data_tables = True

    def migrate_data_item(self, row):
        value = json.loads(row['value'].decode())
        if isinstance(value, dict) and 'text' in value and 'status_code' in value:
            value['content'] = value.pop('text').encode()
        value = msgpack.dumps(value, use_bin_type=True)
        return dict(value=value)


class KeyToSha1(Migration):

    name = "key to sha1"
    data_tables = True

    def migrate_data_item(self, row):
        key = row['key']
        value = msgpack.loads(row['value'], encoding='utf-8')
        data = [key, value]
        key = hashlib.sha1(msgpack.dumps(key, use_bin_type=True)).hexdigest()
        value = msgpack.dumps(data, use_bin_type=True)
        return dict(key=key, value=value)


class AlterKeyField(Migration):

    name = "key to unicode(40)"
    data_tables = True

    def migrate_data_table(self, table):
        with self.op.batch_alter_table(table.name) as op:
            op.drop_index('ix_%s_key' % table.name)
            op.alter_column('key', type_=sa.Unicode(40))
        self.op.create_index('ix_%s_key' % table.name, table.name, ['key'])


class Migrations(object):

    migrations = {
        MigrationsTable: set(),
        ValueToMsgpack: {MigrationsTable},
        KeyToSha1: {ValueToMsgpack},
        AlterKeyField: {KeyToSha1},
    }

    def __init__(self, models, engine, output=sys.stdout, verbosity=1):
        self.models = models
        self.engine = engine
        self.output = output
        self.verbosity = verbosity

    def available(self):
        return toposort.toposort_flatten(self.migrations, sort=True)

    def applied(self):
        if 'databotmigrations' in self.table_names():
            available = {x.name: x for x in self.available()}
            return {available[row['name']] for row in self.engine.execute(self.models.migrations.select())}
        else:
            return set()

    def unapplied(self):
        return set(self.available()) - self.applied()

    def mark_applied(self, name, conn=None):
        conn = conn or self.engine
        return conn.execute(
            self.models.migrations.insert(),
            name=name,
            created=datetime.datetime.utcnow(),
        )

    def table_names(self):
        inspector = reflection.Inspector.from_engine(self.engine)
        return sorted(inspector.get_table_names())

    def data_tables(self):
        result = []
        tables = set(reflection.Inspector.from_engine(self.engine).get_table_names())
        for pipe in self.engine.execute(self.models.pipes.select()):
            table = 't%s' % pipe['id']
            if table in tables:
                result.append((
                    sa.Table(table, self.models.metadata, autoload=True, autoload_with=self.engine),
                    pipe['pipe'],
                ))
        return result

    def has_initial_state(self):
        """Check if we need to run initial migrations or not."""
        return len(self.table_names()) == 0

    def initialize(self):
        self.models.metadata.create_all(self.engine, checkfirst=True)
        for fn in self.available():
            self.mark_applied(fn.name)

    def migrate(self):
        applied = self.applied()
        for Migration in self.available():
            if Migration not in applied:
                self.output.info('- %s...' % Migration.name)
                with self.engine.begin() as conn:
                    migration = Migration(self.models, conn, self.output, self.verbosity)
                    migration.migrate()
                    if migration.data_tables:
                        for table, pipe in self.data_tables():
                            self.output.info('  %s' % pipe)
                            migration.migrate_data_table(table)
                            migration.migrate_data(table, pipe)
                            applied.add(Migration)
                    self.mark_applied(migration.name, conn)
            else:
                self.output.info('- %s... (already applied)' % Migration.name)
        self.output.info('done.')
