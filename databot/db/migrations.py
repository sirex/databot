import sys
import datetime
import toposort
import json
import msgpack
import tqdm

from databot.db import models
from databot.db.windowedquery import windowed_query


def value_to_msgpack(engine, table, output, verbosity):
    total = engine.execute(table.count()).scalar()
    rows = windowed_query(engine, table.select(), table.c.id)
    if verbosity == 1:
        rows = tqdm.tqdm(rows, total=total, file=output)
    for row in rows:
        value = json.loads(row['value'].decode())
        value['content'] = value.pop('text').encode()
        value = msgpack.dumps(value, use_bin_type=True)
        engine.execute(table.update().values(value=value))


_migrations = {
    value_to_msgpack: set(),
}


def get_applied_migrations(engine):
    models.migrations.create(engine, checkfirst=True)
    return {row['name'] for row in engine.execute(models.migrations.select())}


def add_migration(engine, name):
    return engine.execute(
        models.migrations.insert(),
        name=name,
        created=datetime.datetime.utcnow(),
    )


def iter_data_tables(engine, metadata):
    pipes = {'t%s' % row.id: row['pipe'] for row in engine.execute(models.pipes.select())}
    metadata.reflect(bind=engine)
    for table in metadata.sorted_tables:
        if table.name in pipes:
            yield table, pipes[table.name]


def run_migrations(engine, tables, migrations, applied, output, verbosity):
    for migration in reversed(toposort.toposort_flatten(migrations, sort=True)):
        name = migration.__name__
        print('Migrate %s...' % name, file=output)
        if name not in applied:
            for table, pipe in tables:
                print('  %s' % pipe, file=output)
                migration(engine, table, output, verbosity)
                applied.add(name)
                add_migration(engine, name)
        else:
            print('Migrate %s... (already applied)' % name, file=output)


def migrate(engine, metadata, migrations=_migrations, output=sys.stdout, verbosity=1):
    tables = list(iter_data_tables(engine, metadata))
    run_migrations(engine, tables, migrations, get_applied_migrations(), output, verbosity)
