import sys
import datetime
import toposort

from databot.db import models


def value_to_msgpack(engine, output):
    pass


_migrations = {
    value_to_msgpack: set(),
}


def get_applied_migrations(engine):
    models.migrations.create(engine, checkfirst=True)
    return {row['name'] for row in engine.execute(models.migrations.select()).fetchall()}


def add_migration(engine, name):
    return engine.execute(
        models.migrations.insert(),
        name=name,
        created=datetime.datetime.utcnow(),
    )


def run_migrations(engine, migrations, applied, output):
    for migration in reversed(toposort.toposort_flatten(migrations, sort=True)):
        name = migration.__name__
        if name not in applied:
            print('Migrate %s...' % name, file=output)
            migration(engine)
            applied.add(name)
            add_migration(engine, name)
        else:
            print('Migrate %s... (already applied)' % name, file=output)


def migrate(engine, migrations=_migrations, output=sys.stdout):
    run_migrations(engine, migrations, get_applied_migrations(), output)
