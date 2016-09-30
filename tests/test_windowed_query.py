import pytest

from databot.db.windowedquery import windowed_query
from databot.db.serializers import dumps


def row(key, value=None):
    return dict(key=str(key), value=dumps(value))


def keys(result):
    return [int(row['key']) for row in result]


def populate(engine, table, keys):
    engine.execute(table.insert(), [row(key) for key in keys])


@pytest.fixture
def table(db):
    table = db.models.get_data_table('t1')
    db.meta.create_all()
    return table


def test_windowed_query(db, table):
    populate(db.engine, table, [1, 2, 3, 4])
    query = windowed_query(db.engine, table.select(), table.c.id)
    assert keys(query) == [1, 2, 3, 4]


def test_small_windowsize(db, table):
    populate(db.engine, table, [1, 2, 3, 4, 5])
    query = windowed_query(db.engine, table.select(), table.c.id, windowsize=2)
    assert keys(query) == [1, 2, 3, 4, 5]


def test_even_windowsize(db, table):
    populate(db.engine, table, [1, 2, 3, 4, 5, 6])
    query = windowed_query(db.engine, table.select(), table.c.id, windowsize=2)
    assert keys(query) == [1, 2, 3, 4, 5, 6]
