import sqlalchemy as sa

from databot.bulkinsert import BulkInsert
from databot.utils.objsize import getsize


def test_bulk_insert(db):
    table = db.models.get_data_table('t1')
    db.meta.create_all(db.engine)

    def query():
        return [int(row[table.c.key]) for row in db.engine.execute(sa.select([table]))]

    size = getsize({'key': '1', 'value': b'a'})
    bulk = BulkInsert(db.engine, table, size * 2)

    bulk.append({'key': '1', 'value': b'a'})
    assert query() == []

    bulk.append({'key': '2', 'value': b'b'})
    assert query() == []

    bulk.append({'key': '3', 'value': b'c'})
    assert query() == [1, 2]

    bulk.save()
    assert query() == [1, 2, 3]
