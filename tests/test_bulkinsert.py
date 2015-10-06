import sqlalchemy as sa
import tests.db

from databot.db.models import get_data_table
from databot.bulkinsert import BulkInsert
from databot.utils.objsize import getsize


@tests.db.usedb()
class BulkInserTests(object):
    def test_bulk_insert(self):
        table = get_data_table('t1', self.db.meta)
        self.db.meta.create_all(self.db.engine)

        query = lambda: [int(row[table.c.key]) for row in self.db.engine.execute(sa.select([table]))]

        size = getsize({'key': '1', 'value': b'a'})
        bulk = BulkInsert(self.db.engine, table, size * 2)

        bulk.append({'key': '1', 'value': b'a'})
        self.assertEqual(query(), [])

        bulk.append({'key': '2', 'value': b'b'})
        self.assertEqual(query(), [])

        bulk.append({'key': '3', 'value': b'c'})
        self.assertEqual(query(), [1, 2])

        bulk.save()
        self.assertEqual(query(), [1, 2, 3])
