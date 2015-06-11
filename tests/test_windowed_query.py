import tests.db

from databot.db.windowedquery import windowed_query
from databot.db.models import get_data_table
from databot.db.serializers import dumps


def row(key, value=None):
    return dict(key=str(key), value=dumps(value))


def keys(result):
    return [int(row['key']) for row in result]


def populate(engine, table, keys):
    engine.execute(table.insert(), [row(key) for key in keys])


@tests.db.usedb()
class WindowedQueryTests(object):
    def setUp(self):
        super().setUp()
        self.table = get_data_table('t1', self.db.meta)
        self.db.meta.create_all()

    def test_windowed_query(self):
        populate(self.db.engine, self.table, [1, 2, 3, 4])
        query = windowed_query(self.db.engine, self.table.select(), self.table.c.id)
        self.assertEqual(keys(query), [1, 2, 3, 4])

    def test_small_windowsize(self):
        populate(self.db.engine, self.table, [1, 2, 3, 4, 5])
        query = windowed_query(self.db.engine, self.table.select(), self.table.c.id, windowsize=2)
        self.assertEqual(keys(query), [1, 2, 3, 4, 5])

    def test_even_windowsize(self):
        populate(self.db.engine, self.table, [1, 2, 3, 4, 5, 6])
        query = windowed_query(self.db.engine, self.table.select(), self.table.c.id, windowsize=2)
        self.assertEqual(keys(query), [1, 2, 3, 4, 5, 6])
