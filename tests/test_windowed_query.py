import unittest
import sqlalchemy as sa

from databot.db.windowedquery import windowed_query
from databot.db.models import get_data_table
from databot.db.serializers import dumps


def row(key, value=None):
    return dict(key=str(key), value=dumps(value))


def keys(result):
    return [int(row['key']) for row in result]


def populate(engine, table, keys):
    engine.execute(table.insert(), [row(key) for key in keys])


class WindowedQueryTests(object):
    def test_windowed_query(self):
        populate(self.engine, self.table, [1, 2, 3, 4])
        query = windowed_query(self.engine, self.table.select(), self.table.c.id)
        self.assertEqual(keys(query), [1, 2, 3, 4])

    def test_small_windowsize(self):
        populate(self.engine, self.table, [1, 2, 3, 4, 5])
        query = windowed_query(self.engine, self.table.select(), self.table.c.id, windowsize=2)
        self.assertEqual(keys(query), [1, 2, 3, 4, 5])


class WindowedQuerySqliteTests(WindowedQueryTests, unittest.TestCase):
    def setUp(self):
        self.engine = sa.create_engine('sqlite:///:memory:')
        meta = sa.MetaData(self.engine)
        self.table = get_data_table('t1', meta)
        meta.create_all()


class WindowedQueryPsqlTests(WindowedQueryTests, unittest.TestCase):
    def setUp(self):
        self.engine = sa.create_engine('postgresql:///databot')
        meta = sa.MetaData(self.engine)
        self.table = get_data_table('t1', meta)
        meta.create_all()

    def tearDown(self):
        sa.MetaData(self.engine, reflect=True).drop_all()
