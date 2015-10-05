import sys
import os.path
import pathlib
import unittest
import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.pool
import nose.plugins.attrib

connections = {}


class DatabaseFixture(object):
    def __init__(self, engine=None):
        self.engine = engine
        self.meta = None

    def set_up(self):
        self.meta = sa.MetaData(self.engine)

    def tear_down(self):
        self.meta.reflect()
        self.meta.drop_all()


class NoDatabaseFixture(object):
    def __init__(self, error):
        super().__init__()
        self.error = error


def get_database_fixture(name, uri, FixtureClass=DatabaseFixture):
    global connections

    if name not in connections:
        engine = sa.create_engine(uri, poolclass=sqlalchemy.pool.SingletonThreadPool)
        try:
            engine.connect()
        except sqlalchemy.exc.OperationalError as e:
            connections[name] = NoDatabaseFixture(str(e))
        else:
            connections[name] = DatabaseFixture(engine)

    fixture = connections[name]

    if isinstance(fixture, NoDatabaseFixture):
        raise unittest.SkipTest(fixture.error)
    else:
        return connections[name]


def get_sqlite(name='sqlite'):
    return get_database_fixture(name, 'sqlite:///:memory:')


def get_postgresql(name='postgresql'):
    return get_database_fixture(name, 'postgresql:///databottestsdb')


def get_mysql(name='mysql'):
    if name not in connections:
        def find_my_cnf(locations):
            for location in locations:
                path = pathlib.Path(os.path.expanduser(location))
                if path.exists():
                    return path.resolve()

        my_cnf = find_my_cnf(['~/.my.cnf', 'my.cnf'])
        uri = 'mysql+pymysql:///databottestsdb' + ('?read_default_file=%s' % my_cnf if my_cnf else '')
    else:
        uri = None

    return get_database_fixture(name, uri)


class SqliteTestCase(object):
    def setUp(self):
        self.db = get_sqlite()
        self.db.set_up()
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.db.tear_down()


class PsqlTestCase(object):
    def setUp(self):
        self.db = get_postgresql()
        self.db.set_up()
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.db.tear_down()


class MysqlTestCase(object):
    def setUp(self):
        self.db = get_mysql()
        self.db.set_up()
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.db.tear_down()


def usedb(*bases):
    """Duplicate same test with different database setups.

    Duplicated test cases has nose attributes [1], each test case has sqlite, psql and mysq attributes and all
    databases, except sqlite has slowdb attribute.

    To run all tests, except slowdb use:

        nosetests -a !slowdb tests

    To test database tests run:

        nosetests -a psql -a mysql tests

    [1] http://nose.readthedocs.org/en/latest/plugins/attrib.html
    """

    bases = tuple(bases) or (unittest.TestCase,)

    def decorator(klass):
        module = sys.modules[klass.__module__]
        test_cases = (
            ('sqlite', 'Sqlite', SqliteTestCase),
            ('psql', 'PostgreSQL', PsqlTestCase),
            ('mysql', 'MySQL', MysqlTestCase),
        )
        for attr, test_case_name, TestCaseClass in test_cases:
            attrib = nose.plugins.attrib.attr(**{attr: True, 'slowdb': attr != 'sqlite'})
            name = '%s_%s' % (klass.__name__, test_case_name)
            TestCase = type(name, (klass, TestCaseClass) + bases, {})
            setattr(module, name, attrib(TestCase))
        return klass

    return decorator
