import sys
import os.path
import pathlib
import unittest
import sqlalchemy as sa
import sqlalchemy.exc

connections = {}


class DatabaseFixture(object):
    def __init__(self, engine=None):
        self.engine = engine
        self.meta = None

    def set_up(self):
        self.meta = sa.MetaData(self.engine, reflect=True)

    def tear_down(self):
        self.meta.drop_all()


class NoDatabaseFixture(object):
    def __init__(self, error):
        super().__init__()
        self.error = error


def get_database_fixture(name, uri, FixtureClass=DatabaseFixture):
    global connections

    if name not in connections:
        engine = sa.create_engine(uri)
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
    bases = tuple(bases) or (unittest.TestCase,)

    def decorator(klass):
        module = sys.modules[klass.__module__]
        test_cases = (
            ('Sqlite', SqliteTestCase),
            ('PostgreSQL', PsqlTestCase),
            ('MySQL', MysqlTestCase),
        )
        for test_case_name, TestCaseClass in test_cases:
            name = '%s_%s' % (klass.__name__, test_case_name)
            TestCase = type(name, (klass, TestCaseClass) + bases, {})
            setattr(module, name, TestCase)
        return klass

    return decorator
