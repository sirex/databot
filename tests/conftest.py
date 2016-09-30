import io
import os.path
import pathlib
import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.pool
import pytest
import databot

from databot.db.models import Models

_connections = {}


def get_db_connection(name, uri):
    global _connections
    if name not in _connections:
        engine = sa.create_engine(uri, poolclass=sqlalchemy.pool.SingletonThreadPool)
        engine.connect()
        _connections[name] = engine
    return _connections[name]


class Db:

    def __init__(self):
        self.uri = self.get_connection_uri()
        self.engine = get_db_connection(self.name, self.uri)
        if self.engine:
            self.meta = sa.MetaData(self.engine)
            self.models = Models(self.meta)

    def Bot(self):
        output = io.StringIO()
        return databot.Bot(self.engine, output=output, models=self.models)

    def finalize(self):
        self.meta.reflect()
        self.meta.drop_all()


class NoDatabaseFixture(object):

    def __init__(self, error):
        super().__init__()
        self.error = error


class SqliteDb(Db):
    name = 'sqlite'

    def get_connection_uri(self):
        return 'sqlite:///:memory:'


class PostgreSqlDb(Db):
    name = 'postgresql'

    def get_connection_uri(self):
        return 'postgresql:///databottestsdb'


class MySqlDb(Db):
    name = 'mysql'

    def get_connection_uri(self):
        global _connections

        if self.name not in _connections:
            def find_my_cnf(locations):
                for location in locations:
                    path = pathlib.Path(os.path.expanduser(location))
                    if path.exists():
                        return path.resolve()

            my_cnf = find_my_cnf(['~/.my.cnf', 'my.cnf'])
            return 'mysql+pymysql:///databottestsdb' + ('?read_default_file=%s' % my_cnf if my_cnf else '')
        else:
            return None


@pytest.fixture(params=[SqliteDb, PostgreSqlDb, MySqlDb])
def db(request):
    DbClass = request.param
    try:
        db = DbClass()
    except (sqlalchemy.exc.OperationalError, ImportError) as e:
        pytest.skip(str(e))
    else:
        request.addfinalizer(db.finalize)
        return db


@pytest.fixture
def sqlite(request):
    db = SqliteDb()
    request.addfinalizer(db.finalize)
    return db


@pytest.fixture
def bot():
    return databot.Bot('sqlite:///:memory:', output=io.StringIO())
