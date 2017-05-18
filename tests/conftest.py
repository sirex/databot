import io
import os.path
import pathlib
import sqlalchemy as sa
import sqlalchemy.exc
import sqlalchemy.pool
import pytest
import databot
import requests_mock
import datetime
import dateutil

from freezegun.api import FakeDatetime, FakeDate, FrozenDateTimeFactory, convert_to_timezone_naive

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


@pytest.yield_fixture
def requests():
    with requests_mock.mock() as mock:
        yield mock


@pytest.fixture
def freezetime(request, mocker):
    def unfreeze():
        FakeDate.dates_to_freeze.pop()
        FakeDate.tz_offsets.pop()

        FakeDatetime.times_to_freeze.pop()
        FakeDatetime.tz_offsets.pop()

    def freeze(time_to_freeze_str, tz_offset=0):
        if isinstance(time_to_freeze_str, datetime.datetime):
            time_to_freeze = time_to_freeze_str
        elif isinstance(time_to_freeze_str, datetime.date):
            time_to_freeze = datetime.datetime.combine(time_to_freeze_str, datetime.time())
        else:
            time_to_freeze = dateutil.parser.parse(time_to_freeze_str)

        time_to_freeze = convert_to_timezone_naive(time_to_freeze)
        time_to_freeze = FrozenDateTimeFactory(time_to_freeze)

        FakeDate.dates_to_freeze.append(time_to_freeze)
        FakeDate.tz_offsets.append(tz_offset)

        FakeDatetime.times_to_freeze.append(time_to_freeze)
        FakeDatetime.tz_offsets.append(tz_offset)

        mocker.patch('datetime.date', FakeDate)
        mocker.patch('datetime.datetime', FakeDatetime)

        request.addfinalizer(unfreeze)

    return freeze
