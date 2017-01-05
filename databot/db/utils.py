import pathlib
import sqlalchemy as sa
import sqlalchemy.orm.exc

from databot.db.models import Compression
from databot.db.serializers import loads


class Row(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self


def create_row(row):
    compression = None if row['compression'] is None else Compression(row['compression'])
    key, value = loads(row['value'], compression)
    return Row(row, key=key, value=value, compression=compression)


def strip_prefix(row, prefix):
    result = Row()
    for key, value in row.items():
        if key.startswith(prefix):
            result[key[len(prefix):]] = value
    return result


def fetch_some(result, n):
    """Fetch n rows from result and close result cursor.

    Parameters:
    - result: sqlalchemy.engine.result.ResultProxy
    - n: int, number of rows to fetch

    Returns: generator
    """
    row = None
    for i in range(n):
        row = result.fetchone()
        if row:
            yield Row(row)
        else:
            break
    if row:
        result.close()


def get_or_none(engine, model, *params):
    result = list(fetch_some(engine.execute(model.select(sa.and_(*params))), 2))
    n = len(result)
    if n == 1:
        return result[0]
    elif n > 1:
        raise sqlalchemy.orm.exc.MultipleResultsFound
    else:
        return None


def get_or_create(engine, model, fields, data):
    """Get instance from database or create if not found.

    Parameters:
    - engine: sqlalchemy.engine.base.Engine
    - model: sqlalchemy.sql.schema.Table
    - fields: list of keys from data dict
    - data: dict for new object to be created if existing does not exists

    Returns: sqlalchemy.engine.result.RowProxy

    """
    params = [model.columns[field] == data[field] for field in fields]
    row = get_or_none(engine, model, *params)
    if row:
        return Row(row)
    else:
        engine.execute(model.insert(), **data)
        return get_or_none(engine, model, *params)


def get_engine(uri_or_engine, path=''):
    if isinstance(uri_or_engine, str):
        spl = uri_or_engine.split(':', 1)
        spl = spl[0].split('+', 1) if len(spl) > 1 and '+' in spl[0] else spl
        if len(spl) > 1 and spl[0] in ('sqlite', 'postgresql', 'mysql'):
            return sa.create_engine(uri_or_engine.format(path=path))
        else:
            filename = pathlib.Path(uri_or_engine)
            return sa.create_engine('sqlite:///%s' % filename)
    else:
        return uri_or_engine
