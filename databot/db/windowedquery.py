import funcy
import sqlalchemy as sa

from sqlalchemy.engine.base import Engine


def column_windows(engine, column, windowsize):
    # Based on: https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/WindowedRangeQuery
    rows = sa.select([column, sa.func.row_number().over(order_by=column).label('rownum')]).alias()
    whereclause = sa.text('rownum %% %d = 1' % windowsize) if windowsize > 1 else None
    query = sa.select([rows.c[column.name]], whereclause).select_from(rows).order_by(rows.c.rownum)
    intervals = [id for id, in engine.execute(query)]

    end_id = intervals[0] if intervals else None
    for start_id, end_id in funcy.pairwise(intervals):
        yield sa.and_(sa.and_(column >= start_id, column < end_id))

    if end_id is not None:
        yield column >= end_id


def offset_windows(engine, query, column, windowsize):
    total = engine.execute(query.alias().count()).scalar()
    yield from range(0, total, windowsize)


def windowed_query(engine, query, column, windowsize=1000):
    """"Break a query into windows on a given column."""
    name = engine.name if isinstance(engine, Engine) else engine.engine.name
    if name == 'postgresql':
        for whereclause in column_windows(engine, column, windowsize):
            yield from list(engine.execute(query.where(whereclause).order_by(column)))
    else:
        for offset in offset_windows(engine, query, column, windowsize):
            yield from list(engine.execute(query.order_by(column).offset(offset).limit(windowsize)))
