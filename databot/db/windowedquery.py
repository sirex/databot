import funcy
import sqlalchemy as sa


def column_windows(engine, column, windowsize):
    # Based on: https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/WindowedRangeQuery
    rows = sa.select([column, sa.func.row_number().over(order_by=column).label('rownum')]).alias()
    whereclause = sa.text('rownum %% %d = 1' % windowsize) if windowsize > 1 else None
    query = sa.select([rows.c[column.name]], whereclause).select_from(rows).order_by(rows.c.rownum)
    intervals = [id for id, in engine.execute(query)]
    print('intervals = %r' % intervals)

    end_id = intervals[0] if intervals else None
    for start_id, end_id in funcy.pairwise(intervals):
        yield sa.and_(sa.and_(column >= start_id, column < end_id))

    if end_id is not None:
        yield column >= end_id


def offset_windows(engine, query, column, windowsize):
    total = engine.execute(query.count()).scalar()
    for offset in range(0, total, windowsize):
        for row in engine.execute(query.order_by(column).offset(offset).limit(windowsize)):
            yield row


def windowed_query(engine, query, column, windowsize=1000):
    """"Break a query into windows on a given column."""
    if engine.name == 'postgresql':
        for whereclause in column_windows(engine, column, windowsize):
            for row in engine.execute(query.where(whereclause).order_by(column)):
                yield row
    else:
        yield from offset_windows(engine, query, column, windowsize)
