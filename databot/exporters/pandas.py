from databot.exporters.utils import flatten_rows


def index_from_columns(columns, name):
    if name in columns:
        pos = columns.index(name)
        columns = [x for x in columns if x != name]
    else:
        pos = None
    return pos, columns


def index_from_row(row, index, idx):
    if index is not None:
        idx = row[index]
        row = [x for i, x in enumerate(row) if i != index]
    return idx, row


def rows_to_dataframe_items(rows, index):
    for i, row in enumerate(rows):
        yield index_from_row(row, index, i)


def export(pd, rows, exclude=None, include=None, update=None):
    rows = flatten_rows(rows, exclude, include, update)
    columns = next(rows, None)
    index, columns = index_from_columns(columns, 'key')
    items = rows_to_dataframe_items(rows, index)
    return pd.DataFrame.from_items(items, columns=columns, orient='index')
