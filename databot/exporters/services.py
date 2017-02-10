import types

from databot.exporters import csv, jsonl, pandas


def export(rows, dest, **kwargs):
    if isinstance(dest, str):
        if dest.endswith('.jsonl'):
            jsonl.export(dest, rows, **kwargs)
        else:
            csv.export(dest, rows, **kwargs)
    elif isinstance(dest, types.ModuleType) and dest.__name__ == 'pandas':
        return pandas.export(dest, rows, **kwargs)
    else:
        raise TypeError("Unknown destination: %s" % type(dest))
