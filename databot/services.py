from operator import itemgetter
from itertools import groupby

from databot import recursive


def merge_rows(items):
    """Merge all values grouped by keys.

    items shoulbe be sorted by key already.
    """
    for key, values in groupby(items, key=itemgetter(0)):
        values = (v for k, v in values)
        value = next(values, None)
        merged = False
        for new in values:
            if value is None and isinstance(new, dict):
                value = new
            elif isinstance(value, dict) and isinstance(new, dict):
                recursive.merge(value, new)
                merged = True
            else:
                merged = False
                break
        if merged:
            yield key, value
