import io
import json
import collections
import contextlib


@contextlib.contextmanager
def open(path_or_file, *args, **kwargs):
    if isinstance(path_or_file, str):
        with io.open(path_or_file, *args, **kwargs) as f:
            yield f
    else:
        yield path_or_file


def export(path, pipe, append=False):
    mode = 'a' if append else 'w'

    with open(path, mode, encoding='utf-8') as f:
        for row in pipe.data.rows():
            if isinstance(row.value, dict):
                value = row.value
            else:
                value = {'value': row.value}
            data = collections.OrderedDict([('key', row.key)])
            data.update(value)
            f.write(json.dumps(data) + '\n')
