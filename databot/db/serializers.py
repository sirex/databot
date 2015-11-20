import msgpack


def loads(value):
    """Convert Python object to a primitive value for storing to database."""
    if value is None:
        return None
    else:
        return msgpack.loads(value, encoding='utf-8')


def dumps(value):
    """Convert primitive value received from database to Python object."""
    return msgpack.dumps(value, use_bin_type=True)
