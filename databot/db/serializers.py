import msgpack
import hashlib


def loads(value):
    """Convert Python object to a primitive value for storing to database."""
    if value is None:
        return None
    else:
        return msgpack.loads(value, encoding='utf-8')


def dumps(value):
    """Convert primitive value received from database to Python object."""
    return msgpack.dumps(value, use_bin_type=True)


def serkey(key):
    """Serialize a value to fixed size sha1 key"""
    assert isinstance(key, (int, str, bytes, list, tuple))
    return hashlib.sha1(dumps(key)).hexdigest()


def serrow(key, value, **kwargs):
    return dict(kwargs, key=serkey(key), value=dumps([key, value]))
