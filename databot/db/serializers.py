import gzip
import msgpack
import hashlib

from databot.db.models import Compression


def loads(value, compression=None):
    """Convert Python object to a primitive value for storing to database."""
    if value is None:
        return None
    if compression == Compression.gzip:
        value = gzip.decompress(value)
    return msgpack.loads(value, encoding='utf-8')


def dumps(value):
    """Convert primitive value received from database to Python object."""
    return msgpack.dumps(value, use_bin_type=True)


def serkey(key):
    """Serialize a value to fixed size sha1 key"""
    assert isinstance(key, (int, str, bytes, list, tuple)), '%s: %s' % (type(key), repr(key))
    return hashlib.sha1(dumps(key)).hexdigest()


def serrow(key, value, compression=None, **kwargs):
    value = dumps([key, value])
    if compression == Compression.gzip:
        value = gzip.compress(value)
    compression = None if compression is None else int(compression.value)
    return dict(kwargs, key=serkey(key), value=value, compression=compression)
