import json


def loads(value):
    """Convert Python object to a primitive value for storing to database."""
    if value is None:
        return None
    else:
        return json.loads(value.decode('utf-8'))


def dumps(value):
    """Convert primitive value received from database to Python object."""
    return json.dumps(value).encode('utf-8')
