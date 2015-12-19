class ErrorHandler(object):
    """Pipe call handler that throws error for specified key"""

    def __init__(self, *error_keys):
        self.error_keys = set(error_keys)

    def __call__(self, row):
        if row.key in self.error_keys:
            raise ValueError('Error.')
        else:
            yield row.key, row.value.upper()
