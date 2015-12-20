import ast


def parse(value):
    if not isinstance(value, (str, bytes)):
        return value
    try:
        return ast.literal_eval(value)
    except ValueError:
        return value
