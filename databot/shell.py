import re

from unidecode import unidecode

name_re = re.compile(r'[^a-z0-9]+')


def name_to_attr(name):
    # Transliterate everything to ASCII
    name = unidecode(name)

    # Attributes can't start with number
    if name and name[0].isdigit():
        name = '_' + name

    # Replace all non alphanumeric characters to '_'
    name = name_re.sub('_', name)

    return name


class ShellHelper(object):

    def __init__(self, bot):
        for pipe in bot.pipes:
            name = name_to_attr(pipe.name)
            setattr(self, name, pipe)
