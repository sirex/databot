import sys
import pprint
import textwrap
import subprocess
import texttable

from pygments import highlight
from pygments.lexers import get_lexer_by_name
from pygments.formatters import Terminal256Formatter
from pygments.styles import get_style_by_name

from databot.exporters.csv import flatten_rows


class Printer(object):

    def __init__(self):
        self.isatty = sys.stdin.isatty()
        if self.isatty:
            self.height, self.width = map(int, subprocess.check_output(['stty', 'size']).split())
        else:
            self.height = 120
            self.width = 120

    def highlight(self, code, *args, **kwargs):
        if self.isatty:
            return highlight(code, *args, **kwargs)
        else:
            return code

    def print_key_value(self, key, value, short=False, exclude=None):
        style = get_style_by_name('emacs')
        formatter = Terminal256Formatter(style=style)

        py = get_lexer_by_name('python')
        html = get_lexer_by_name('html')

        exclude = exclude or []

        if 'key' not in exclude:
            if key is None or isinstance(key, (str, int)):
                print('- key: %s' % self.highlight(repr(key), py, formatter))
            else:
                code = '\n\n' + textwrap.indent(pprint.pformat(key, width=self.width), '    ')
                print('- key:')
                print(self.highlight(code, py, formatter))

        if 'value' not in exclude:
            if isinstance(value, str):
                print('  value: %s' % self.highlight(repr(value[:100]), py, formatter))
            elif isinstance(value, dict) and 'status_code' in value and 'text' in value:
                if 'headers' not in exclude:
                    print('  headers:')
                    code = textwrap.indent(pprint.pformat(value['headers'], width=self.width), '    ')
                    print(self.highlight(code, py, formatter))
                if 'cookies' not in exclude:
                    print('  cookies:')
                    code = textwrap.indent(pprint.pformat(value.get('cookies'), width=self.width), '    ')
                    print(self.highlight(code, py, formatter))
                if 'status_code' not in exclude:
                    print('  status_code: %s' % self.highlight(repr(value['status_code']), py, formatter))
                if 'encoding' not in exclude:
                    print('  encoding: %s' % self.highlight(repr(value['encoding']), py, formatter))
                if 'text' not in exclude:
                    if short:
                        print('  text: %s' % self.highlight(repr(value['text'][:100]), html, formatter))
                    else:
                        print('  text:')
                        code = textwrap.indent(value['text'], '    ')
                        print(self.highlight(code, html, formatter))
            elif value is None or isinstance(value, (int, float)):
                print('  value: %s' % self.highlight(repr(value), py, formatter))
            else:
                print('  value:')
                for key in exclude:
                    value.pop(key, None)

                code = textwrap.indent(pprint.pformat(value, width=self.width), '    ')
                print(self.highlight(code, py, formatter))

    def print_table(self, rows, exclude=None, include=None):
        _rows = []
        exclude = exclude or []
        flat_rows = flatten_rows(rows)

        for row in flat_rows:
            if include:
                _rows.append(include)
            else:
                _rows.append([c for c in row if c not in exclude])
            break

        if _rows:
            cols = row
            max_value_size = (self.width // len(_rows[0])) * 3

        if include:
            cols = {c: i for i, c in enumerate(cols) if c in include}
            cols = [cols[c] for c in include]
        else:
            cols = [i for i, c in enumerate(cols) if c not in exclude]

        for row in flat_rows:
            _row = []
            for c in cols:
                value = row[c]
                if isinstance(value, list):
                    value = repr(value)
                if isinstance(value, str) and len(value) > max_value_size:
                    value = value[:max_value_size] + '...'
                _row.append(value)
            _rows.append(_row)

        table = texttable.Texttable(self.width)
        table.set_deco(texttable.Texttable.HEADER)
        table.add_rows(_rows)
        print(table.draw())
