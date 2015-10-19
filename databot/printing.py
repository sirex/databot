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
        if sys.stdin.isatty():
            self.height, self.width = map(int, subprocess.check_output(['stty', 'size']).split())
        else:
            self.height = 120
            self.width = 120

    def print_key_value(self, key, value, short=False, exclude=None):
        style = get_style_by_name('emacs')
        formatter = Terminal256Formatter(style=style)

        py = get_lexer_by_name('python')
        html = get_lexer_by_name('html')

        exclude = exclude or []

        if 'key' not in exclude:
            if isinstance(value, str):
                print('- key: %s' % highlight(repr(key), py, formatter))
            else:
                code = '\n\n' + textwrap.indent(pprint.pformat(key, width=self.width), '    ')
                print('- key:')
                print(highlight(code, py, formatter))

        if 'value' not in exclude:
            if isinstance(value, str):
                print('  value: %s' % highlight(repr(value[:100]), py, formatter))
            elif isinstance(value, dict) and 'status_code' in value and 'text' in value:
                if 'headers' not in exclude:
                    print('  headers:')
                    code = textwrap.indent(pprint.pformat(value['headers'], width=self.width), '    ')
                    print(highlight(code, py, formatter))
                if 'cookies' not in exclude:
                    print('  cookies:')
                    code = textwrap.indent(pprint.pformat(value.get('cookies'), width=self.width), '    ')
                    print(highlight(code, py, formatter))
                if 'status_code' not in exclude:
                    print('  status_code: %s' % highlight(repr(value['status_code']), py, formatter))
                if 'encoding' not in exclude:
                    print('  encoding: %s' % highlight(repr(value['encoding']), py, formatter))
                if 'text' not in exclude:
                    if short:
                        print('  text: %s' % highlight(repr(value['text'][:100]), html, formatter))
                    else:
                        print('  text:')
                        code = textwrap.indent(value['text'], '    ')
                        print(highlight(code, html, formatter))
            elif value is None or isinstance(value, (int, float)):
                print('  value: %s' % highlight(repr(value), py, formatter))
            else:
                print('  value:')
                for key in exclude:
                    value.pop(key, None)

                code = textwrap.indent(pprint.pformat(value, width=self.width), '    ')
                print(highlight(code, py, formatter))

    def print_table(self, rows, exclude=None):
        _rows = []
        exclude = exclude or []
        flat_rows = flatten_rows(rows)

        for row in flat_rows:
            _rows.append([c for c in row if c not in exclude])
            break

        if _rows:
            cols = row
            max_value_size = (self.width // len(_rows[0])) * 3

        for row in flat_rows:
            _row = []
            for col, value in enumerate(row):
                if cols[col] in exclude:
                    continue
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
