import sys
import pprint
import textwrap
import subprocess
import texttable
import logging
import sqlalchemy as sa

from pygments import highlight
from pygments.lexers import get_lexer_by_name
from pygments.formatters import Terminal256Formatter
from pygments.styles import get_style_by_name

from databot.db import models
from databot.exporters.csv import flatten_rows


class Printer(object):

    def __init__(self, output=None):
        self.output = output
        self.isatty = False if output is None else sys.stdin.isatty()
        if self.isatty:
            self.height, self.width = map(int, subprocess.check_output(['stty', 'size']).split())
        else:
            self.height = 120
            self.width = 120

    def print(self, level, string):
        if self.output:
            print(string, file=self.output)

    def debug(self, string):
        self.print(logging.DEBUG, string)

    def info(self, string):
        self.print(logging.INFO, string)

    def highlight(self, code, *args, **kwargs):
        if self.isatty:
            return highlight(code, *args, **kwargs)
        else:
            return code

    def key_value(self, key, value, short=False, exclude=None):
        style = get_style_by_name('emacs')
        formatter = Terminal256Formatter(style=style)

        py = get_lexer_by_name('python')
        html = get_lexer_by_name('html')

        exclude = exclude or []

        if 'key' not in exclude:
            if key is None or isinstance(key, (str, int)):
                self.info('- key: %s' % self.highlight(repr(key), py, formatter))
            else:
                code = '\n\n' + textwrap.indent(pprint.pformat(key, width=self.width), '    ')
                self.info('- key:')
                self.info(self.highlight(code, py, formatter))

        if 'value' not in exclude:
            if isinstance(value, str):
                self.info('  value: %s' % self.highlight(repr(value[:100]), py, formatter))
            elif isinstance(value, dict) and 'status_code' in value and 'text' in value:
                if 'headers' not in exclude:
                    self.info('  headers:')
                    code = textwrap.indent(pprint.pformat(value['headers'], width=self.width), '    ')
                    self.info(self.highlight(code, py, formatter))
                if 'cookies' not in exclude:
                    self.info('  cookies:')
                    code = textwrap.indent(pprint.pformat(value.get('cookies'), width=self.width), '    ')
                    self.info(self.highlight(code, py, formatter))
                if 'status_code' not in exclude:
                    self.info('  status_code: %s' % self.highlight(repr(value['status_code']), py, formatter))
                if 'encoding' not in exclude:
                    self.info('  encoding: %s' % self.highlight(repr(value['encoding']), py, formatter))
                if 'text' not in exclude:
                    if short:
                        self.info('  text: %s' % self.highlight(repr(value['text'][:100]), html, formatter))
                    else:
                        self.info('  text:')
                        code = textwrap.indent(value['text'], '    ')
                        self.info(self.highlight(code, html, formatter))
            elif value is None or isinstance(value, (int, float)):
                self.info('  value: %s' % self.highlight(repr(value), py, formatter))
            else:
                self.info('  value:')
                for key in exclude:
                    value.pop(key, None)

                code = textwrap.indent(pprint.pformat(value, width=self.width), '    ')
                self.info(self.highlight(code, py, formatter))

    def table(self, rows, exclude=None, include=None):
        _rows = []
        flat_rows = flatten_rows(rows, exclude, include)

        for row in flat_rows:
            max_value_size = (self.width // len(row)) * 3
            _rows.append(row)
            break

        for row in flat_rows:
            _row = []
            for value in row:
                if isinstance(value, list):
                    value = repr(value)
                if isinstance(value, str) and len(value) > max_value_size:
                    value = value[:max_value_size] + '...'
                _row.append(value)
            _rows.append(_row)

        table = texttable.Texttable(self.width)
        table.set_deco(texttable.Texttable.HEADER)
        table.add_rows(_rows)
        self.info(table.draw())

    def status(self, bot):
        pipes = models.pipes
        target = pipes.alias('target')
        pipes = {t.id: t for t in bot.pipes}
        lines = []

        lines.append('%5s  %6s %9s  %s' % ('id', '', 'rows', 'source'))
        lines.append('%5s  %6s %9s  %s' % ('', 'errors', 'left', '  target'))
        lines.append(None)
        for source in bot.pipes:
            lines.append('%5d  %6s %9d  %s' % (source.id, '', source.data.count(), source.name.replace(' ', '-')))

            query = sa.select([models.state.c.target_id]).where(models.state.c.source_id == source.id)
            for target_id, in bot.engine.execute(query):
                if target_id in pipes:
                    target = pipes[target_id]
                    with source:
                        lines.append('%5s  %6s %9d    %s' % (
                            '', target.errors.count(), target.count(), target.name.replace(' ', '-')
                        ))

            lines.append(None)

        lenght = max(map(len, filter(None, lines)))
        border = '='
        for line in lines:
            if line is None:
                self.info(border * lenght)
                border = '-'
            else:
                self.info(line)
