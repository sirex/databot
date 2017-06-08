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

from databot.exporters.utils import flatten
from databot.utils.html import get_content


class Printer(object):

    def __init__(self, models, output=None):
        self.models = models
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

    def error(self, string):
        self.print(logging.ERROR, string)

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

        cut = self.width * 8

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
                self.info('  value: %s' % self.highlight(repr(value[:cut]), py, formatter))
            elif isinstance(value, dict) and 'status_code' in value and ('content' in value):
                if 'request' not in exclude and 'request' in value:
                    self.info('  request:')
                    code = textwrap.indent(pprint.pformat(value['request'], width=self.width), '    ')
                    self.info(self.highlight(code, py, formatter))
                if 'history' not in exclude and 'history' in value:
                    self.info('  history:')
                    code = textwrap.indent(pprint.pformat(value['history'], width=self.width), '    ')
                    self.info(self.highlight(code, py, formatter))
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
                if 'content' not in exclude and 'content' in value:
                    if short:
                        self.info('  content: %s' % self.highlight(repr(value['content'][:cut]), html, formatter))
                    else:
                        self.info('  content:')
                        code = textwrap.indent(get_content(value, 'ignore'), '    ')
                        self.info(self.highlight(code, html, formatter))
                special = {'history', 'request', 'headers', 'cookies', 'status_code', 'encoding', 'content'}
                for k, v in value.items():
                    if k not in exclude and k not in special:
                        self.info('  %s: %s' % (k, self.highlight(repr(v), py, formatter)))
            elif value is None or isinstance(value, (int, float)):
                self.info('  value: %s' % self.highlight(repr(value), py, formatter))
            else:
                self.info('  value:')
                if isinstance(value, dict):
                    value = {k: v for k, v in value.items() if k not in exclude}
                code = textwrap.indent(pprint.pformat(value, width=self.width), '    ')
                self.info(self.highlight(code, py, formatter))

    def table(self, rows, exclude=None, include=None):
        _rows = []
        flat_rows = flatten(rows, exclude, include)

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
        pipes = self.models.pipes
        target = pipes.alias('target')
        pipes = {t.id: t for t in bot.pipes}
        lines = []

        lines.append('%5s  %6s %9s  %s' % ('id', '', 'rows', 'source'))
        lines.append('%5s  %6s %9s  %s' % ('', 'errors', 'left', '  target'))
        lines.append(None)
        for source in bot.pipes:
            lines.append('%5d  %6s %9d  %s' % (source.id, '', source.count(), source.name.replace(' ', '-')))

            query = sa.select([self.models.state.c.target_id]).where(self.models.state.c.source_id == source.id)
            for target_id, in bot.engine.execute(query):
                if target_id in pipes:
                    target = pipes[target_id]
                    tpipe = target(source)
                    lines.append('%5s  %6s %9d    %s' % (
                        '', tpipe.errors.count(), tpipe.count(), target.name.replace(' ', '-')
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

    def errors(self, errors, exclude=None):
        style = get_style_by_name('emacs')
        formatter = Terminal256Formatter(style=style)

        pytb = get_lexer_by_name('py3tb')

        for err in errors:
            self.key_value(err.row.key, err.row.value, exclude=exclude)
            tb = err.traceback
            tb = self.highlight(tb, pytb, formatter)
            tb = textwrap.indent(tb, '  ')
            self.info(tb)
            self.info('  Created: %s' % err.created.strftime('%Y-%m-%d %H:%M:%S'))
            self.info('  Updated: %s' % err.updated.strftime('%Y-%m-%d %H:%M:%S'))
            self.info('  Retries: %s' % err.retries)
