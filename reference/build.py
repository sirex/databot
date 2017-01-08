#!/usr/bin/env python3

import pprint
import textwrap

from pathlib import Path
from collections import OrderedDict

from jinja2 import Environment, FileSystemLoader
from jinja2 import nodes
from jinja2.ext import Extension, Markup

from pygments import highlight
from pygments.lexers import get_lexer_by_name
from pygments.formatters import HtmlFormatter

from databot import Bot, this


class HighlightExtension(Extension):
    tags = set(['code'])

    def parse(self, parser):
        lineno = next(parser.stream).lineno
        args = [parser.parse_expression()]
        if parser.stream.skip_if('comma'):
            args.append(parser.parse_expression())
        else:
            args.append(nodes.Const(None))
        body = parser.parse_statements(['name:endcode'], drop_needle=True)
        return nodes.CallBlock(self.call_method('_highlight', args), [], [], body).set_lineno(lineno)

    def _highlight(self, lang, sample, caller=None):
        # highlight code using Pygments
        body = caller()
        lexer = get_lexer_by_name(lang, stripall=False)
        formatter = HtmlFormatter()
        body = textwrap.dedent(body.rstrip())

        sample = getattr(samples, sample, None)
        if sample:
            result = sample(body.strip())
        else:
            result = None

        html = highlight(Markup(body).unescape(), lexer, formatter)

        if result:
            formatter = HtmlFormatter(cssclass='output')
            html += highlight(Markup(result).unescape(), lexer, formatter)

        return html


class Samples:

    def append(self, code):
        bot = Bot()
        p1 = bot.define('p1')
        eval(code, {}, {
            'bot': bot,
            'p1': p1,
        })
        return repr(list(p1.data.items()))

    def expressions(self, code):
        result = eval(code, {}, {
            'this': this,
            'int': int,
        })
        return pprint.pformat(result._eval({
            'key': 1,
            'value': {
                'title': 'Foo bar "1000"',
                'link': 'https://www.example.com/?q=42',
            },
        }), width=42)


samples = Samples()


def main():
    here = Path(__file__).parent

    env = Environment(
        loader=FileSystemLoader(str(here)),
        extensions=[HighlightExtension],
    )

    formatter = HtmlFormatter(style='manni')
    template = env.get_template('index.html.j2')
    output = template.render(
        style=formatter.get_style_defs(),
        toc=OrderedDict([
            ('append', 'Appending data to pipe'),
            ('expressions', 'Lazy expressions'),
        ])
    )

    with (here / 'index.html').open('w') as f:
        f.write(output)


if __name__ == "__main__":
    main()
