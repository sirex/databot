#!/usr/bin/env python3

import textwrap

from pathlib import Path
from collections import OrderedDict
from pprintpp import pformat

from jinja2 import Environment, FileSystemLoader
from jinja2.ext import Markup
import jinjatag

from pygments import highlight
from pygments.lexers import get_lexer_by_name
from pygments.formatters import HtmlFormatter

from databot import Bot, this, select

here = Path(__file__).parent


@jinjatag.simple_tag()
def codefile(lang, path):
    lexer = get_lexer_by_name(lang, stripall=False)
    formatter = HtmlFormatter()

    with (here / path).open() as f:
        body = f.read()

    return highlight(Markup(body).unescape(), lexer, formatter)


@jinjatag.simple_block()
def code(body, lang, sample=None):
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
        return pformat(result._eval({
            'key': 1,
            'value': {
                'title': '  Foo bar "1000"  ',
                'link': 'https://www.example.com/?q=42',
            },
        }), width=42)

    def html(self, code):
        bot = Bot()
        html = bot.define('html')
        p1 = bot.define('p1')

        with (here / 'fixtures/sample.html').open('rb') as f:
            content = f.read()

        html.append([('https://example.com/', {
            'headers': {},
            'cookies': {},
            'status_code': 200,
            'encoding': 'utf-8',
            'content': content,
        })])

        with html:
            eval(code, {}, {
                'bot': bot,
                'html': html,
                'p1': p1,
                'this': this,
                'int': int,
                'select': select,
            })

        return pformat(list(p1.data.items()), width=42)


samples = Samples()


def main():
    jinja_tag = jinjatag.JinjaTag()
    env = Environment(
        loader=FileSystemLoader(str(here)),
        extensions=[jinja_tag],
    )
    jinja_tag.init()

    formatter = HtmlFormatter(style='manni')
    template = env.get_template('index.html.j2')
    output = template.render(
        style=formatter.get_style_defs(),
        toc=OrderedDict([
            ('append', 'Appending data to pipe'),
            ('expressions', 'Lazy expressions'),
            ('html', 'HTML parsing'),
        ])
    )

    with (here / 'index.html').open('w') as f:
        f.write(output)


if __name__ == "__main__":
    main()
