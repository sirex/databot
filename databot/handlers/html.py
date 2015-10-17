import re
import lxml.html
import lxml.etree
import itertools

from cssselect.parser import SelectorSyntaxError
from cssselect.xpath import ExpressionError
from bs4 import BeautifulSoup


def create_html_parser(row):
    content = row.value['text']
    parser = lxml.html.HTMLParser(encoding='utf-8')
    html = lxml.html.document_fromstring(content, parser=parser)
    html.make_links_absolute(row.key)
    return html


def create_bs4_parser(row):
    content = row.value['text']
    return BeautifulSoup(content)


class Select(object):

    def __init__(self, key, value=None):
        self.key = key
        self.value = value

    def __call__(self, row):
        self.html = create_html_parser(row)
        if isinstance(self.key, list) and self.value is None:
            return self.render(row, self.html, self.key)
        else:
            return [(self.render(row, self.html, self.key), self.render(row, self.html, self.value))]

    def render(self, row, html, value):
        if value is None:
            return None
        elif isinstance(value, Call):
            return value(row, html, self.render(row, html, value.query))
        elif callable(value):
            return value(row, html)
        elif isinstance(value, dict):
            return {k: self.render(row, html, v) for k, v in sorted(value.items())}
        elif isinstance(value, list):
            if len(value) == 2:
                query, value = value
                return [self.render(row, node, value) for node in self.select(html, query)]
            else:
                return self.select(html, value[0])
        elif isinstance(value, tuple):
            return tuple([self.render(row, html, v) for v in value])
        else:
            result = self.select(html, value)
            if len(result) == 0:
                if value.endswith('?'):
                    return None
                else:
                    if html.tag != 'html':
                        raise ValueError("'%s' did not returned any results. Context:\n\n%s" % (
                            value, lxml.etree.tostring(html, pretty_print=True).decode('utf-8')
                        ))
                    else:
                        raise ValueError("'%s' did not returned any results. Source: %s" % (value, row.key))
            elif len(result) > 1:
                raise ValueError("'%s' returned more than one value: %r." % (value, result))
            else:
                return result[0]

    def select(self, html, query):
        engines = {'xpath': self.xpath, 'css': self.cssselect}
        split_re = re.compile(r'\b(%s):' % '|'.join(engines.keys()))
        queries = filter(None, [s.strip() for s in split_re.split(query)])
        engine = self.cssselect
        result = [html]
        for subquery in queries:
            if subquery in engines:
                engine = engines[subquery]
            else:
                result = itertools.chain.from_iterable([engine(node, subquery) for node in result])
        return list(result)

    def xpath(self, html, query):
        return html.xpath(query)

    def cssselect(self, html, query):
        attr = None
        text = False
        content = False
        tail = False

        nth_child_re = re.compile(r'\[(\d+)\]')
        query = nth_child_re.sub(r':nth-child(\1)', query)

        query = query.rstrip('?')

        attr_re = re.compile(r'@([a-zA-Z0-9-_]+)$')
        match = attr_re.search(query)
        if match:
            attr = match.group(1)
            query = attr_re.sub('', query)
        elif query.endswith(':content'):
            content = True
            query = query[:-8]
        elif query.endswith(':text'):
            text = True
            query = query[:-5]
        elif query.endswith(':tail'):
            tail = True
            query = query[:-5]

        if query.startswith('/'):
            query = query[1:]
            html = self.html

        if query:
            try:
                elements = html.cssselect(query)
            except (SelectorSyntaxError, ExpressionError) as e:
                raise ValueError('Invalid selector "%s", %s' % (query, e))
        else:
            elements = [html]

        result = []
        for elem in elements:
            if attr:
                result.append(elem.get(attr))
            elif content:
                result.append(elem.text_content())
            elif text:
                result.append(elem.text)
            elif tail:
                result.append(elem.tail)
            else:
                result.append(elem)
        return result


class Call(object):

    def __init__(self, callables, query):
        self.query = query
        self.callables = callables if isinstance(callables, tuple) else (callables,)

    def __call__(self, row, node, value):
        for call in self.callables:
            value = call(value)
        return value


class Value(object):

    def __init__(self, value):
        self.value = value

    def __call__(self, row, node):
        return self.value
