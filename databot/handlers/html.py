import re
import lxml.html
import itertools

from cssselect.parser import SelectorSyntaxError
from cssselect.xpath import ExpressionError
from bs4 import UnicodeDammit


class Select(object):

    def __init__(self, key, value=None):
        self.key = key
        self.value = value

    def __call__(self, row):
        content = row.value['text']
        doc = UnicodeDammit(content, is_html=True)
        encoding = doc.original_encoding
        parser = lxml.html.HTMLParser(encoding=encoding)
        html = lxml.html.document_fromstring(content, parser=parser)
        html.make_links_absolute(row.key)

        if isinstance(self.key, list) and self.value is None:
            return self.render(row, html, self.key)
        else:
            return [(self.render(row, html, self.key), self.render(row, html, self.value))]

    def render(self, row, html, value):
        if value is None:
            return None
        elif callable(value):
            return value(row, html, value)
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
                    raise ValueError("'%s' did not returned any results." % value)
            elif len(result) > 1:
                raise ValueError("'%s' returned more than one value: %r." % (value, result))
            else:
                return result[0]

    def select(self, html, query):
        engines = {'xpath': self.xpath, 'css': self.cssselect}
        split_re = re.compile(r' (%s):' % '|'.join(engines.keys()))
        queries = split_re.split(query)
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

        try:
            elements = html.cssselect(query)
        except (SelectorSyntaxError, ExpressionError) as e:
            raise ValueError('Invalid selector "%s", %s' % (query, e))

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
