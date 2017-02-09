import re
import lxml.html
import lxml.etree
import itertools

from cssselect.parser import SelectorSyntaxError
from cssselect.xpath import ExpressionError
from bs4 import BeautifulSoup

from databot.utils.html import get_content
from databot.expressions.utils import handler
from databot.expressions.base import Expression, Func


def create_html_parser(row):
    content = get_content(row.value)
    parser = lxml.html.HTMLParser(encoding='utf-8')
    html = lxml.html.document_fromstring(content, parser=parser)
    if 'url' in row.value:
        html.make_links_absolute(row.value['url'])
    else:
        html.make_links_absolute(row.key)
    return html


def create_bs4_parser(row):
    content = row.value['content']
    return BeautifulSoup(content, 'lxml')


class Select(object):

    def __init__(self, key, value=None, check=True):
        """
        Parameters
        ==========
        check : bool or string
            If check is True, always make sure, that selector returns something, if check is False, it is ok, if
            selector did not returned any results.

            If check is a string, then if selector did not returned anything, do another query, to double check if we
            are string on the right page and if second query returns nothing, then raise exception.
        """
        self.key = key
        self.value = value
        self.check = check

    def __call__(self, row):
        self.set_row(row)

        if isinstance(self.key, (list, Call, Expression)) and self.value is None:
            return self.check_render(row, self.html, self.key)
        else:
            return [(self.render(row, self.html, self.key), self.check_render(row, self.html, self.value))]

    def set_row(self, row):
        if isinstance(row.value, dict) and 'content' in row.value:
            self.html = create_html_parser(row)
        else:
            self.html = None

    def check_render(self, row, html, value, **kwargs):
        result = self.render(row, html, value, **kwargs)
        if result:
            return result
        elif isinstance(result, list):
            if self.check is False:
                return result
            elif isinstance(self.check, str) and self.select(html, self.check):
                return result
            else:
                if self.value:
                    selector = (self.key, self.value)
                else:
                    selector = self.key
                raise ValueError("Select query did not returned any results. %r" % (selector,))
        else:
            return result

    def render(self, row, html, value, many=False, single=True):
        """Process provided value in given context.

        How `value` will be processed depends on `value` type.

        Parameters
        ==========
        value : None or Call or databot.rowvalue.Row or callable or dict or list or tuple or str

        row : databot.rowvalue.Row

        html : lxml.etree.Element

        many : bool
            Indicates, that multiple values can be returned.

            If many is False, only single value will be returned, if there is no matching elements or more that one
            element, ValueError will be raised.

        single : bool
            Indicates if values should be processed individually or all at once.

            This argument mainly has effect, when many is True. If many is True and single is True, then all matching
            values should be processed separately. If many is True and single is False, then list of values should be
            processed as list.

            For example, here `number` process valus one by one:

            .. code-block:: python

                @databot.func()
                def number(value):
                    return int(value)

                selector = Select([number('a@name')])

            And here, all values will be processed at once:

                @databot.func()
                def number(values):
                    return list(map(int, values))

                selector = html.Select(number(['a@name']))

        """
        if value is None:
            return None
        elif isinstance(value, Expression):
            item = value._stack[0] if value._stack and isinstance(value._stack[0], Func) else None
            name = item.name if item else None
            if name == 'select':
                expr = Expression(value._stack[1:])
                kwargs = dict(many=many, single=single)
                kwargs.update(item.kwargs)

                if item.args:
                    value = self.render(row, html, *item.args, **kwargs)
                else:
                    value = html

                if many and single:
                    return [expr._eval(v) for v in value]
                else:
                    return expr._eval(value)
            else:
                return value._eval(row)
        elif isinstance(value, Call):
            return value(self, row, html, many, single)
        elif callable(value):
            return value(row, html)
        elif isinstance(value, dict):
            return {k: self.render(row, html, v, many, single) for k, v in sorted(value.items())}
        elif isinstance(value, list):
            if len(value) == 2:
                query, value = value
                nodes = self.render(row, html, query, many=True, single=False)
                return [self.render(row, node, value) for node in nodes]
            else:
                return self.render(row, html, value[0], many=True, single=True)
        elif isinstance(value, tuple):
            return tuple([self.render(row, html, v, many, single) for v in value])
        else:
            result = self.select(html, value)

            if not isinstance(result, list):
                return result

            if many:
                return result
            elif len(result) == 0:
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
                if html.tag != 'html':
                    raise ValueError("'%s' returned more than one value: %r. Context:\n\n%s" % (
                        value, result, lxml.etree.tostring(html, pretty_print=True).decode('utf-8')
                    ))
                else:
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
                result = [engine(node, subquery) for node in result]
                result = [x if isinstance(x, list) else [x] for x in result]
                result = itertools.chain.from_iterable(result)
        return list(result)

    def xpath(self, html, query):
        query = query.rstrip('?')
        try:
            return html.xpath(query)
        except lxml.etree.XPathEvalError as e:
            raise ValueError('Invalid selector "%s", %s' % (query, e))

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


class Call:

    def __init__(self, __callables__, __query__, *args, **kwargs):
        self.query = __query__
        self.callables = __callables__ if isinstance(__callables__, tuple) else (__callables__,)
        self.args = args
        self.kwargs = kwargs

    def __call__(self, select, row, node, many=False, single=True):
        value = select.render(row, node, self.query, many, single)
        for call in self.callables:
            if many and single:
                value = [call(v, *self.args, **self.kwargs) for v in value]
            else:
                value = call(value, *self.args, **self.kwargs)
        return value


class Join(Call):

    def __init__(self, *queries):
        self.queries = queries

    def __call__(self, select, row, node, many=False, single=True):
        result = []
        for query in self.queries:
            result.extend(select.render(row, node, query, many, single))
        return result


class First(Call):

    def __init__(self, *queries):
        self.queries = queries

    def __call__(self, select, row, node, many=False, single=True):
        for query in self.queries:
            value = select.render(row, node, query, many, single=True)
            for val in value or []:
                if val:
                    return val
        return None


class Subst(Call):

    def __init__(self, query, subst, default=Exception):
        self.query = query
        self.subst = subst
        self.default = default

    def __call__(self, select, row, node, many=False, single=True):
        value = select.render(row, node, self.query, many, single)

        if self.default is Exception:
            return self.subst[value]
        elif isinstance(self.default, Expression):
            default = select.render(row, node, self.default, many, single)
            return self.subst.get(value, default)
        else:
            return self.subst.get(value, self.default)


def func(skipna=False):
    def wrapper(func):

        # Modified version of func
        def f(value, *args, **kwargs):
            if skipna:
                return None if value is None else func(value, *args, **kwargs)
            else:
                return func(value, *args, **kwargs)

        def decorator(__query__, *args, **kwargs):
            return Call(f, __query__, *args, **kwargs)

        return decorator

    return wrapper


@handler(item='method')
def text(expr, nodes, strip=True, exclude=None):
    # Recursively extract all texts
    def extract(nodes):
        texts = []
        for node in nodes:
            if node.tag is lxml.etree.Comment:
                continue
            if node.tag in ('script', 'style', 'head'):
                continue
            if node in exclude_nodes:
                continue

            texts.append(node.text)
            texts.extend(extract(node.getchildren()))
            if node.tag in ('p', 'br', 'h1', 'h2', 'h3', 'h4', 'h5'):
                texts.append('\n')
            texts.append(node.tail)
        return texts

    nodes = nodes if isinstance(nodes, list) else [nodes]

    # Find all nodes to be excluded
    exclude_nodes = []
    if exclude:
        selector = Select(None)
        for node in nodes:
            for query in exclude:
                exclude_nodes.extend(selector.select(node, query))

    # Join all texts into one single text string
    text = ' '.join(filter(None, extract(nodes)))

    # Strip all repeated whitespaces, but preserve newlines.
    if strip:
        lines = []
        for line in text.splitlines():
            words = [w.strip() for w in line.split()]
            lines.append(' '.join(filter(None, words)))
        text = '\n\n'.join(filter(None, lines))

    return text
