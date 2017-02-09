import bs4
import cgi


def get_content(data, errors='strict'):
    headers = {k.lower(): v for k, v in data.get('headers', {}).items()}
    content_type_header = headers.get('content-type', '')
    content_type, params = cgi.parse_header(content_type_header)
    if content_type.lower() in ('text/html', 'text/xml'):
        soup = bs4.BeautifulSoup(data['content'], 'lxml', from_encoding=data['encoding'])
        return data['content'].decode(soup.original_encoding, errors)
    elif content_type.startswith('text/'):
        return data['content'].decode(data['encoding'], errors)
    else:
        return data['content']
