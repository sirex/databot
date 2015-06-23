def wrapper(handler, row):
    from bs4 import BeautifulSoup
    if isinstance(row.value, dict) and 'text' in row.value:
        html = BeautifulSoup(row.value['text'])
    else:
        html = BeautifulSoup(row.value)
    yield from handler(row, html)
