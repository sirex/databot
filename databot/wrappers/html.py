def wrapper(handler, row):
    from bs4 import BeautifulSoup
    html = BeautifulSoup(row.value['text'])
    yield from handler(item, html)
