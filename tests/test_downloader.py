def test_download(bot, requests):
    url = 'http://example.com'
    requests.get(url)
    a = bot.define('a').append(url)
    b = bot.define('b')
    assert list(b(a).download().target.keys()) == [url]


def test_download_str(bot, requests):
    url = 'http://example.com'
    requests.get(url)
    assert list(bot.define('a').download(url).keys()) == [url]


def test_download_list(bot, requests):
    urls = [
        'http://example.com/1',
        'http://example.com/2',
    ]

    for url in urls:
        requests.get(url)

    assert list(bot.define('a').download(urls).keys()) == urls
