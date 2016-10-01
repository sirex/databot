def test_download(bot, requests):
    url = 'http://example.com'
    requests.get(url)
    with bot.define('a').append(url):
        assert list(bot.define('b').download().data.keys()) == [url]


def test_download_str(bot, requests):
    url = 'http://example.com'
    requests.get(url)
    assert list(bot.define('a').download(url).data.keys()) == [url]


def test_download_list(bot, requests):
    urls = [
        'http://example.com/1',
        'http://example.com/2',
    ]

    for url in urls:
        requests.get(url)

    assert list(bot.define('a').download(urls).data.keys()) == urls
