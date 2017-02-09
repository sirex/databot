from databot import task


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


def test_download_check(bot, requests):
    url = 'http://example.com/1'
    requests.get(url, content=b'''
        <div>
            <h1>Test</h1>
            <p>1</p>
            <p>2</p>
            <p>3</p>
            <h2></h2>
        </div>
    ''')

    source = bot.define('source').append(url)
    target = bot.define('target')
    pipe = target(source)

    tasks = [
        task('source', 'target').download(check='xpath://h1[text() = "None"]')
    ]

    bot.commands.run(tasks, limits=(0,))
    assert target.count() is 0
    assert pipe.errors.count() == 1
    assert list(pipe.errors.keys()) == [url]


def test_download_check_multiple(bot, requests):
    url = 'http://example.com/1'
    requests.get(url, content=b'''
        <div>
            <h1>Test</h1>
            <p>1</p>
            <p>2</p>
            <p>3</p>
            <h2></h2>
        </div>
    ''')

    source = bot.define('source').append(url)
    target = bot.define('target')
    pipe = target(source)

    tasks = [
        task('source', 'target').download(check='p')
    ]

    bot.commands.run(tasks, limits=(0,), error_limit=0)
    assert target.count() == 1
    assert pipe.errors.count() == 0
    assert list(target.keys()) == [url]
