from databot.utils.urls import url


def test_url_domains():
    assert url('http://example.com', domains='example.com') == 'http://example.com'
    assert url('http://www.example.com', domains='example.com') is None
    assert url('http://example.com', domains='www.example.com') is None
    assert url('http://example.com', domains=['example.com', 'www.example.com']) == 'http://example.com'


def test_url_query():
    assert url('http://example.com?') == 'http://example.com'
    assert url('http://example.com?a=1&b=2') == 'http://example.com?a=1&b=2'
    assert url('http://example.com?b=2&a=1') == 'http://example.com?a=1&b=2'
    assert url('http://example.com?b=2&a=1', query='a') == 'http://example.com?a=1'
    assert url('http://example.com?b=2&a=1', query=['a']) == 'http://example.com?a=1'


def test_url_fragment():
    assert url('http://example.com#') == 'http://example.com'
    assert url('http://example.com#foobar') == 'http://example.com'
