import pytest

from databot.db.serializers import serkey


def test_serkey():
    assert serkey(1) == 'bf8b4530d8d246dd74ac53a13471bba17941dff7'
    assert serkey('1') == 'deb12f0578a40628ec941aa2bd60d7a838765ed0'
    assert serkey([1, 2]) == 'a4001841d163db31660e03679efe46d9f99a54eb'
    assert serkey((1, 2)) == 'a4001841d163db31660e03679efe46d9f99a54eb'

    with pytest.raises(AssertionError):
        serkey({1, 2})

    with pytest.raises(AssertionError):
        serkey({1: 2})
