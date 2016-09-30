from databot.shell import name_to_attr


def test_name_to_attr():
    assert name_to_attr('name') == 'name'
    assert name_to_attr('two words') == 'two_words'
    assert name_to_attr('lietuviškas žodis') == 'lietuviskas_zodis'
    assert name_to_attr('42 name') == '_42_name'
