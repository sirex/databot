import io
import databot

from databot.main import main


def test_main(tmpdir):
    maindb = str(tmpdir / 'main.db')

    bot = databot.Bot(maindb)
    bot.define('p1').append([(1, 'a')])
    bot.define('p2', str(tmpdir / 'external.db')).append([(2, 'b')])

    output = io.StringIO()
    main([maindb], output)

    assert output.getvalue().splitlines() == [
        '   id              rows  source',
        '       errors      left    target',
        '=================================',
        '    1                 1  p1',
        '---------------------------------',
    ]
