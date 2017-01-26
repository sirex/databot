from databot import task


def test_watch(bot):
    a = bot.define('a')
    b = bot.define('b')

    def handler(row):
        if row.key < 16:
            yield row.key + row.key

    bot.commands.run([
        task('a').append(1),
        task('a', 'b', watch=True).call(handler),
        task('b', 'a', watch=True).call(handler),
        task('b').append(1),
    ])

    assert list(a.keys()) == [1, 4, 16, 2, 8]
    assert list(b.keys()) == [2, 8, 1, 4, 16]
