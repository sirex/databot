def test_main(db):
    def define(bot):
        bot.define('t1')
        bot.define('t2')

    def run(bot):
        with bot.pipe('t1').append([('1', 'a'), ('2', 'b'), ('3', 'c')]):
            bot.pipe('t2').call(lambda row: [(row.key, row.value.upper())])

    bot = db.Bot().main(define, run, argv=['-v0', 'run'])

    assert list(bot.pipe('t1').data.items()) == [('1', 'a'), ('2', 'b'), ('3', 'c')]
    assert list(bot.pipe('t2').data.items()) == [('1', 'A'), ('2', 'B'), ('3', 'C')]
