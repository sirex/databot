import databot
import tests.db


@tests.db.usedb()
class DatabotTests(object):
    def test_main(self):
        def define(bot):
            bot.define('t1')
            bot.define('t2')

        def run(bot):
            with bot.pipe('t1').append([('1', 'a'), ('2', 'b'), ('3', 'c')]):
                bot.pipe('t2').call(lambda row: [(row.key, row.value.upper())])

        bot = databot.Bot(self.db.engine).argparse(['-v0', 'run'], define, run)

        self.assertEqual(list(bot.pipe('t1').data.items()), [('1', 'a'), ('2', 'b'), ('3', 'c')])
        self.assertEqual(list(bot.pipe('t2').data.items()), [('1', 'A'), ('2', 'B'), ('3', 'C')])
