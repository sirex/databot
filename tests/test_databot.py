import unittest
import databot


class DatabotTests(unittest.TestCase):
    def test_main(self):

        class TestBot(databot.Bot):
            db = 'sqlite:///:memory:'

            def task_t2(self, row):
                yield row.key, row.value.upper()

            def init(self):
                self.define('t1', None)
                self.define('t2')

            def run(self):
                with self.task('t1').append([('1', 'a'), ('2', 'b'), ('3', 'c')]):
                    self.task('t2').run()

        bot = TestBot()
        bot.main()

        self.assertEqual(list(bot.task('t1').data.items()), [('1', 'a'), ('2', 'b'), ('3', 'c')])
        self.assertEqual(list(bot.task('t2').data.items()), [('1', 'A'), ('2', 'B'), ('3', 'C')])
