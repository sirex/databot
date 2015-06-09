import unittest
import databot


class TestBot(databot.Bot):
    db = 'sqlite:///:memory:'


class ErrorHandlingTests(unittest.TestCase):
    def test_main(self):
        error_key = '2'

        def t2(row):
            nonlocal error_key
            if row.key == error_key:
                raise ValueError('Error.')
            else:
                yield row.key, row.value.upper()

        bot = TestBot()
        bot.define('t1', None).append([('1', 'a'), ('2', 'b'), ('3', 'c')])
        bot.define('t2', t2)

        with bot.task('t1'):
            bot.task('t2').run()
            self.assertEqual(bot.task('t2').errors.count(), 1)
            self.assertEqual(list(bot.task('t2').errors.keys()), ['2'])
            self.assertEqual(list(bot.task('t2').data.items()), [('1', 'A'), ('3', 'C')])

        with bot.task('t1'):
            bot.task('t2').retry()
            self.assertEqual(bot.task('t2').errors.count(), 1)
            self.assertEqual(list(bot.task('t2').errors.keys()), ['2'])
            self.assertEqual(list(bot.task('t2').data.items()), [('1', 'A'), ('3', 'C')])

        error_key = None

        with bot.task('t1'):
            bot.task('t2').retry()
            self.assertEqual(bot.task('t2').errors.count(), 0)
            self.assertEqual(list(bot.task('t2').errors.keys()), [])
            self.assertEqual(list(bot.task('t2').data.items()), [('1', 'A'), ('3', 'C'), ('2', 'B')])
#
#
# class RetryTests(unittest.TestCase):
#     def test_retry_query(self):
#         error_key = '2'
#
#         def t2(row):
#             nonlocal error_key
#             if row.key == error_key:
#                 raise ValueError('Error.')
#             else:
#                 yield row.key, row.value.upper()
#
#         bot = TestBot()
#         bot.define('t1', None).append([('1', 'a'), ('2', 'b'), ('3', 'c')])
#         bot.define('t2', t2)
#
#         with bot.task('t1'):
#             bot.task('t2').run()
#             self.assertEqual(list(bot.task('t2').errors.keys()), ['2'])
#
#         self.assertEqual(list(bot.query_retry_tasks()))
