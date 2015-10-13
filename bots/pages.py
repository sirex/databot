#!/usr/bin/env python3

import sys
import databot


def define(bot):
    bot.define('pages')


def run(bot):
    bot.compact()


if __name__ == '__main__':
    databot.Bot('sqlite:///data/pages.db').argparse(sys.argv[1:], define, run)
