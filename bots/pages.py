#!/usr/bin/env python3

import databot


def define(bot):
    bot.define('pages')


def run(bot):
    bot.compact()


if __name__ == '__main__':
    databot.Bot('sqlite:///data/pages.db').main(define, run)
