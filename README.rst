This tool currently is under development and is not ready to be used.

How to use
==========

First you need to write your script, that looks something like this:

.. code-block:: python

    #!/usr/bin/env python3

    import sys
    import databot


    def define(bot):
        bot.define('index urls')
        bot.define('index pages')


    def run(bot):
        self.compact()

        start_url = 'http://opendata.gov.lt/index.php?vars=/public/public/search'
        with self.pipe('index urls').append(start_url):
            with self.pipe('index pages').download():
                self.pipe('index urls').select(['td > a.path@href']).dedup()


    if __name__ == '__main__':
        databot.Bot('sqlite:///data/script.db').argparse(sys.argv[1:], define, run)


Before running your script, probably you would like to tesk how it works::

    script.py try 1 append 'http://opendata.gov.lt/index.php?vars=/public/public/search'     

    script.py try 1 dowload

    bots/rinkimai.py try 1 select 'td > a.path@href'     

If all that works, then you can run your script::

    ./script.py run

And show status information::

    ./script.py

If something failed, retry failed items and turn on debugging mode::

    ./script.py run --retry -d                                                                            

If everyting works, you can rerun your script later and it will fetches only
what was changed::

    ./script.py run
