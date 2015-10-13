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


Test before writing code
========================

First you need to created simplescript:

.. code-block:: python

    #!/usr/bin/env python3

    import sys
    import databot


    def define(bot):
        bot.define('pages')


    def run(bot):
        bot.compact()


    if __name__ == '__main__':
        databot.Bot('sqlite:///data/pages.db').argparse(sys.argv[1:], define, run)

Then you need to download a page and append it to defined ``pages`` pipe::

    ./script.py download '<url>' -a pages

Then you can experiment with xpath/css queries on that downloaded page::

    ./script.py select pages -k '<url>' '<css or xpath: query>'

By default ``select`` accepts CSS query, but you can mix CSS and XPATH together, for example:

    .a-css-selector xpath:a/@href

Here, ``.a-css-selector`` selectes all elements matching this CSS selector and then each result will be used to query
with ``a/@href`` XPATH query.
