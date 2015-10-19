This tool currently is under development and is not ready to be used.

Quick start
===========

In this quick start guide we are going to scrape `Hacker News`_ index page and
will extract all data from that index page.

.. _Hacker News: https://news.ycombinator.com/

Them main ``databot`` object is a pipe. A pipe is just a database table with
basically key and value columns. For each pipe new table is created. ``key``
column is used to identify distinct object and ``value`` is for storing the
data.

So first thing we need to do is to define our pipes. In our case, we are going
to need two pipes, ``index`` for whole HTML code of Hacker News index page and
``news`` for storing extracted data.

Here is how our initial version of script looks:

.. code-block:: python

    #!/usr/bin/env python3

    from databot import Bot


    def define(bot):
        bot.define('index')
        bot.define('news')


    if __name__ == '__main__':
        Bot('sqlite:///hackernews.db').main(define)

Save this script under ``hackernews.py`` name, make it executable ``chmod +x
hackernews.py`` and run it::

    $ ./hackernews.py 

    id              rows  source
        errors      left    target
    ==============================
     1                 0  index
    ------------------------------
     2                 0  news
    ------------------------------

If you execute script without any arguments it just show the status of all
pipes. Now it's time to add some data to our pipes. Firs we need to add the HTML
code of index page. You can do that using following command::

    $ ./hackernews.py download 'https://news.ycombinator.com/' -a index -x text

What just happened? Here we instructed *databot* to download
``https://news.ycombinator.com/`` page and append it's content to ``index``
pipe. Since ``download`` command prints whole downloaded content, additionally
we specified ``-x text`` to not print downloaded content.

Now let's check the status output::

    $ ./hackernews.py                                                          

    id              rows  source
        errors      left    target
    ==============================
     1                 1  index
    ------------------------------
     2                 0  news
    ------------------------------

We see, that one item was added to our ``index`` pipe. We can see content of
last added item in pipe using ``show`` command::

    $ ./hackernews.py show index -x text

Again, we used ``-x text`` for the same reasons as with ``download`` command. If
you want to see content of a specific item, you can add ``key`` argument::

    $ ./hackernews.py show index 'https://news.ycombinator.com/' -x text

Now we need to extract news titles from downloaded HTML code. For this we need
to specified CSS selector or XPath query. We can check what our selector returns
before adding it to the script using ``select`` command::

    $ ./hackernews.py select index '.athing > td[3] > a:text' -t

                             key                                value 
    =================================================================
    The Hostile Email Landscape                                 None  
    On Botnets and Streaming Music Services                     None  
    Leaked Pinterest Documents Show Revenue, Growth Forecasts   None  

This command selects data using last item from ``index`` pipe as source using
``'.athing > td[3] > a:text'`` CSS selector. ``-t`` flag tels to show output as
a table instead of JSON.

As you see, ``'.athing > td[3] > a:text'``` CSS selector has two things that are
not CSS selector expressions. ``[3]`` will be expanded to ``:nth-child(3)`` and
``:text`` just takes text content of selected element. These two small
extensions to CSS selector expressions just helps to write less code.

OK, we have titles, but we need more data. Let's update our selector to get more
data::

    $ ./hackernews.py select index "[
          '.athing', (
              'td[3] > a@href', {
                  'title': 'td[3] > a:text',
                  'score': 'xpath:./following-sibling::tr[1]/td[2] css:.score:text?',
                  'time': 'xpath:./following-sibling::tr[1]/td[2]/a[2]/text()?',
                  'comments': 'xpath:./following-sibling::tr[1]/td[2]/a[3]/text()?',
              }
          )
      ]"

    - key: 'http://liminality.xyz/the-hostile-email-landscape/'

      value:
        {'comments': '112 comments',
         'score': '214 points',
         'time': '2 hours ago',
         'title': 'The Hostile Email Landscape'}

At first this might look a bit scary, but actually it is really easy to
understand. This example combines together data structure and selectors in one
place.

For example, ``['.athing', ...]`` tells *databot*, that we want list and since
this list has two items in it, it means, that first we query all ``.athing``
elements and then process each element with ``...``. In our case ``...`` is a
tuple of two elements. In other words, we are returning ``[(key, value)]``.
``key`` is a string taken by ``td[3] > a@href`` selector which is relative to
``.athing`` selected elements. ``value`` is a dict where each key of that dict
is assigned to another selector.

Basically the idea is that you can build any data structure and *databot* will
replace all selectors in that structure with real values. Also data bot expect,
that your data structure will be one of these: ``'key'``, ``['key']`` or
``['selector', ('key', 'value')]``. If you specify just ``'key'``, *databot*
checks if only one element is selected and will rise error otherwise.

As you probably mentioned, our selectors has both XPath and CSS selectors mixed
together. Usually CSS selectors are very continent to use, but they ar not
flexible enough, so in some situations you will need XPath, like in our case.

Each selector is split in parts by ``(xpath|css):`` and each part is selected
with specified selector where subsequent selector is executed on previously
selected elements.

Additionally, selectors can have ``?`` suffix, which tells, that if element is
not found, return ``None`` without raising error.

If we are satisfied with selected data, we can move these selectors to the
script. Here is how our updated script looks:

.. code-block:: python

    #!/usr/bin/env python3

    from databot import Bot


    def define(bot):
        bot.define('start urls')
        bot.define('index')
        bot.define('news')


    def run(bot):
        start_url = 'https://news.ycombinator.com/'
        with bot.pipe('start urls').append(start_url):
            with bot.pipe('index').download():
                bot.pipe('news').select([
                    '.athing', (
                        'td[3] > a@href', {
                            'title': 'td[3] > a:text',
                            'score': 'xpath:./following-sibling::tr[1]/td[2] css:.score:text?',
                            'time': 'xpath:./following-sibling::tr[1]/td[2]/a[2]/text()?',
                            'comments': 'xpath:./following-sibling::tr[1]/td[2]/a[3]/text()?',
                        }
                    )
                ])

        bot.compact()


    if __name__ == '__main__':
        Bot('sqlite:///hackernews.db').main(define, run)

*databot* uses *Python's* context managers to take data from one pipe as input
for another pipe. For example:

.. code-block:: python

    with bot.pipe('start urls'):
        bot.pipe('index').download()

Here ``index`` pipe takes ``start urls`` as input and calls ``download``
function for each row from ``start urls``. Build in ``download`` function, takes
``key`` from received row and downloads URL provided in ``key`` value.
Downloaded content is stored in ``index`` pipe.

Same thing happens with:

.. code-block:: python

    with bot.pipe('index'):
        bot.pipe('news').select(...)

This time, ``news`` pipe takes downloaded content from ``index`` pipe and
executes ``select`` build in function to extract data. All extracted data are
appended to ``news`` pipe.

One interesting this is that each pair of pipes remembers where they left last
time and when executed again, they will continue from position left last time.
That means, that you can run this script many times and only new this will be
processed.

Since all pipes are append only, at the end of script you need
``bot.compact()``, this will group all rows in each pipe by key and removes all
duplicates leaving just those added last. There is another function ``dedup()``
to remove all duplicates leaving just those added first.

Now, we have fully working scraper script and we can run it using following
command::

    $ ./hackernews.py run

    start urls -> index, rows processed: 1                                                  
    index -> news, rows processed: 1                                          

You will see nice progress bar for each pair of pipes during data processing.
After scraping is finished, you can check status::

    $ ./hackernews.py    
    id              rows  source
        errors      left    target
    ================================
     3                 1  start-urls
             0         0    news
             0         0    index
    --------------------------------
     1                 1  index
             0         0    news
    --------------------------------
     2                30  news
    --------------------------------

Also, you can check your data::

    $ ./hackernews.py tail news -t -x key

    comments     score        time                     title                                       
    ============================================================================
    discuss    18 points   5 hours ago   A 15-Year Series of Campaign Simulators                                          
    discuss    14 points   5 hours ago   The Universal Design                                                             
    discuss    13 points   4 hours ago   The History of American Surveillance                                             

And export to CSV::

    $ ./hackernews.py export news hackernews.csv

Our *databot* script works well, but sometimes ``time`` can be found not in
``xpath:./following-sibling::tr[1]/td[2]/a[2]/text()``, but in
``xpath:./following-sibling::tr[1]/td[2]/text()``. And the second case has
extra spaces at the beginning. To fix that, we can add following improvement:

.. code-block:: python

    'time': first(
        'xpath:./following-sibling::tr[1]/td[2]/a[2]/text()?',
        strip('xpath:./following-sibling::tr[1]/td[2]/text()'),
    ),

Also, we would like to see raw numbers of comments and score. To fix that we can
add following code:

.. code-block:: python

    'score': call(clean_number, 'xpath:./following-sibling::tr[1]/td[2] css:.score:text?'),
    'comments': call(clean_number, 'xpath:./following-sibling::tr[1]/td[2]/a[3]/text()?'),

See full example below.

.. code-block:: python

    #!/usr/bin/env python3

    from databot import Bot, first, strip, call


    def clean_number(value):
        value = [int(v) for v in value.split() if v.isnumeric()] if value else None
        return value[0] if value else None


    def define(bot):
        bot.define('start urls')
        bot.define('index')
        bot.define('news')


    def run(bot):
        start_url = 'https://news.ycombinator.com/'
        with bot.pipe('start urls').append(start_url):
            with bot.pipe('index').download():
                bot.pipe('news').select([
                    '.athing', (
                        'td[3] > a@href', {
                            'title': 'td[3] > a:text',
                            'score': call(clean_number, 'xpath:./following-sibling::tr[1]/td[2] css:.score:text?'),
                            'time': first(
                                'xpath:./following-sibling::tr[1]/td[2]/a[2]/text()?',
                                strip('xpath:./following-sibling::tr[1]/td[2]/text()'),
                            ),
                            'comments': call(clean_number, 'xpath:./following-sibling::tr[1]/td[2]/a[3]/text()?'),
                        }
                    )
                ])

        bot.compact()


    if __name__ == '__main__':
        Bot('sqlite:///hackernews.db').main(define, run)


Debugging
=========

In order to debug your script, you need to ``skip`` pair of pipes, set relative
offset to ``'-1'`` and then ``run`` your script with ``-d`` flag::

    $ ./script.py skip source target
    $ ./script.py offset source target '-1'
    $ ./script.py run -d

This will run only the last row and results will not be stored, since ``-d``
flag is present.
