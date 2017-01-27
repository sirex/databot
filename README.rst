This tool currently is under development and is not ready to be used.

`Reference documentation
<https://rawgit.com/sirex/databot/master/reference/index.html>`_

.. contents::


Quick start
===========

In this quick start guide we are going to scrape Reddit_ index page and will
extract all data from index page.

.. _Reddit: https://news.ycombinator.com/

Just open a `Jupyter notebook`_ or any text editor and paste this code:

.. _Jupyter notebook: https://jupyter.org/

.. code-block:: python

    import databot

    bot = databot.Bot('/tmp/reddit.db')

    index = bot.define('index').download('https://www.reddit.com/')

    bot.commands.select(index, table=True, query=[
        '.thing.link', (
            '.entry .title > a@href', {
                'title': '.entry .title > a:text',
                'score': '.midcol .score.likes@title',
                'time': databot.first(['.tagline time@datetime']),
                'comments': '.entry a.comments:text',
            }
        )
    ])

Last method call will output the data::

    key                   comments       score  time                  title
    ========================================================================================
    http://i.imgur.com/Y  704 comments   20592  2017-01-05T06:57:31+  Reflex level: German
    cX7DND.gifv                                 00:00                 Solider
    https://youtu.be/hNw  567 comments   5494   2017-01-05T08:16:49+  If you haven't seen
    ORVbNN-s                                    00:00                 it, the 2016
                                                                      Japanese Godzilla
                                                                      Resurgence is an
                                                                      amaz...
    https://www.reddit.c  1001 comments  8032   2017-01-05T06:34:41+  CALL TO ARMS #4 -
    om/r/UpliftingNews/c                        00:00                 LET'S SHOW THE
    omments/5m4uuw/call_                                              CHICAGO VICTIM SOME
    to_arms_4_le...                                                   LOVE


Now dig deeper into the code.


.. code-block:: python

    bot = databot.Bot('/tmp/reddit.db')

Here we define ``Bot`` object and tell where all the data should be stored. In
this case we simple pass a path to the Sqlite database. PostgreSQL and MySQL
databases are supported too, `just give a dsn
<http://docs.sqlalchemy.org/en/rel_1_1/core/engines.html#database-urls>`_
instead of a path.

.. code-block:: python

    index = bot.define('index').download('https://www.reddit.com/')

Here we define a new pipe called ``index``, then download
``https://www.reddit.com/`` page and store it in ``index`` pipe.

A pipe is just a database table with basically key and value columns.

When we download a page and store in into a pipe, key for will be downloaded
page url and value will be content with metadata.

.. code-block:: python

    bot.commands.select(index, table=True, query=[
        '.thing.link', (
            '.entry .title > a@href', {
                'title': '.entry .title > a:text',
                'score': '.midcol .score.likes@title',
                'time': databot.first(['.tagline time@datetime']),
                'comments': '.entry a.comments:text',
            }
        )
    ])

Once we have some HTML stored in a pipe, we can extract data from it using
``select`` function.

Query can be a list, dict, tuple or string. All strings are css selectors with
some syntactic sugar added on top of it. Lists, dicts and tuples are used to
define structure of extracted data.

Here is a quick reference::

    str: 'css/xpath selector (expects single item)'

    tuple: (<key query>, <value query>)

    dict: {<field>: <query>}

    list: [<query a list container>, <query an item in the container>]

    list: [<query (expects multiple items)>]

So in our case, query is a list ``[]``, it means, that we expect list of items.
Since our list has two items in it, first item ``.thing.link`` is selector that
points to a container and second item is a tuple. A tuple can be only at the
top level of query and it expects two selectors, one for key and other for
value.

As I said before, pipes (or tables) have only key and value for storing data.
So we always have to provide key and value.

In our case key is ``.entry .title > a@href``, and value is a dict. Keep in
mind, that all queries inside list of two items are relative to element
selected by first item of that list.

It is a good idea to use key values, that uniquely identify object that is
being scraped.

css/xpath expressions have these syntactic sugar additions:

- ``selector[1]`` - expands to ``selector:nth-child(1)``.

- ``selector?`` - it is OK if there is no elements matching this selector,
  ``None`` will be returned.

- ``selector:text`` - take text part of selected element.

- ``selector@attr`` - take attribute value of selected element.

- ``selector:content`` - extract text content of selected element and all his
  descendants.

- ``xpath:selector`` - switch from css selector to xpath selector.

- ``selector xpath:selector css:selector`` - start with css selector then
  switch to xpath and then back to css. Each subsequent is relative to previous
  one. Unless selector starts with ``/``.


Writing scraper bot scripts
===========================

Example provided in quick start is good if you want to play with it in an
interactive Python console, but if you want to run this scraper many times, it
is better to move it to a script.

Here is how previous example can be transformed into a script:

.. code-block:: python

    #!/usr/bin/env python3

    from databot import define, task, first

    pipeline = {
        'pipes': [
            define('index'),
            define('news'),
        ],
        'tasks': [
            task('index').once().download('https://www.reddit.com/'),
            task('index', 'news').select([
                '.thing.link', (
                    '.entry .title > a@href', {
                        'title': '.entry .title > a:text',
                        'score': '.midcol .score.likes@title',
                        'time': first(['.tagline time@datetime']),
                        'comments': '.entry a.comments:text',
                    }
                )
            ]),
            task('news').export('/tmp/reddit.jsonl'),
            task().compact(),
        ],
    }

    if __name__ == '__main__':
        databot.Bot('/tmp/reddit.db').main(pipeline)



Save this script under ``reddit.py`` name, make it executable ``chmod +x
reddit.py`` and run it::

    $ ./reddit.py 
    id              rows  source
        errors      left    target
    ==============================
     1                 0  index
    ------------------------------
     2                 0  news
    ------------------------------

When you run this script without any parameters it shows status of all your
pipes.

To do the scraping use ``run`` subcommand::

    $ ./reddit.py run
    index -> news: 100%|█████████████████| 1/1 [00:00<00:00,  4.94it/s]

If you will check status again you will see following output::

    $ ./reddit.py 
    id              rows  source
        errors      left    target
    ==============================
     1                 1  index
             0         0    news
    ------------------------------
     2                35  news
    ------------------------------

It shows that ``index -> news`` does not have any errors and all items are
processed. Also we see, than we have 1 row in ``index`` pipe and 35 rows in
``news`` pipe.

You can inspect content of pipes using ``tail`` or ``show`` commands::

    $ ./reddit.py tail news -t -x key,title -n 5
      comments      score             time            
    =================================================
    717 comments    25194   2017-01-05T16:37:01+00:00 
    533 comments    9941    2017-01-05T17:34:22+00:00 
    1111 comments   26383   2017-01-05T16:19:22+00:00 
    1122 comments   9813    2017-01-05T17:33:36+00:00 
    832 comments    7963    2017-01-05T16:58:55+00:00 

    $ ./reddit.py show news -x title
    - key: 'https://www.reddit.com/r/DIY/comments/5m7ild/hi_reddit_greetings_from_this_old_house/'

      value:
        {'comments': '832 comments',
         'score': '7963',
         'time': '2017-01-05T16:58:55+00:00'}

Since we exported structured data here:

.. code-block:: python

    news.export('/tmp/reddit.jsonl')

We can use any tool to work with the data, for example::

    $ tail -n1 /tmp/reddit.jsonl | jq .
    {
      "key": "https://www.reddit.com/r/DIY/comments/5m7ild/hi_reddit_greetings_from_this_old_house/",
      "comments": "832 comments",
      "time": "2017-01-05T16:58:55+00:00",
      "score": "7963",
      "title": "Hi Reddit! Greetings from THIS OLD HOUSE."
    }

How does it work?
=================

*databot* uses *Python's* context managers to take data from one pipe as input
for another pipe. For example:

.. code-block:: python

    with index.download('https://www.reddit.com/'):
        news.select(...)

Here ``news`` pipe takes downloaded content from ``index`` pipe and executes
``select`` method to extract data. All extracted data are appended to the
``news`` pipe.

One interesting point is that each pair of pipes remembers where they left last
time and when executed again, they will continue from position left last time.
That means, that you can run this script many times and only new items will be
processed.

Pipeline warm up
================

Databot executes each task one by one. Each task will process all unprocessed
items and only then new task begins.

If you have a lot of data to process, usually you would like to test all tasks
with several items, and when all tasks where tested, then run tasks one by one
with all items.

By default, databot runs all tasks limiting number of items for each task to
one, and once whole pipeline is run, then continue running all tasks again
with all items. This is sort of pipeline warm up.

This way, if one of your tasks fails, you will see it immediately.

Pipeline warm up can be controlled with ``--limit`` flag, by default it is
``--limit=1,0``, where ``1`` means, run each task with single item, and ``0``
means, run each tasks with all items.

You can specify different warm up strategy, for example ``--limit=0`` means run
all items without warming up. Another example ``--limit=1,5,10,100,0``, this
will run bot with ``1``, ``5``, ``10``, ``100`` items to warm up, and then
continues with all other items.

Since your pipeline will be run multiple times, some times you want to control
how often you want a task to run. For example, usually you start a pipeline
with a task, that downloads a starting page:

.. code-block:: python

  task('index').download('https://www.reddit.com/'),

But since pipeline can be executed multiple times, you want to make sure, than
starting page will be downloaded only once. To do that, use ``once()`` method
call, like this:

.. code-block:: python

  task('index').once().download('https://www.reddit.com/'),

Now starting page will be downloaded only the first time. All subsequent
pipline reruns will do nothing.


Pagination
==========

You can scrape web pages that use pagination using watch functionality.

.. code-block:: python

    'tasks': [
        task('listing pages').once().download('https://example.com'),
        task('listing pages', 'listing urls', watch=True).select(['.pagination a.page@href']).dedup(),
        task('listing urls', 'listing pages', watch=True).download(),
    ],

All tasks, that have ``watch=True`` flag, will be run multiple times if source
pipe gets new data to process. In this case, when all pages are downloaded for
extracted  urls in third task, second task will will run again and populates
'listing urls' with new urls, then third tasks will run again and downloads
pages from new urls. And this will continue, until there is not urls left to
extract.


Continuous scraping
===================

Databot is built with continuous scraping in mind. Each pipeline should be
runnable multiple times. For this to work, databot offers some utility methods
to control when a task should be run.

``task('x').once()`` - runs only once per run. If your run a pipeline with
multiple limit rounds, then all ``once()`` tasks will be run only the first
time.

``task('x').daily()``, ``task('x').weekly()``, ``task('x').monthly()`` - runs
task only if last entry in the pipeline is older than specified.

``task('x').freq(<datetime.timedelta>)`` or ``task('x').freq(seconds=0,
minutes=0, hours=0, days=0, weeks=0)`` - for more detailed frequency control.

It is enough to specify these time restrictions for initial tasks, all other
tasks, that use initial pipes as source, will wait while new data will be
provided.


Error handling
==============

By default, when you ``run`` your bot, all errors are stored in errors table
with possibility to retry all items by running ``retry`` command.

But sometimes it is a good idea to limit number of error with ``run -f`` flag.
``-f`` without argument will stop scraping on first error. It means, that if
you run ``run -f`` again, *databot* will continue where it left.

You can specify number of errors with ``run -f 10``, here scraping will stop
after 10th error.

If you run but with limited number of items per task (``--limit`` flag), then
if not specified, ``-f`` flag will be turned on for each non-zero limit round.
When you specify limit rounds, it is expected, than you wan to test you
pipeline, before running all items per tasks. When testing, usually you want to
get error as soon as possible. That's why ``-f`` is turned of by default if you
use limit rounds.

Limiting number of errors is good idea in situations, when server starts to
block *databot* after some time, in that case there is no point in trying to
scrape more items, since error will be the same for all items.

In order to inspect what errors where recorded you can use ``errors <pipe>``
command. It will print whole source item and nice Python traceback. If source
item is downloaded html page it is good idea to run ``errors <pipe> -x
content``. This will suppress HTML content from output.


Debugging
=========

In order to debug your script, you need to ``skip`` pair of pipes, set relative
offset to ``'-1'`` and then ``run`` your script with ``-d`` flag::

    $ ./script.py skip source target
    $ ./script.py offset source target '-1'
    $ ./script.py run -d

This will run only the last row and results will not be stored, since ``-d``
flag is present.


Multi database support
======================

If you are using SQLite as your database backend, all data of all pipes are
stored in single file. This file can grow really big. You can split some pipes
into different databases. To do that, you just need to specify different
database connection string, when defining pipes:

.. code-block:: python

    def define(bot):
        bot.define('external', 'sqlite:///external.db')
        bot.define('internal')


Now you can use ``external`` pipe same way as internal and data will live in
external database.

Multiple different bots, can access same external pipe and use or update it's
data.


Interactive shell
=================

You can access your databot object using interactive shell::

    $ ./hackernews.py sh

Renaming pipes
==============

Since pipes are defined both on database and in code, you can't just rename it
in code. Renaming bot just in code will create new pipe with new name, leaving
old as is.

To rename it in database you need to execute following command::

    $ ./hackernews.py rename 'old name' 'new name'


Compressing pipe data
=====================

Some times you want to compress some pipes, especially those, containing HTML
pages. Compressing HTML pages can save `up to 3 times of disk space
<https://quixdb.github.io/squash-benchmark/#results>`_.

You can specify compression level like this:

.. code-block:: python

    bot.define('html-pages', compress=True)

If you specify ``compress=True``, only new entries will be compressed. In order
to compress existing entries, run following command::

    $ ./bot.py compress html-pages

Also you can decompress existing data::

    $ ./bot.py decompress html-pages

After compressing existing data, Sqlite file size stays same as before, in
order for compression to take effect you need to vacuum you Sqlite database
using this command::

    $ sqlite3 path/to/sqlite.db vacuum

``vacuum`` command requires as much as `twice the size
<https://www.sqlite.org/lang_vacuum.html>`_ of the original database file of
free disk space.

Manual access to the data
=========================

Small example below demonstrates how to access pipe data manually, without
using ``databot`` library:

.. code-block:: python

  import msgpack
  import sqlalchemy as sa


  def get_table(engine, db, name):
      pipe = db.tables['databotpipes']
      query = sa.select([pipe.c.id], pipe.c.pipe == name)
      table_id = engine.execute(query).scalar()
      return db.tables['t%d' % table_id]


  def query_rows(engine, table):
      query = sa.select([table.c.value])
      for row in engine.execute(query):
          value = gzip.decompress(row.value) if row.compression == 1 row.value
          yield msgpack.loads(value, encoding='utf-8')


  def main():
      dbpath = '/path/to/data.db'
      engine = sa.create_engine('sqlite:///%s' % dbpath)
      db = sa.MetaData()
      db.reflect(bind=engine)

      for key, value in query_rows(engine, get_table(engine, db, 'mypipe')):
          print(key, value)

As you see data storage format is pretty simple.


Running tests
=============

Install dependencies::

    pip install -e .
    pip install -r reference/requirements.txt

Run tests::

    py.test --cov-report=term-missing --cov=databot tests
