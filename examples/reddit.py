#!/usr/bin/env python3

from databot import Bot, define, task, first

pipeline = {
    'pipes': [
        define('index'),
        define('news'),
    ],
    'tasks': [
        task('index').download('https://www.reddit.com/'),
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
    Bot('/tmp/reddit.db').main(pipeline)
