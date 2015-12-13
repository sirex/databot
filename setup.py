from setuptools import setup, find_packages

setup(
    name="databot",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'lxml',
        'sqlalchemy',
        'alembic',
        'pygments',
        'texttable',
        'cssselect',
        'tqdm',
        'toposort',
        'msgpack-python',
        'funcy',
        'beautifulsoup4',
        'requests',
    ],
)
