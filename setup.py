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
        'freezegun',
        'unidecode',
        'mock',
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'pytest-mock',
        'requests-mock',
        'freezegun',
    ],
    entry_points={
        'console_scripts': [
            'databot = databot.main:main',
        ],
    },
)
