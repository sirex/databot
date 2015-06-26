from setuptools import setup, find_packages

setup(
    name='databot',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'SQLAlchemy',
        'prettytable',
        'tqdm',
    ],
    extras_require={
        'mysql':  ['PyMySQL'],
        'postgresql':  ['psycopg2'],
        'html':  ['beautifulsoup4', 'requests', 'lxml'],
    }
)
