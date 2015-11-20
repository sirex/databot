from setuptools import setup, find_packages

setup(
    name="databot",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'lxml',
        'sqlalchemy',
        'pygments',
        'texttable',
        'cssselect',
        'tqdm',
        'toposort',
        'msgpack-python',
    ],
)
