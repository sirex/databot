from setuptools import setup, find_packages


def requirements(filename):
    with open(filename) as f:
        return [req for req in (req.partition('#')[0].strip() for req in f) if req and not req.startswith('-')]


setup(
    name="databot",
    version="0.1",
    packages=find_packages(),
    install_requires=requirements('requirements.in'),
    tests_require=requirements('requirements-dev.in'),
    entry_points={
        'console_scripts': [
            'databot = databot.main:main',
        ],
    },
)
