.PHONY: env
env: env/bin/py.test

.PHONY: test
test: env/bin/py.test
	env/bin/py.test -vvxra -W error:::alembic.util.messaging --tb=native --cov-report=term-missing --cov=databot tests

env/bin/py.test: env/bin/pip requirements-dev.txt
	env/bin/pip install -r requirements-dev.txt -e .

env/bin/pip:
	python -m venv env
	env/bin/pip install --upgrade pip setuptools

requirements-dev.txt: env/bin/pip-compile requirements.in requirements-dev.in
	env/bin/pip-compile requirements.in requirements-dev.in -o requirements-dev.txt
	env/bin/pip-compile requirements.in -o requirements.txt

env/bin/pip-compile: env/bin/pip
	env/bin/pip install pip-tools
