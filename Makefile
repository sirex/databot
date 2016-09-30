# pip install pytest pytest-mock pytest-cov
test: ; py.test -vvxra --tb=native --cov-report=term-missing --cov=databot tests
