test: ; nosetests -v --with-coverage --cover-erase --cover-package=databot tests                  
ftest: ; nosetests -a !slowdb -v --with-coverage --cover-erase --cover-package=databot tests                  
