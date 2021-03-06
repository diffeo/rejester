
.PHONY: clean test build

clean: 
	python setup.py clean --all
	rm -rf build dist rejester.egg-info runtests.py

test: install
	python setup.py install_test
	py.test --genscript=runtests.py
	cd src && python ../runtests.py --runperf -vvs -n 8 tests

build: clean
	python setup.py build bdist_egg sdist

install: build
	pip install .

register:
        ## upload both source and binary
	python setup.py sdist bdist_egg upload

check:
	pylint -i y --output-format=parseable src/`git remote -v | grep origin | head -1 | cut -d':' -f 2 | cut -d'.' -f 1`

