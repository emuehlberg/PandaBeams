requirements:
	pip freeze > requirements.txt

install:
	pip install --upgrade pip
	pip install -r requirements.txt

run:
	python main.py

run-panda:
	python pandas_implementation.py