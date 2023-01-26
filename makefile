requirements:
	pip freeze > requirements.txt

install:
	pip install -r requirements.txt

run:
	python main.py

run-panda:
	python pandas_implementation.py

run-beam:
	python beam_implementation.py

viola:
	make install
	make run