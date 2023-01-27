requirements:
	pip freeze > requirements.txt

install:
	pip install -r requirements.txt

run:
	python loaddata.py dataset1.csv dataset2.csv -o report.csv