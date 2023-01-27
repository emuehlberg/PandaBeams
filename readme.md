## Requirements
- Make
- Python3

## Setup

Install requirements using `pip install -r requirements.txt` or `Make install`

## Running

`python3 loaddata.py dataset1.csv dataset2.csv -o report.csv`

or 

replace `dataset1.csv` and `dataset2.csv` with the desired dataset files and use `Make run`

## Areas to improve

- Panda is unable to properly handle custom line term at the moment
- Could expand to scan for header size on each file and build a 'virtual table' for auto mapping and auto joins based on common headers
- Report customization
- Better abstraction and reduction in code duplication
- Unit tests around utils
- Integration tests on engines