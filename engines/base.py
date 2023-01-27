from typing import List

from shared import INDICES, get_groupings


class DataLoader:

    wide_csv: str
    narrow_csv: str
    output_file: str
    lineterm: str
    groupings: List[str]

    def __init__(self, input_files: List[str], output_file:str = 'output.csv', lineterm='\r'):
        self.output_file = f'{self.__class__.__name__}-{output_file}'
        self.lineterm = lineterm
        self.init_input(input_files)
        self.groupings = get_groupings(INDICES)

    def init_input(self, input_files: List[str]):
        for filename in input_files:
            with open(filename) as file:
                header = file.readline()
                data = header.split(',')
                if len(data) == 6:
                    self.wide_csv = filename
                elif len(data) == 2:
                    self.narrow_csv = filename

    def run(self):
        raise Exception("Run not implemented")
