import itertools
from typing import List
import apache_beam as beam
from engines.base import DataLoader
from engines.beam.util import create_key, fillna, flatten_data, format_csv, get_sum, get_max, reduce_key, rename_column

from engines.beam.join import Join

class BeamLoader(DataLoader):

    output_suffix: str

    def __init__(self, input_files: List[str], output_file: str = 'output.csv', lineterm='\r'):
        output = output_file.split('.')
        super().__init__(input_files, output[0], lineterm)
        self.output_suffix = output[1]

    def run(self):
        with beam.Pipeline() as p:
            #upstream
            delimiter = str.encode(self.lineterm)
            row_map = lambda x: {
                'invoice_id':int(x[0]),
                'legal_entity':x[1],
                'counter_party':x[2],
                'rating':int(x[3]),
                'status':x[4],
                'value':int(x[5])
            }

            left = ( 
                p
                | "set1" >> beam.io.ReadFromText(
                        self.wide_csv,
                        coder=beam.coders.StrUtf8Coder(),
                        delimiter=delimiter,
                        skip_header_lines=True
                    )
                | "set1 split" >> beam.Map(lambda x: x.split(","))
                | 'Filter bad set1 rows' >> beam.Filter(lambda x: len(x) == 6)
                | "map set1 rows" >> beam.Map(row_map)
            )

            row_map2 = lambda x: {
                'counter_party':x[0],
                'tier':int(x[1]),
            }

            right = (
                p
                | "set2" >> beam.io.ReadFromText(
                        self.narrow_csv,
                        coder=beam.coders.StrUtf8Coder(),
                        delimiter=delimiter,
                        skip_header_lines=True
                    )
                | "set2 split" >> beam.Map(lambda x: x.split(","))
                | 'Filter bad set2 rows' >> beam.Filter(lambda x: len(x) == 2)
                | "map set2 rows" >> beam.Map(row_map2)
            )

            #full dataset
            dataset = (
                {'left': left, 'right': right}
                | 'left join' >> Join(
                        left_pcol_name="left",
                        right_pcol_name="right",
                        left_pcol=left,
                        right_pcol=right,
                        join_type='left',
                        join_keys={'left': ['counter_party'], 'right': ['counter_party']}
                    )
            )
            
            streams = []
            for idx_group in self.groupings:
                group_name = '_'.join(idx_group)
                max_values = (
                    dataset
                    | f'max key - {group_name}' >> beam.Map(lambda row, idx_key: (create_key(row, idx_key), row), idx_key=idx_group)
                    | f'get max - {group_name}' >> beam.CombinePerKey(get_max)
                    | f'flatten max - {group_name}' >> beam.Map(lambda group: flatten_data(group))
                )

                sum_arap = (
                    dataset
                    | f'filter arap - {group_name}' >> beam.Filter(lambda row: row['status']=='ARAP')
                    | f'arap key - {group_name}' >> beam.Map(lambda row, idx_key: (create_key(row, idx_key), row), idx_key=idx_group)
                    | f'get arap - {group_name}' >> beam.CombinePerKey(get_sum)
                    | f'flatten arap - {group_name}' >> beam.Map(lambda group: flatten_data(group))
                    | f'rename arap - {group_name}' >> beam.Map(lambda row: rename_column(row, 'value', 'sum_arap'))
                )

                sum_accr = (
                    dataset
                    | f'filter accr - {group_name}' >> beam.Filter(lambda row: row['status']=='ACCR')
                    | f'accr key - {group_name}' >> beam.Map(lambda row, idx_key: (create_key(row, idx_key), row), idx_key=idx_group)
                    | f'sum accr - {group_name}' >>  beam.CombinePerKey(get_sum)
                    | f'flatten accr - {group_name}' >> beam.Map(lambda group: flatten_data(group))
                    | f'rename accr - {group_name}' >> beam.Map(lambda row: rename_column(row, 'value', 'sum_accr'))
                )
                mergeset = (
                    {'left': max_values, 'right': sum_accr}
                    | f'left join max - {group_name}' >> Join(
                            left_pcol_name="left",
                            right_pcol_name="right",
                            left_pcol=left,
                            right_pcol=right,
                            join_type='left',
                            join_keys={'left': ['key'], 'right': ['key']}
                        )
                )
                final = (
                    {'left': mergeset, 'right': sum_arap}
                    | f'final merge - {group_name}' >> Join(
                            left_pcol_name="left",
                            right_pcol_name="right",
                            left_pcol=left,
                            right_pcol=right,
                            join_type='left',
                            join_keys={'left': ['key'], 'right': ['key']}
                        )
                    | f'fillna - {group_name}' >> beam.Map(lambda row: fillna(row))
                    | f'reduce key - {group_name}' >> beam.Map(lambda row: reduce_key(row))
                    | f'format - {group_name}' >> beam.Map(lambda row: format_csv(row))
                )
                streams.append(final)

            output = (
                streams
                | beam.Flatten()
                | f'write' >> beam.io.WriteToText(
                        self.output_file,
                        file_name_suffix=self.output_suffix,
                        header='legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)',
                        shard_name_template='',
                        append_trailing_newlines=True
                    )
            )

            res = p.run()
            res.wait_until_finish()
