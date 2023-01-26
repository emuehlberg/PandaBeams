import itertools
import apache_beam as beam

from join import Join
from shared import INDICES, get_groupings

def get_max(rows):
    max = None
    for row in rows:
        if max is None:
            max = row
            continue
        if max['rating'] < row['rating']:
            max = row
    if max is None:
        return {'rating': 0}
    return {
        'rating': max['rating']
    }

def get_sum(rows):
    total = 0
    for row in rows:
        if 'value' not in row:
            return row
        total = total + row['value']
    return {'value': total}

def rename_column(row, old, new):
    row[new] = row[old]
    del row[old]
    return row

def fillna(row):
    if 'rating' not in row:
        row['rating'] = 0
    if 'sum_arap' not in row:
        row['sum_arap'] = 0
    if 'sum_accr' not in row:
        row['sum_accr'] = 0
    return row

def create_key(row, indices):
    key = {}
    remainder_indices = list(set(INDICES) - set(indices))
    for idx in remainder_indices:
        key[idx] = 'Total'
    for idx in indices:
        key[idx] = row[idx]
    
    legal_entity = key['legal_entity']
    counterparty = key['counterparty']
    tier = key['tier']
    return f'{legal_entity}-{counterparty}-{tier}'

def reduce_key(row):
    values = row['key'].split('-')
    del row['key']
    columns = {
        'legal_entity': values[0],
        'counterparty': values[1],
        'tier': values[2]
    }
    row.update(columns)
    return row

def flatten_data(group):
    data = {'key':group[0]}
    data.update(group[1])
    return data

def format_csv(row):
    values = [
        row['legal_entity'],
        row['counterparty'],
        str(row['tier']),
        str(row['rating']),
        str(row['sum_arap']),
        str(row['sum_accr'])
    ]
    return ', '.join(values)

def run():
    with beam.Pipeline() as p:
        
        #upstream
        delimiter = str.encode('\r')
        row_map = lambda x: {
            'invoice_id':int(x[0]),
            'legal_entity':x[1],
            'counterparty':x[2],
            'rating':int(x[3]),
            'status':x[4],
            'value':int(x[5])
        }

        left = ( 
            p
            | "set1" >> beam.io.ReadFromText('dataset1.csv', coder=beam.coders.StrUtf8Coder(), delimiter=delimiter, skip_header_lines=True)
            | "set1 split" >> beam.Map(lambda x: x.split(","))
            | 'Filter bad set1 rows' >> beam.Filter(lambda x: len(x) == 6)
            | "map set1 rows" >> beam.Map(row_map)
        )

        row_map2 = lambda x: {
            'counterparty':x[0],
            'tier':int(x[1]),
        }

        right = (
            p
            | "set2" >> beam.io.ReadFromText('dataset2.csv', coder=beam.coders.StrUtf8Coder(), delimiter=delimiter, skip_header_lines=True)
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
                    join_keys={'left': ['counterparty'], 'right': ['counterparty']}
                )
        )
        # start_file = (
        #     | f'write - {group_name}' >> beam.io.WriteToText('beam-report', file_name_suffix='.csv' header='legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)')
        # )
        streams = []
        for idx_group in get_groupings(INDICES):
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
                    'beam-report',
                    file_name_suffix='.csv',
                    header='legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)',
                    shard_name_template='',
                    append_trailing_newlines=True
                )
        )

        res = p.run()
        res.wait_until_finish()

run()