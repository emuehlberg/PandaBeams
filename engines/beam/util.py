
from shared import INDICES


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
    counterparty = key['counter_party']
    tier = key['tier']
    return f'{legal_entity}-{counterparty}-{tier}'

def reduce_key(row):
    values = row['key'].split('-')
    del row['key']
    columns = {
        'legal_entity': values[0],
        'counter_party': values[1],
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
        row['counter_party'],
        str(row['tier']),
        str(row['rating']),
        str(row['sum_arap']),
        str(row['sum_accr'])
    ]
    return ', '.join(values)
