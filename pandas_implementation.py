import itertools
import pandas as pd

from shared import INDICES, get_groupings

def get_sum(dataset, status_filter, grouping):
    res = dataset[dataset['status']==status_filter].groupby(
        grouping,
        dropna=False,
        as_index=True
    )['value'].sum().to_frame()
    return res.rename(columns={
        'value':f'sum(value where status={status_filter})',
    })

def get_max(dataset, grouping):
    res = dataset.groupby(grouping, dropna=False, as_index=True)['rating'].max().to_frame()
    return res.rename(columns={
        'rating': 'max(rating by counterparty)',
    })

def pad_dataset(dataset:pd.DataFrame, columns):
    for column in columns:
        dataset.insert(0,column,'Total')
    return dataset

def concat_data(dataset, new_data, merge=True):
    if merge:
        return pd.merge(dataset, new_data, how='outer', left_index=True, right_index=True)
    return pd.concat([dataset, new_data])

def sanitize(dataset: pd.DataFrame):
    res = dataset.reset_index()
    res = res.set_index('legal_entity')
    res = res.fillna(0)
    return res

def format_table(dataset: pd.DataFrame):
    
    res = dataset.rename(columns={
        'counter_party': 'counterparty'
    })

    res.sort_values(['legal_entity', 'counterparty', 'tier'], inplace=True)

    return res[['counterparty','tier','max(rating by counterparty)', 'sum(value where status=ARAP)', 'sum(value where status=ACCR)']]

def run():
    dataset_1 = pd.read_csv('dataset1.csv')
    dataset_2 = pd.read_csv('dataset2.csv')

    data = pd.merge(
        dataset_2,
        dataset_1,
        how='outer',
        left_on='counter_party',
        right_on='counter_party'
    )

    data.set_index(['invoice_id'], drop=True, inplace=True)
    data.sort_values('invoice_id', inplace=True)

    full_dataset = None
    for grouping in get_groupings(INDICES):
        remainder_indices = list(set(INDICES) - set(grouping))
        dataset = concat_data(
            concat_data(
                get_max(data, grouping),
                get_sum(data, 'ARAP', grouping)
            ),
            get_sum(data, 'ACCR', grouping)
        )
        dataset = pad_dataset(dataset, remainder_indices)
        dataset = sanitize(dataset)
        if full_dataset is None:
            full_dataset = dataset
            continue
        full_dataset = concat_data(full_dataset, dataset, False)
    if full_dataset is None:
        return

    full_dataset = format_table(full_dataset)
    full_dataset.to_csv('pandas-report.csv')

run()