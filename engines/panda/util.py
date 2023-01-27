import pandas as pd

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
