from typing import List
import pandas as pd
from engines.base import DataLoader
from engines.panda.util import concat_data, get_max, get_sum, pad_dataset, sanitize, format_table

from shared import INDICES, get_groupings

class PandaLoader(DataLoader):

    def run(self):
        dataset_1 = pd.read_csv(self.wide_csv)
        dataset_2 = pd.read_csv(self.narrow_csv)

        data = pd.merge(
            dataset_1,
            dataset_2,
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
        full_dataset.to_csv(self.output_file)
