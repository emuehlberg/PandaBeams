import itertools

INDICES = ['legal_entity', 'counter_party', 'tier']

def get_groupings(indices):
    groupings = []
    for idx in range(len(indices)):
        for grouping in itertools.combinations(indices, idx+1):
            groupings.append(list(grouping))
    return groupings
