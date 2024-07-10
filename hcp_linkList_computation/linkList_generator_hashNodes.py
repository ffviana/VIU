import pandas as pd
from itertools import combinations
import pickle
import sys

# Data paths
fpath = sys.argv[1] # Fetch Behaviour filepath

df = pd.read_csv(fpath)

def get_node_pairs(x):
    result = [(x[i], x[j]) for i, j in combinations(range(len(x)), 2)]
    return result

node_pairs = get_node_pairs(df.hashtag.unique())


def get_linkweight_per_hash_pair(node_pair, df):

    count_hash_per_user = df[df.hashtag.isin(node_pair)
                           ].drop_duplicates(subset = ['user', 'hashtag']
                                             ).groupby('user').hashtag.count()
    link_weight = sum(count_hash_per_user==2)

    return link_weight

link_list = [node_pair + (get_linkweight_per_hash_pair(node_pair, df),) for node_pair in node_pairs]

with open('{}_link_list.pickle'.format(fpath.split('.csv')[0]), 'wb') as f:
    pickle.dump(link_list, f)