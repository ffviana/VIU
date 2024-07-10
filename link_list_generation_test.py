import pandas as pd
from itertools import combinations
import pickle


# Data paths
n9_path = 'D:/FV/Personal/VIU/clean_data/9n_9ngranmarchaporlajusticia.csv'

n9_df = pd.read_csv(n9_path)
n9_df['date'] =pd.to_datetime(n9_df.hour * 3600, unit='s')

n9_example_nework_df =  n9_df[n9_df.date == '2019-11-09 21:00']

n9_example_users_per_node = n9_example_nework_df.groupby('hashtag').user.agg(['count', 'nunique']).sort_values(by='count', ascending=False)

def get_node_pairs(x):
    result = [(x[i], x[j]) for i, j in combinations(range(len(x)), 2)]
    return result

node_pairs = get_node_pairs(n9_example_nework_df.hashtag.unique())
# node_pairs = get_node_pairs(n9_example_nework_df.user.unique())

df = n9_example_nework_df.copy()

def get_linkweight_per_hash_pair(node_pair, df):

    count_hash_per_user = df[df.hashtag.isin(node_pair)
                           ].drop_duplicates(subset = ['user', 'hashtag']
                                             ).groupby('user').hashtag.count()

    link_weight = sum(count_hash_per_user==2)

    return link_weight

def get_linkweight_per_user_pair(node_pair, df):

    count_hash_per_user = df[df.user.isin(node_pair)
                           ].drop_duplicates(subset = ['user', 'hashtag']
                                             ).groupby('hashtag').user.count()

    link_weight = sum(count_hash_per_user==2)

    return link_weight

link_list = [node_pair + (get_linkweight_per_user_pair(node_pair, df),) for node_pair in node_pairs[:5]]

# Link list for bipartite graph

bipartite_link_list_df = n9_example_nework_df.groupby(by=['user', 'hashtag'], as_index=False).size().sort_values(by='size', ascending=False)
bipartite_link_list =  [tuple(row) for row in bipartite_link_list_df.values.tolist()]

with open('D:/FV/VIU/clean_data/link_list_example.pickle', 'wb') as f:
    pickle.dump(link_list, f)


