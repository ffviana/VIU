import pandas as pd
import pickle
import sys

# Data paths
fpath = sys.argv[1] # Fetch Behaviour filepath

df = pd.read_csv(fpath)

bipartite_link_list_df = df.groupby(by=['user', 'hashtag'], as_index=False).size().sort_values(by='size', ascending=False)
bipartite_link_list =  [tuple(row) for row in bipartite_link_list_df.values.tolist()]

with open('{}_bipartite_link_list.pickle'.format(fpath.split('.csv')[0]), 'wb') as f:
    pickle.dump(bipartite_link_list, f)