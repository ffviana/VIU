from glob import glob
from tqdm import tqdm
import pickle
import numpy as np
import pandas as pd
import networkx as nx


# str_identifier = 'userNodes'
# str_identifier = 'hashNodes'
str_identifier = 'bipartite'

folder_path = 'D:/FV/Personal/VIU/clean_data/link_lists_after_cleaning/*{}*'.format(str_identifier)

files = glob(folder_path)

files = [f for f in files if '.csv' not in f]
file_path = files[0]

complete_df = pd.DataFrame()

dic_9n = {}
dic_noAlt = {}

for file_path in tqdm(files, desc='Computing pageRank and k-core in {}'.format(str_identifier)):
    if '.csv' in file_path:
        continue

    hour = file_path.split('_link_list.pickle')[0].split('_')[-2]
    database = file_path.split('\\')[-1].split('_')[0]

    with open(file_path, 'rb') as f:
        link_list = pickle.load(f)

    if str_identifier == 'bipartite':
        hash_count = pd.DataFrame(link_list).groupby(1).size()
        hash_only_used_by_one_user = hash_count[hash_count==1].index
        clean_link_list = [link for link in link_list if link[1] not in hash_only_used_by_one_user]
    else:
        clean_link_list = [tup for tup in link_list if tup[2] != 0]

    # Remove nodes with low frequency
    # df_path = glob('D:/FV/Personal/VIU/clean_data/clean_datasets_perHour/*{}*{}*'.format(database, hour))[0]
    # df = pd.read_csv(df_path, index_col = 0).melt()['value'].value_counts()
    # nodes = [str(p) for p in list(df[df > 2].index)]
    # clean_link_list = [tup for tup in clean_link_list if (str(tup[0]) in nodes) and ((str(tup[1]) in nodes))]
    # # ---------------------------

    clean_link_arr = np.array(clean_link_list)

    G = nx.Graph()
    
    G.add_weighted_edges_from(clean_link_list)
    components = sorted(nx.connected_components(G), key=len, reverse=True)
    G = G.subgraph(components[0])
    # get pagerank
    pr = nx.pagerank(G, alpha=0.85) # default alpha .85

    # get k-core
    kCore = nx.k_core(G)

    hour_dic = {}

    hour_dic['pagerank'] = pr
    hour_dic['kcore'] = kCore

    # Get database
    if database == '9n':
        dic_9n[hour] = hour_dic

    elif database == 'no':
        dic_noAlt[hour] = hour_dic

    final_dic = {'9n': dic_9n,
                'noAlt': dic_noAlt}

with open('D:/FV/Personal/VIU/clean_data/pageRank_giantComp_{}.pickle'.format(str_identifier), 'wb') as handle:
    pickle.dump(final_dic, handle, protocol=pickle.HIGHEST_PROTOCOL)