from glob import glob
from tqdm import tqdm
import pickle
import numpy as np
import pandas as pd
import networkx as nx


str_identifier = 'userNodes'
# str_identifier = 'hashNodes'
# str_identifier = 'bipartite'

folder_path = 'D:/FV/Personal/VIU/clean_data/link_lists_after_cleaning/*{}*'.format(str_identifier)

files = glob(folder_path)

files = [f for f in files if '.csv' not in f]
file_path = files[0]



complete_df = pd.DataFrame()

dic_9n = {}
dic_noAlt = {}

for file_path in tqdm(files):
    if '.csv' in file_path:
        continue

    hour = file_path.split('_link_list.pickle')[0].split('_')[-2]
    database = file_path.split('\\')[-1].split('_')[0]

    with open(file_path, 'rb') as f:
        link_list = pickle.load(f)

    if str_identifier == 'bipartite': # Trimming network
        hash_count = pd.DataFrame(link_list).groupby(1).size()
        hash_only_used_by_one_user = hash_count[hash_count==1].index
        clean_link_list = [link for link in link_list if link[1] not in hash_only_used_by_one_user]
    else:
        clean_link_list = [tup for tup in link_list if tup[2] != 0]

    clean_link_arr = np.array(clean_link_list)
    nodes = np.concatenate([clean_link_arr[:,0], clean_link_arr[:,1]])

    G = nx.Graph()
    G.add_weighted_edges_from(clean_link_list)

    # get components
    components = sorted(nx.connected_components(G), key=len, reverse=True)
    nr_of_components = len(components)

    components = [list(comp) for comp in components]

    hour_dic = {}
    G_sub = G.subgraph(components[0])
    hour_dic['degree'] = dict(G_sub.degree)
    hour_dic['degree_cent'] = nx.degree_centrality(G_sub)
    hour_dic['closeness_cent'] = nx.closeness_centrality(G_sub)
    try:
        hour_dic['eigen_cent'] = nx.eigenvector_centrality(G_sub, max_iter=500, weight='weight')
    except:
        hour_dic['eigen_cent'] = np.nan
    try:
        hour_dic['between_cent'] = nx.betweenness_centrality(G_sub, weight='weight')
    except:
        hour_dic['between_cent'] = np.nan

    # Get database
    if database == '9n':
        dic_9n[hour] = hour_dic

    elif database == 'no':
        dic_noAlt[hour] = hour_dic

final_dic = {'9n': dic_9n,
             'noAlt': dic_noAlt}

with open('D:/FV/Personal/VIU/clean_data/compGiant2_metrics_{}.pickle'.format(str_identifier), 'wb') as handle:
    pickle.dump(final_dic, handle, protocol=pickle.HIGHEST_PROTOCOL)