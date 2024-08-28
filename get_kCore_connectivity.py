import pickle
from tqdm import tqdm
import pandas as pd
from glob import glob
import networkx as nx

def get_kcore(comp_metrics, db, net_type):
    kcores = []
    for hour in tqdm(comp_metrics[db].keys(), desc='Processing {} {} network'.format(db, net_type)): 
        G = comp_metrics[db][hour]['kcore']

        node_size = len(list(G.nodes))
        edge_size = len(list(G.edges))
        edge_conncetivity = nx.edge_connectivity(G)
        node_connectiviy = nx.node_connectivity(G)
            
        kcores.append([db, hour, net_type,node_size , edge_size, node_connectiviy, edge_conncetivity])
            
    return kcores

kCore = []

for net_type in ['bipartite', 'hashNodes', 'userNodes']:

    # def plot_Node_freq():
    pd.set_option('future.no_silent_downcasting', True)
    if 'bipartite' in net_type:
        nodes = ['hashtag', 'user']
        comp_col = 'bip_comp'
    elif net_type == 'hashNodes':
        nodes = ['hashtag']
        comp_col = 'hash_comp'
    else:
        nodes = ['user']
        comp_col = 'user_comp'
        
    comp_metrics_path = 'D:/FV/Personal/VIU/clean_data/pageRank_kCore_{}.pickle'.format(net_type)

    with open(comp_metrics_path, 'rb') as handle:
        comp_metrics = pickle.load(handle)

    n_top_nodes = 5


    for db in ['9n', 'noAlt']:

        kCore += get_kcore(comp_metrics, db, net_type)

with open('D:/FV/Personal/VIU/clean_data/kCore_connectivity.pickle', 'wb') as handle:
    pickle.dump(kCore, handle, protocol=pickle.HIGHEST_PROTOCOL)