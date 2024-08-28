from glob import glob
from tqdm import tqdm
import pandas as pd
import numpy as np
import pickle

folder_path = 'clean_data/clean_datasets_perHour/*'

files = glob(folder_path)

node_freq_df = pd.DataFrame()

for file_path in tqdm(files, desc='Computing node frequencies'):
    # file_path = files[0]
    hour = file_path.split('_')[-1].split('.csv')[0]
    database = file_path.split('\\')[-1].split('_')[0]

    node_freq_hour_df = pd.DataFrame(pd.read_csv(file_path, index_col = 0).melt().value_counts()).reset_index()

    node_freq_hour_df.columns = ['node_type', 'node', 'freq']

    node_freq_hour_df['db'] = database
    node_freq_hour_df['hour'] = hour

    node_freq_df = pd.concat([node_freq_df, node_freq_hour_df])

node_freq_df['db'] = node_freq_df.db.replace('no','noAlt')

str_identifier = 'hashNodes'


communities_path = 'D:/FV/Personal/VIU/clean_data/communities_{}_nodeFreqLargerThan2.pickle'.format(str_identifier)

with open(communities_path, 'rb') as handle:
    comp_metrics = pickle.load(handle)


# Calculate total number of iterations
total_iterations = sum(len(comp_metrics[db][hour]['louvain_c'])
                       for db in comp_metrics.keys()
                       for hour in comp_metrics[db].keys()
                       if isinstance(comp_metrics[db][hour]['louvain_c'], list))
    
with tqdm(total=total_iterations, desc = 'Computing for hashtag networks') as pbar:
    for db in comp_metrics.keys():
        db_mask = node_freq_df['db'] == db
        for hour in comp_metrics[db].keys():
            hour_mask = node_freq_df['hour'] == hour
            if isinstance(comp_metrics[db][hour]['louvain_c'], list):
                for comp_id in range(len(comp_metrics[db][hour]['louvain_c'])):
                    comp = comp_metrics[db][hour]['louvain_c'][comp_id]
                    node_freq_df.loc[db_mask & hour_mask &
                        (node_freq_df['node'].isin(comp)), ['hash_community', 'hash_community_size']] = [comp_id, len(comp)]
                    pbar.update(1)


str_identifier = 'userNodes'

communities_path = 'D:/FV/Personal/VIU/clean_data/communities_{}_nodeFreqLargerThan2.pickle'.format(str_identifier)

with open(communities_path, 'rb') as handle:
    comp_metrics = pickle.load(handle)


# Calculate total number of iterations
total_iterations = sum(len(comp_metrics[db][hour]['louvain_c'])
                       for db in comp_metrics.keys()
                       for hour in comp_metrics[db].keys()
                       if isinstance(comp_metrics[db][hour]['louvain_c'], list))
    
with tqdm(total=total_iterations, desc = 'Computing for user networks') as pbar:
    for db in comp_metrics.keys():
        db_mask = node_freq_df['db'] == db
        for hour in comp_metrics[db].keys():
            hour_mask = node_freq_df['hour'] == hour
            if isinstance(comp_metrics[db][hour]['louvain_c'], list):
                for comp_id in range(len(comp_metrics[db][hour]['louvain_c'])):
                    comp = comp_metrics[db][hour]['louvain_c'][comp_id]
                    node_freq_df.loc[db_mask & hour_mask &
                        (node_freq_df['node'].isin(comp)), ['user_community', 'user_community_size']] = [comp_id, len(comp)]
                    pbar.update(1)


str_identifier = 'bipartite'

communities_path = 'D:/FV/Personal/VIU/clean_data/communities_{}_nodeFreqLargerThan2.pickle'.format(str_identifier)

with open(communities_path, 'rb') as handle:
    comp_metrics = pickle.load(handle)


# Calculate total number of iterations
total_iterations = sum(len(comp_metrics[db][hour]['louvain_c'])
                       for db in comp_metrics.keys()
                       for hour in comp_metrics[db].keys()
                       if isinstance(comp_metrics[db][hour]['louvain_c'], list))
    
with tqdm(total=total_iterations, desc = 'Computing for bipartite networks') as pbar:
    for db in comp_metrics.keys():
        db_mask = node_freq_df['db'] == db
        for hour in comp_metrics[db].keys():
            hour_mask = node_freq_df['hour'] == hour
            if isinstance(comp_metrics[db][hour]['louvain_c'], list):
                for comp_id in range(len(comp_metrics[db][hour]['louvain_c'])):
                    comp = comp_metrics[db][hour]['louvain_c'][comp_id]
                    node_freq_df.loc[db_mask & hour_mask &
                        (node_freq_df['node'].isin(comp)), ['bip_community', 'bip_community_size']] = [comp_id, len(comp)]
                    pbar.update(1)

with open('D:/FV/Personal/VIU/clean_data/node_freq_comm_per_hour_nodeFreqLargerThan2.pickle', 'wb') as handle:
    pickle.dump(node_freq_df, handle, protocol=pickle.HIGHEST_PROTOCOL)

