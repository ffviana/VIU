from glob import glob
from tqdm import tqdm
import pandas as pd
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


comp_metrics_path = 'D:/FV/Personal/VIU/clean_data/comp_metrics_{}.pickle'.format(str_identifier)

with open(comp_metrics_path, 'rb') as handle:
    comp_metrics = pickle.load(handle)


# Calculate total number of iterations
total_iterations = sum(len(comp_metrics[db][hour]['components'])
                       for db in comp_metrics.keys()
                       for hour in comp_metrics[db].keys())
    
with tqdm(total=total_iterations, desc = 'Computing for hashtag networks') as pbar:
    for db in comp_metrics.keys():
        db_mask = node_freq_df['db'] == db
        for hour in comp_metrics[db].keys():
            hour_mask = node_freq_df['hour'] == hour
            for comp_id in range(len(comp_metrics[db][hour]['components'])):
                comp = comp_metrics[db][hour]['components'][comp_id]
                node_freq_df.loc[db_mask & hour_mask &
                    (node_freq_df['node'].isin(comp)), ['hash_comp', 'hash_comp_size']] = [comp_id, len(comp)]
                pbar.update(1)


str_identifier = 'userNodes'

comp_metrics_path = 'D:/FV/Personal/VIU/clean_data/comp_metrics_{}.pickle'.format(str_identifier)

with open(comp_metrics_path, 'rb') as handle:
    comp_metrics = pickle.load(handle)


# Calculate total number of iterations
total_iterations = sum(len(comp_metrics[db][hour]['components'])
                       for db in comp_metrics.keys()
                       for hour in comp_metrics[db].keys())
    
with tqdm(total=total_iterations, desc = 'Computing for user networks') as pbar:
    for db in comp_metrics.keys():
        db_mask = node_freq_df['db'] == db
        for hour in comp_metrics[db].keys():
            hour_mask = node_freq_df['hour'] == hour
            for comp_id in range(len(comp_metrics[db][hour]['components'])):
                comp = comp_metrics[db][hour]['components'][comp_id]
                node_freq_df.loc[db_mask & hour_mask &
                    (node_freq_df['node'].isin(comp)), ['user_comp', 'user_comp_size']] = [comp_id, len(comp)]
                pbar.update(1)


str_identifier = 'bipartite'

comp_metrics_path = 'D:/FV/Personal/VIU/clean_data/comp_metrics_{}.pickle'.format(str_identifier)

with open(comp_metrics_path, 'rb') as handle:
    comp_metrics = pickle.load(handle)


# Calculate total number of iterations
total_iterations = sum(len(comp_metrics[db][hour]['components'])
                       for db in comp_metrics.keys()
                       for hour in comp_metrics[db].keys())
    
with tqdm(total=total_iterations, desc = 'Computing for bipartite networks') as pbar:
    for db in comp_metrics.keys():
        db_mask = node_freq_df['db'] == db
        for hour in comp_metrics[db].keys():
            hour_mask = node_freq_df['hour'] == hour
            for comp_id in range(len(comp_metrics[db][hour]['components'])):
                comp = comp_metrics[db][hour]['components'][comp_id]
                node_freq_df.loc[db_mask & hour_mask &
                    (node_freq_df['node'].isin(comp)), ['bip_comp', 'bip_comp_size']] = [comp_id, len(comp)]
                pbar.update(1)

with open('D:/FV/Personal/VIU/clean_data/node_freq_per_hour.pickle', 'wb') as handle:
    pickle.dump(node_freq_df, handle, protocol=pickle.HIGHEST_PROTOCOL)

