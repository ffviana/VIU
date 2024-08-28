import pickle
import itertools
import nltk
import numpy as np
from tqdm import tqdm
from joblib import Parallel, delayed
import pandas as pd


# Function to compute the Levenshtein distance between two elements
def compute_distance(pair, giant_comp_per_hour, hours):
    i, j = pair
    dist = nltk.edit_distance(giant_comp_per_hour[hours[i]], giant_comp_per_hour[hours[j]])
    return i, j, dist

def parallel_compute_distance(comp_metrics_db):
    giant_comp_per_hour = {key:sorted([str(element) for element in comp_metrics_db[key]['components'][0]]) for key in comp_metrics_db.keys()}
    hours = sorted(giant_comp_per_hour.keys())

    n = len(hours)
    pairs = list(itertools.combinations(range(n), 2))

    # Use parallel processing to compute the distances with a progress bar
    results = Parallel(n_jobs=8)(delayed(compute_distance)(pair, giant_comp_per_hour, hours) for pair in tqdm(pairs, desc="Calculating distances"))

    # Fill in the distance matrix
    leven_distance_mat = np.full((len(hours), len(hours)), np.nan)
    for i, j, dist in results:
        leven_distance_mat[i, j] = dist
        leven_distance_mat[j, i] = dist  # Since the distance matrix is symmetric
    leven_distance_df = pd.DataFrame(leven_distance_mat, index=hours, columns = hours)

    return leven_distance_df


for str_identifier in ['hashNodes', 'userNodes', 'bipartite']:
# for str_identifier in ['bipartite']:

    comp_metrics_path = 'D:/FV/Personal/VIU/clean_data/comp_metrics_{}.pickle'.format(str_identifier)

    with open(comp_metrics_path, 'rb') as handle:
        comp_metrics = pickle.load(handle)

    comp_metrics
    comp_metrics_9n = comp_metrics['9n']
    leven_distance_9n_df = parallel_compute_distance(comp_metrics_9n)

    # =============================================

    comp_metrics_no = comp_metrics['noAlt']
    leven_distance_no_df = parallel_compute_distance(comp_metrics_no)

    out_dic = {'9n': leven_distance_9n_df,
            'no': leven_distance_no_df}

    with open('D:/FV/Personal/VIU/clean_data/giantComp_levenshtein_distance_{}.pickle'.format(str_identifier), 'wb') as handle:
        pickle.dump(out_dic, handle, protocol=pickle.HIGHEST_PROTOCOL)