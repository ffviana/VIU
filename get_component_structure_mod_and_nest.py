import pandas as pd
import numpy as np
import pickle
import networkx as nx
from nestedness_master.nestedness_calculator import NestednessCalculator
from glob import glob
from tqdm import tqdm


# folder_path = 'D:/FV/Personal/VIU/clean_data/link_lists_after_cleaning/*userNodes*'
# node_type = 'user'
# edge_type = 'hashtag'
# output_fname = 'graph_metrics_userNodes_clean_final.csv'

folder_path = 'D:/FV/Personal/VIU/clean_data/link_lists_after_cleaning/*hashNodes*'
node_type = 'hashtag'
edge_type = 'user'
output_fname = 'graph_metrics_hashNodes_clean_final.csv'


output_path = 'D:/FV/Personal/VIU/clean_data/' + output_fname

files = glob(folder_path)

# Gather all files in the folder and filter out any CSV files
files = [f for f in glob(folder_path) if '.csv' not in f]

def get_component_metrics(file_list, node_type, edge_type):

    # Data paths
    n9_path = 'D:/FV/Personal/VIU/clean_data/9n_9ngranmarchaporlajusticia.csv'
    noAlTar_path = 'D:/FV/Personal/VIU/clean_data/noaltarifazo_ruidazonacional.csv'

    # Initialize an empty DataFrame to store the complete data
    complete_df = pd.DataFrame()

    # Loop through each file in the provided list
    for file_path in tqdm(file_list, desc = 'Creating ' + output_fname):
        
        # Extract hour and database type from the file path
        hour = file_path.split('_link_list.pickle')[0].split('_')[-2]
        database = file_path.split('\\')[-1].split('_')[0]

        # Load the link list from the pickle file
        with open(file_path, 'rb') as f:
            link_list = pickle.load(f)

        # Filter out links with zero weight
        clean_link_list = [tup for tup in link_list if tup[2] != 0]
        clean_link_arr = np.array(clean_link_list)
        nodes = np.unique(np.concatenate([clean_link_arr[:, 0], clean_link_arr[:, 1]]))

        # Create a graph and add cleaned links as weighted edges
        G = nx.Graph()
        G.add_weighted_edges_from(clean_link_list)

        # Calculate modularity using the Louvain method
        louvain_communities = nx.community.louvain_communities(G)
        modularity = nx.community.modularity(G, louvain_communities)

        # Calculate Nestedness
        adj_matrix = nx.adjacency_matrix(G, weight=None).todense()
        nestedness = NestednessCalculator(adj_matrix).nodf(adj_matrix)

        # Get connected components and count the number of components
        components = sorted(nx.connected_components(G), key=len, reverse=True)
        nr_of_components = len(components)

        # Load and filter the database file based on the database type and hour
        if database == '9n':
            db_df = pd.read_csv(n9_path)
            db_df['hashtag'] = db_df.hashtag.str.lower(
                        ).str.normalize('NFKD'
                        ).str.encode('ascii', errors='ignore'
                        ).str.decode('utf-8')
            db_hour_df = db_df[db_df.hour == int(hour)]
        elif database == 'no':
            db_df = pd.read_csv(noAlTar_path)
            db_df['hashtag'] = db_df.hashtag.str.lower(
                            ).str.normalize('NFKD'
                            ).str.encode('ascii', errors='ignore'
                            ).str.decode('utf-8')
            db_hour_df = db_df[db_df.hour == int(hour)]

        # Calculate total and unique edges in the network
        total_nr_of_edges = db_hour_df[db_hour_df[node_type].isin(nodes)][edge_type].count()
        total_nr_of_unique_edges = db_hour_df[db_hour_df[node_type].isin(nodes)][edge_type].nunique()

        # Calculate total and unique nodes in the network directly from components
        total_nr_of_unique_nodes = len(np.unique(nodes))
        nr_of_unique_nodes_per_c = [len(comp) for comp in components]
        nr_of_unique_nodes_in_giant_c = nr_of_unique_nodes_per_c[0]

        # Calculate edges and unique nodes in each component, including the giant component
        nr_of_edges_per_c = [db_hour_df[db_hour_df[node_type].isin(list(comp))][edge_type].count() for comp in components]
        nr_of_unique_edges_per_c = [db_hour_df[db_hour_df[node_type].isin(list(comp))][edge_type].nunique() for comp in components]
        
        # Extract the giant component metrics
        nr_of_edges_in_giant_c = nr_of_edges_per_c[0]
        nr_of_unique_edges_in_giant_c = nr_of_unique_edges_per_c[0]

        # Create a DataFrame for the current file's data
        hour_df = pd.DataFrame([[
            database, hour, modularity, nestedness, nr_of_components,
            total_nr_of_edges, total_nr_of_unique_edges, total_nr_of_unique_nodes,
            nr_of_edges_in_giant_c, nr_of_unique_edges_in_giant_c, nr_of_unique_nodes_in_giant_c,
            nr_of_edges_per_c, nr_of_unique_edges_per_c, nr_of_unique_nodes_per_c
        ]], columns=[
            'db', 'hour', 'mod', 'nest', 'n_comp',
            'total_nr_of_edges', 'total_nr_of_unique_edges', 'total_nr_of_unique_nodes',
            'nr_of_edges_in_giant_c', 'nr_of_unique_edges_in_giant_c', 'nr_of_unique_nodes_in_giant_c',
            'nr_of_edges_per_c', 'nr_of_unique_edges_per_c', 'nr_of_unique_nodes_per_c'
        ])

        # Append the current hour's data to the complete DataFrame
        complete_df = pd.concat([complete_df, hour_df], ignore_index=True)

    return complete_df


complete_df = get_component_metrics(files, node_type, edge_type)

complete_df.to_csv(output_path)
