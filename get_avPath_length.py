import pickle
import pandas as pd
import networkx as nx
from tqdm import tqdm
from glob import glob

def analyze_graphs(filepaths, output_csv='graph_analysis_results.csv'):
    results = []

    for filepath in tqdm(filepaths, desc='Creating ' + output_csv):

        hour = filepath.split('_link_list.pickle')[0].split('_')[-2]
        database = filepath.split('\\')[-1].split('_')[0]

        date = pd.to_datetime(int(hour) * 3600, unit='s')

        # Define date boundaries as Timestamps
        start_date_9n = pd.Timestamp('2019-11-08 06:00')
        end_date_9n = pd.Timestamp('2019-11-10 19:00')
        start_date_no = pd.Timestamp('2019-01-01 06:00')
        end_date_no = pd.Timestamp('2019-01-07 22:00')

        # Adjust conditional checks
        if not ((database == '9n') and (start_date_9n <= date <= end_date_9n)) and not ((database == 'no') and (start_date_no <= date <= end_date_no)):
            continue
        # Load the graph from the pickle file
        with open(filepath, 'rb') as f:
            link_list = pickle.load(f)

        if 'bipartite' in output_csv:
            hash_count = pd.DataFrame(link_list).groupby(1).size()
            hash_only_used_by_one_user = hash_count[hash_count==1].index
            clean_link_list = [link for link in link_list if link[1] not in hash_only_used_by_one_user]
        else:
            clean_link_list = [tup for tup in link_list if tup[2] != 0]

        graph = nx.Graph()
        graph.add_weighted_edges_from(clean_link_list)

        
        
        # Initialize values
        num_connected_components = None
        largest_component_nodes = None
        largest_component_edges = None
        avg_path_length = None
        diameter = None

        # Filter connected components with at least 5 nodes
        components = [comp for comp in nx.connected_components(graph) if len(comp) >= 5]

        if components:
            # Number of connected components with at least 5 nodes
            num_connected_components = len(components)
            
            # Get the largest component with at least 5 nodes
            largest_component = max(components, key=len)
            subgraph = graph.subgraph(largest_component)

            # Calculate properties of the largest component
            largest_component_nodes = subgraph.number_of_nodes()
            largest_component_edges = subgraph.number_of_edges()

            if nx.is_connected(subgraph):
                avg_path_length = nx.average_shortest_path_length(subgraph, weight=True)
                diameter = nx.diameter(subgraph, weight = 'weight')
        else:
            # No connected component with at least 5 nodes
            num_connected_components = 0

        # Append the results
        results.append({
            'database': database,
            'hour':hour,
            'Connected Components (>=5 nodes)': num_connected_components,
            'Largest Component Nodes': largest_component_nodes,
            'Largest Component Edges': largest_component_edges,
            'Average Path Length': avg_path_length,
            'Diameter': diameter
        })

    # Create a DataFrame from the results
    df = pd.DataFrame(results)
    
    # Save the DataFrame to a CSV file
    df.to_csv(output_csv, index=False)
    
    return df

# Example usage
filepaths = glob('D:/FV/Personal/VIU/clean_data/link_lists_after_cleaning/*bipartite*')  # Replace with actual file paths
df = analyze_graphs(filepaths, output_csv='bipartite_connectivit_results.csv')


