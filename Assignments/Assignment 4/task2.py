# Import necessary libraries for Spark configuration and data processing
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import time
import sys
from collections import defaultdict

# Initialize Spark configuration and context
conf = SparkConf().setAppName("CommunityDetection")
context = SparkContext(conf=conf)
# Set log level to ERROR for cleaner output logs
context.setLogLevel("ERROR")
# Create a Spark session from the existing context
spark_session = SparkSession(context)

# Function to load and preprocess review data from a CSV file
def load_reviews(file_path):
    # Read the CSV file as a text file and split each line into a list
    raw_data = context.textFile(file_path).map(lambda line: line.split(','))
    # Extract the first line as header
    header = raw_data.first()
    # Filter out the header, keeping the data only
    data_without_header = raw_data.filter(lambda row: row != header)
    return data_without_header

# Function to determine if a pair of users share enough common businesses
def has_sufficient_common_businesses(pair):
    user1, user2 = pair  # Unpack the pair of users
    # Convert user-business mappings to sets for easier comparison
    businesses_of_user1 = set(user_to_businesses_map[user1])
    businesses_of_user2 = set(user_to_businesses_map[user2])
    # Check if the intersection of businesses meets the threshold
    return len(businesses_of_user1.intersection(businesses_of_user2)) >= common_business_threshold

# Function to calculate the betweenness score of each edge in the graph
def calculate_edge_betweenness(graph):
    edge_betweenness_scores = defaultdict(float)

    # Inner function to perform BFS and calculate shortest paths
    def bfs(node):
        # Initialize BFS structures
        visited = set([node])
        queue = [(node, 0)]
        levels = {node: 0}
        num_paths = {node: 1}
        parents = {node: []}

        # BFS loop
        while queue:
            current_node, level = queue.pop(0)
            for neighbour in graph.get(current_node, []):
                # Handle unvisited nodes
                if neighbour not in visited:
                    visited.add(neighbour)
                    queue.append((neighbour, level + 1))
                    parents[neighbour] = [current_node]
                    levels[neighbour] = level + 1
                    num_paths[neighbour] = num_paths[current_node]
                # Handle revisited nodes at the same level
                elif level + 1 == levels[neighbour]:
                    parents[neighbour].append(current_node)
                    num_paths[neighbour] += num_paths[current_node]

        return levels, num_paths, parents

    # Inner function to compute betweenness contribution of edges
    def compute_edge_contributions(levels, num_paths, parents):
        edge_credits = {}
        node_credits = {node: 1 for node in levels}

        # Calculate edge contributions in reverse level order
        for node in sorted(levels, key=levels.get, reverse=True):
            for parent in parents.get(node, []):
                edge = frozenset([node, parent])
                credit = node_credits[node] * (num_paths[parent] / num_paths[node])
                edge_credits[edge] = edge_credits.get(edge, 0) + credit
                node_credits[parent] += credit

        return edge_credits

    # Calculate betweenness for each node in the graph
    for node in graph:
        levels, num_paths, parents = bfs(node)
        edge_credits = compute_edge_contributions(levels, num_paths, parents)
        for edge, credit in edge_credits.items():
            edge_betweenness_scores[edge] += credit

    # Normalize the scores (each shortest path is counted twice)
    for edge in edge_betweenness_scores:
        edge_betweenness_scores[edge] /= 2.0

    return edge_betweenness_scores

# Function to detect communities within the graph
def detect_communities(nodes_neighbors):
    # Inner function for BFS to identify all connected nodes
    def bfs(start_node):
        visited = set()
        queue = [start_node]
        while queue:
            current = queue.pop(0)
            if current not in visited:
                visited.add(current)
                queue.extend([neighbor for neighbor in nodes_neighbors.get(current, []) if neighbor not in visited])
        return visited

    visited_global = set()
    communities = []
    # Identify communities by BFS from each unvisited node
    for node in nodes_neighbors.keys():
        if node not in visited_global:
            community = bfs(node)
            communities.append(frozenset(community))
            visited_global.update(community)
    return communities

# Function to calculate the modularity of a community structure
def calculate_modularity(communities, neighbors_map, edge_count, degree_dict):
    modularity_score = 0
    # Calculate modularity for each community
    for community in communities:
        for node_i in community:
            degree_i = degree_dict[node_i]
            for node_j in community:
                degree_j = degree_dict[node_j]
                # Check if an edge exists between node_i and node_j
                adjacency_value = 1 if node_j in neighbors_map[node_i] else 0
                # Modularity contribution from the edge (i, j)
                modularity_score += (adjacency_value - (degree_i * degree_j) / (2 * edge_count))
    # Normalize the modularity score
    modularity_score /= (2 * edge_count)
    return modularity_score

# Function to implement Girvan-Newman algorithm for community detection
def girvan_newman_algorithm(edges, neighbor_map):
    total_edges = len(edges)
    remaining_edges = dict(edges)
    filtered_neighbors = dict(neighbor_map)
    node_degrees = {node: len(neighbors) for node, neighbors in neighbor_map.items()}

    highest_modularity = float('-inf')
    optimal_communities = []

    # Iteratively remove edges and recalculate modularity
    while remaining_edges:
        # Find edges with the highest betweenness
        max_edge_betweenness = max(remaining_edges.values())
        edges_with_max_betweenness = {edge for edge, betweenness in remaining_edges.items() if betweenness == max_edge_betweenness}

        # Remove edges with maximum betweenness from the graph
        for edge in edges_with_max_betweenness:
            source, target = edge
            filtered_neighbors[source].remove(target)
            filtered_neighbors[target].remove(source)

        # Recalculate edge betweenness after removing edges
        remaining_edges = calculate_edge_betweenness(filtered_neighbors)

        # Detect communities in the updated graph
        current_communities = detect_communities(filtered_neighbors)

        # Calculate modularity of the current community structure
        modularity = calculate_modularity(current_communities, filtered_neighbors, total_edges, node_degrees)

        # Update optimal communities if current modularity is higher
        if modularity >= highest_modularity:
            optimal_communities = current_communities
            highest_modularity = modularity

    return optimal_communities

# Functions to write results to files
def write_betweenness_to_file(betweenness_scores, output_path):
    with open(output_path, 'w') as file:
        for ((user1, user2), score) in betweenness_scores:
            rounded_score = round(score, 5)
            file.write(f"('{user1}', '{user2}'), {rounded_score}\n")

def write_communities_to_file(communities, output_path):
    with open(output_path, 'w') as file:
        for community in communities:
            community_as_string = ", ".join(f"'{member}'" for member in community)
            file.write(f"{community_as_string}\n")

# Main execution block
if __name__ == '__main__':
    start_time = time.time()

    # Parse command line arguments
    common_business_threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    output_betweenness_path = sys.argv[3]
    output_communities_path = sys.argv[4]

    # Load and preprocess review data
    reviews_rdd = load_reviews(input_file_path)

    # Map users to businesses and filter based on review threshold
    user_business_pairs = reviews_rdd.map(lambda review: (review[0], [review[1]])).reduceByKey(lambda a, b: a + b)
    users_with_enough_reviews = user_business_pairs.filter(lambda pair: len(pair[1]) >= common_business_threshold)

    # Assign unique indices to users and map them to businesses
    user_business_indexed = users_with_enough_reviews.zipWithIndex().map(lambda pair: (pair[1], pair[0][0], pair[0][1]))
    user_to_index_map = user_business_indexed.map(lambda pair: (pair[0], pair[1])).collectAsMap()
    user_to_businesses_map = user_business_indexed.map(lambda pair: (pair[0], pair[2])).collectAsMap()

    # Generate all possible pairs of user indices and filter valid ones
    user_indices = list(user_to_businesses_map.keys())
    potential_pairs = [(user_indices[i], user_indices[j]) for i in range(len(user_indices)) for j in range(i+1, len(user_indices))]
    pairs_rdd = context.parallelize(potential_pairs)
    valid_pairs_rdd = pairs_rdd.filter(has_sufficient_common_businesses)

    # Create vertices and edges for the graph
    vertices = valid_pairs_rdd.flatMap(lambda pair: [pair[0], pair[1]]).distinct().map(lambda index: (user_to_index_map[index],))
    edges = valid_pairs_rdd.flatMap(lambda pair: [(pair[0], pair[1]), (pair[1], pair[0])]).distinct().map(lambda pair: (user_to_index_map[pair[0]], user_to_index_map[pair[1]]))

    # Map vertices to their neighbors
    nodes_neighbors = edges.groupByKey().mapValues(list).collectAsMap()

    # Calculate edge betweenness for the graph
    edge_betweenness = calculate_edge_betweenness(nodes_neighbors)

    # Convert betweenness scores to a format suitable for writing to a file
    betweenness_scores = [((list(edge)[0], list(edge)[1]), score) for edge, score in edge_betweenness.items()]
    edge_betweenness_rdd = context.parallelize(betweenness_scores)
    betweenness_scores_rdd = (edge_betweenness_rdd
                              .map(lambda x: ((min(list(x[0])[0], list(x[0])[1]), 
                                               max(list(x[0])[0], list(x[0])[1])), x[1]))
                              .sortBy(lambda x: (-x[1], x[0][0])))

    # Write betweenness scores to a file
    write_betweenness_to_file(betweenness_scores_rdd.collect(), output_betweenness_path)

    # Apply the Girvan-Newman algorithm to detect communities
    communities = girvan_newman_algorithm(edge_betweenness, nodes_neighbors)

    # Process communities for output
    communities = [sorted(list(community)) for community in communities]
    communities.sort(key=lambda x: (len(x), x[0]))

    # Write detected communities to a file
    write_communities_to_file(communities, output_communities_path)

    # Calculate and print total execution time
    end_time = time.time()
    print('Duration: ', end_time - start_time)
