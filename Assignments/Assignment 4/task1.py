from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from graphframes import GraphFrame
import time
import sys

# Initialize the Spark configuration with an application name
conf = SparkConf().setAppName("CommunityDetection")
context = SparkContext(conf=conf)
# Set log level to ERROR to reduce console output for readability
context.setLogLevel("ERROR")
# Create a Spark session from the Spark context
spark_session = SparkSession(context)

# Function to load review data from a CSV file
def load_reviews(file_path):
    # Read data from the file, splitting each line into elements of an array
    raw_data = context.textFile(file_path).map(lambda line: line.split(','))
    # Take the first row as header
    header = raw_data.first()
    # Filter out the header from the data
    data_without_header = raw_data.filter(lambda row: row != header)
    return data_without_header

# Function to check if a pair of users share a sufficient number of common businesses
def has_sufficient_common_businesses(pair):
    # Unpack the pair tuple
    user1, user2 = pair
    # Retrieve the set of businesses for each user
    businesses_of_user1 = set(user_to_businesses_map[user1])
    businesses_of_user2 = set(user_to_businesses_map[user2])
    # Return True if the intersection meets or exceeds the threshold
    return len(businesses_of_user1.intersection(businesses_of_user2)) >= common_business_threshold

# Function to write the detected communities to a file
def write_communities_to_file(communities, output_path):
    # Open the output file in write mode
    with open(output_path, 'w') as file:
        # Iterate through the communities and write them to the file
        for community in communities:
            # Convert each community to a string and write it to the file
            community_as_string = ", ".join(f"'{member}'" for member in community)
            file.write(f"{community_as_string}\n")

if __name__ == '__main__':
    start_time = time.time()

    # Read command line arguments for the threshold, input file, and output file
    common_business_threshold = int(sys.argv[1])
    input_file_path = sys.argv[2]
    output_file_path = sys.argv[3]

    # Load the review data
    reviews_rdd = load_reviews(input_file_path)

    # Map each user to a list of businesses they reviewed, and reduce by key to concatenate the lists
    user_business_pairs = reviews_rdd.map(lambda review: (review[0], [review[1]])).reduceByKey(lambda a, b: a + b)
    # Filter out users with a number of reviews below the threshold
    users_with_enough_reviews = user_business_pairs.filter(lambda pair: len(pair[1]) >= common_business_threshold)

    # Assign a unique index to each user and map them to their businesses
    user_business_indexed = users_with_enough_reviews.zipWithIndex().map(lambda pair: (pair[1], pair[0][0], pair[0][1]))
    # Create a map from user index to user ID
    user_to_index_map = user_business_indexed.map(lambda pair: (pair[0], pair[1])).collectAsMap()
    # Create a map from user index to the list of businesses they reviewed
    user_to_businesses_map = user_business_indexed.map(lambda pair: (pair[0], pair[2])).collectAsMap()

    # Generate all possible pairs of user indices
    user_indices = list(user_to_businesses_map.keys())
    potential_pairs = [(user_indices[i], user_indices[j]) for i in range(len(user_indices)) for j in range(i+1, len(user_indices))]

    # Parallelize the list of pairs and filter by those with sufficient common businesses
    pairs_rdd = context.parallelize(potential_pairs)
    valid_pairs_rdd = pairs_rdd.filter(has_sufficient_common_businesses)

    # Map the user indices to user IDs for vertices and edges of the graph
    vertices_df = valid_pairs_rdd.flatMap(lambda pair: [pair[0], pair[1]]).distinct().map(lambda index: (user_to_index_map[index],)).toDF(['id'])
    edges_df = valid_pairs_rdd.flatMap(lambda pair: [(pair[0], pair[1]), (pair[1], pair[0])]).distinct().map(lambda pair: (user_to_index_map[pair[0]], user_to_index_map[pair[1]])).toDF(['src', 'dst'])

    # Create the GraphFrame
    graph = GraphFrame(vertices_df, edges_df)
    # Apply the label propagation algorithm to detect communities
    label_propagation_results = graph.labelPropagation(maxIter=5)

    # Process the results to format them as communities
    communities = (label_propagation_results
                   .rdd
                   # Map each row to a tuple with its label and ID
                   .map(lambda row: (row['label'], [row['id']]))
                   # Reduce by key to group IDs by their label
                   .reduceByKey(lambda x, y: x + y)
                   # Sort the communities by size and then by the ID
                   .mapValues(sorted)
                   .sortBy(lambda community: (len(community[1]), community[1]))
                   # Extract the communities
                   .values()
                   .collect())

    # Write the detected communities to the output file
    write_communities_to_file(communities, output_file_path)

    # Calculate and print the duration of the entire operation
    end_time = time.time()
    print("Duration: ", end_time - start_time)
