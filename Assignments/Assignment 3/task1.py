# Standard Libraries
import sys
import time
import random
from itertools import combinations

# Third-party Libraries
from pyspark import SparkContext, SparkConf

# Constants defining the configuration for the locality sensitive hashing (LSH) algorithm.
HASH_FUNCTION_COUNT = 200
LARGE_PRIME = 1999999995
NUM_BANDS = 50
NUM_ROWS = 2

# Setup Spark Configuration for running the application
spark_conf = SparkConf().setAppName("Assignment3_Task1")
sc = SparkContext(conf=spark_conf)
sc.setLogLevel("ERROR")  # Reduce verbosity to only display error logs

def hash_functions(a, b, p, m, x):
    """Generate hash value using a given hash function."""
    return ((a * x + b) % p) % m

def min_hash_signature(user_set, user_mapping, a_params, b_params, prime, max_value):
    """Generate min hash signatures for a given user set."""
    signatures = []
    for a, b in zip(a_params, b_params):
        hashed_values = [hash_functions(a, b, prime, max_value, user_mapping[user]) for user in user_set]
        signatures.append(min(hashed_values))
    return signatures

def extract_bands(pair, num_bands, rows_per_band):
    """Segment the signature into multiple bands."""
    _, signature = pair
    
    def band_signature(band):
        start = band * rows_per_band
        end = (band + 1) * rows_per_band
        return tuple(signature[start:end])
    
    for band in range(num_bands):
        yield ((band, band_signature(band)), pair[0])

def similarity_measure(set1, set2):
    """Compute Jaccard similarity between two sets."""
    return len(set1.intersection(set2)) / len(set1.union(set2))

def compute_similarity(pair, business_to_users_bcast):
    """Compute the similarity for a given pair using the broadcasted business_to_users_map."""
    business_to_users_map = business_to_users_bcast.value
    key = f"{pair[0]},{pair[1]}"
    return key, similarity_measure(business_to_users_map[pair[0]], business_to_users_map[pair[1]])

def save_to_output(matches, output_path):
    """Save potential matches to a file."""
    with open(output_path, "w") as file:
        file.write("business_id_1, business_id_2, similarity\n")
        for pair, sim in sorted(matches.items()):
            file.write("{},{}\n".format(pair, sim))

if __name__ == '__main__':
    start_time = time.time()

    # Input and output paths from command line arguments
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Read data and preprocess
    raw_data = sc.textFile(input_path)
    header = raw_data.first()
    processed_data = raw_data.filter(lambda line: line != header).map(lambda line: line.split(","))

    # Mapping of businesses to users who reviewed them
    businesses_and_users = processed_data.map(lambda x: (x[1], {x[0]})).reduceByKey(lambda a, b: a.union(b))

    # Create a unique index for each user
    user_index = processed_data.map(lambda x: x[0]).distinct().zipWithIndex().collectAsMap()

    max_value = len(user_index)

    # Generate random parameters for hash functions
    random.seed(time.time())
    a_params = [random.randint(1, max_value - 1) for _ in range(HASH_FUNCTION_COUNT)]
    b_params = [random.randint(1, max_value - 1) for _ in range(HASH_FUNCTION_COUNT)]

    # Compute the signatures for each business using MinHash
    signatures_rdd = businesses_and_users.mapValues(lambda user_set: min_hash_signature(user_set, user_index, a_params, b_params, LARGE_PRIME, max_value))

    # Segment signatures into bands and find candidate pairs with potential similarity
    bands = (signatures_rdd.flatMap(lambda pair: extract_bands(pair, NUM_BANDS, NUM_ROWS))
             .groupByKey().filter(lambda x: len(x[1]) > 1))

    candidate_pairs_list = bands.flatMap(lambda x: combinations(sorted(x[1]), 2)).distinct().collect()

    # Broadcast the business_to_users map to all worker nodes for efficient similarity computation
    business_to_users_bcast = sc.broadcast(businesses_and_users.collectAsMap())

    candidate_pairs_rdd = sc.parallelize(candidate_pairs_list)

    # Compute the actual similarities for candidate pairs
    similarity_rdd = (candidate_pairs_rdd.map(lambda pair: compute_similarity(pair, business_to_users_bcast))
                      .filter(lambda x: x[1] >= 0.5))

    # Collect potential matches to the driver node
    potential_matches = similarity_rdd.collectAsMap()

    # Save potential matches to the output file
    save_to_output(potential_matches, output_path)

    # Print the elapsed time
    end_time = time.time()
    print("Duration: ", end_time - start_time)
