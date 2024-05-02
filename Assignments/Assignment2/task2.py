from pyspark import SparkConf, SparkContext
import time
import sys
import itertools 
from datetime import datetime


# Initialize Spark with a given application name
conf = SparkConf().setAppName("Assignment2_Task2")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")  # Set log level to ERROR to avoid verbose logging


# Helper function to load and preprocess data from a file
def preprocess_ta_feng_data(input_file, output_file):

    # Read the data 
    rdd = sc.textFile(input_file)

    # Extract header
    header = rdd.first()
    header_broadcast = sc.broadcast(header) # Broadcast the header
    rdd = rdd.filter(lambda line: line != header_broadcast.value)

    # Process the data using mapPartitions
    def process_lines(lines):
        for line in lines:
            # Remove double quotes and split the line
            parts = [item.replace('"', '') for item in line.split(',')]
            date_str = parts[0]
            # Parse the date and format it as desired
            formatted_date = date_str[:6] + date_str[8:10]
            customer_id = parts[1]
            product_id = str(int(parts[5]))  # Convert to integer and back to string to remove leading zeros
            date_customer_id = formatted_date + "-" + customer_id
            yield date_customer_id + "," + product_id

    processed_rdd = rdd.mapPartitions(process_lines)
    
    # Collect the results and write to the output file
    results = processed_rdd.collect()
    with open(output_file, 'w') as file:
        file.write("DATE-CUSTOMER_ID,PRODUCT_ID\n")  # write header first
        for line in results:
            file.write(line + "\n")


# Helper function to load and preprocess data from a file
def load_data_ta_feng(file_path):
    # Reading the file and splitting each line by commas
    data = sc.textFile(file_path).map(lambda line: line.split(','))
    
    # Removing the header row
    header = data.first()
    data = data.filter(lambda row: row != header)  
    
    return data.groupByKey().mapValues(set)
    
def apriori_first(baskets, support):
    """
    Generate frequent single-item sets from the input data.
    
    Args:
    - baskets (list): List of baskets, each basket is represented as a tuple with the first element being 
                     its ID and the second element being a list of items.
    - support (int): The minimum support count threshold for an item to be considered frequent.
    
    Returns:
    - list: List of frequent single-item sets.
    """
    # Initializing an empty dictionary to keep count of item frequencies
    single_freq = {}

    # Loop through each basket's items and count item frequencies
    for _, items in baskets:
        for item in items:
            single_freq[item] = single_freq.get(item, 0) + 1

    # Get items which are frequent (i.e., have a count >= support)
    single_frequent_items_list = [tuple([k]) for k, v in single_freq.items() if v >= support]

    return single_frequent_items_list

def apriori_next(baskets, support, previous, size):
    """
    Generate frequent item sets of size `size+1` based on the previously found frequent item sets.
    
    Args:
    - baskets (list): List of baskets.
    - support (int): The minimum support count threshold.
    - previous (list): List of previously found frequent item sets.
    - size (int): Size of the item sets in `previous`.
    
    Returns:
    - list: List of frequent item sets of size `size+1`.
    """
    # Check if there are no previous frequent item sets
    if not previous:
        return []

    # Generate candidate item sets of size `size+1` by combining previously found frequent item sets
    temp_candidates = {
        tuple(sorted(set(item[0]).union(item[1])))
        for item in itertools.combinations(previous, 2)
        if len(set(item[0]).union(item[1])) == size + 1
    }

    # Count the occurrences of these candidates in the baskets
    candidates_count = {
        candidate: sum(1 for basket in baskets if set(candidate).issubset(basket[1]))
        for candidate in temp_candidates
    }

    # Filter candidates that meet the support threshold
    current_frequent_items = [itemset for itemset, count in candidates_count.items() if count >= support]
    
    return current_frequent_items

def son_phase1(iterator):
    """
    Phase 1 of the SON (Savasere, Omiecinski, and Navathe) algorithm to find candidate item sets.
    
    Args:
    - iterator (iterator): Iterator over baskets.
    
    Returns:
    - list: List of all candidate item sets.
    """
    # Convert iterator to list
    baskets = list(iterator)
    # Calculate the local support count threshold
    partition_support = local_support * len(baskets) 

    all_candidates = []

    # Get frequent single-item sets
    single_freq_items = apriori_first(baskets, partition_support)

    # Add them to the list of candidates
    all_candidates.extend(single_freq_items)

    # Iteratively find frequent item sets of increasing size
    current_size = 1
    current_freq_items = single_freq_items
    while True:
        previous_freq_items = current_freq_items
        current_freq_items = apriori_next(baskets, partition_support, previous_freq_items, current_size)
        if not current_freq_items:
            break
        all_candidates.extend(current_freq_items)
        current_size += 1

    return all_candidates

def son_phase2(iterator):
    """
    Phase 2 of the SON algorithm to count the occurrences of the candidate item sets.
    
    Args:
    - iterator (iterator): Iterator over baskets.
    
    Returns:
    - list: List of item sets with their counts.
    """
    # Convert iterator to list
    baskets = list(iterator)
    # Retrieve the broadcasted candidates
    candidates = broadcast_candidates.value
    item_counts = {}

    # Count the occurrences of each candidate in the baskets
    for _, basket in baskets:
        for _, candidate in candidates:
            for item in candidate:
                if set(item).issubset(basket):
                    item_counts[item] = item_counts.get(item, 0) + 1

    # Sort and return the result
    result = [(item, item_counts[item]) for item in sorted(item_counts.keys())]
    return [result]


def format_itemsets(itemsets):
    """
    Format the item sets for output.
    
    Args:
    - itemsets (list): List of item sets.
    
    Returns:
    - str: Formatted string of item sets.
    """

    sorted_items = sorted(list(itemsets))
    return ",".join([str(tuple(items)) for items in sorted_items]).replace(',)', ')')

def write_to_file(outputFile, all_candidates, all_frequent_itemsets):
    """
    Write the candidates and frequent item sets to an output file.
    
    Args:
    - outputFile (str): Path to the output file.
    - all_candidates (list): List of candidate item sets.
    - all_frequent_itemsets (list): List of frequent item sets.
    """
    with open(outputFile, 'w') as file:
        # Write all candidate item sets
        file.write('Candidates: \n')
        for _, candidates in sorted(all_candidates):
            file.write(format_itemsets(candidates) + '\n\n')

        # Write all frequent item sets
        file.write('Frequent Itemsets: \n')
        for _, frequent_itemsets in sorted(all_frequent_itemsets):
            file.write(format_itemsets(frequent_itemsets) + '\n\n')

# Main execution starts here
if __name__ == "__main__":
    
    # Parse command-line arguments for configuration
    filter_threshold = int(sys.argv[1])
    support_val = int(sys.argv[2])
    input_file_path = sys.argv[3]
    output_file_path = sys.argv[4]
    temp_file_path = 'customer_product.csv'

    # Load and preprocess data
    preprocess_ta_feng_data(input_file_path, temp_file_path)

    # Record start time for execution time calculation
    startTime = time.time()

    # Load data from the preprocessed file
    baskets = load_data_ta_feng(temp_file_path)

    
    # Filter out baskets with items less than the filter threshold
    baskets = baskets.filter(lambda x: len(x[1]) > filter_threshold)

    # Compute the local support threshold based on the global support value
    total_size = baskets.count()
    local_support = float(support_val) / total_size

    # SON Algorithm Phase 1: Finding candidate itemsets
    all_candidates = (baskets
                    .mapPartitions(son_phase1)
                    .distinct()
                    .groupBy(len)
                    .collect())
    
    # Broadcast the candidates to all worker nodes for phase 2
    broadcast_candidates = sc.broadcast(all_candidates)

    # SON Algorithm Phase 2: Counting the frequencies of candidate itemsets
    all_frequent_itemsets = (baskets
                    .mapPartitions(son_phase2)
                    .flatMap(lambda x: x)
                    .reduceByKey(lambda x, y: x + y)
                    .filter(lambda x: x[1] >= support_val)
                    .keys()
                    .groupBy(len)
                    .collect())
    
    # Write the results to an output file
    write_to_file(output_file_path, all_candidates, all_frequent_itemsets)

    # Compute and print the execution duration
    endTime = time.time()
    print("Duration: ", endTime - startTime)

