
from pyspark import SparkConf, SparkContext
import json
import sys
import time

def custom_partitioner(key):
    # Using a simple hash-based partitioning based on the business_id
    return hash(key)

def main():
    # Initialize Spark
    conf = SparkConf().setAppName("Assignment1_Task2")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # Set the input and output path
    input_filepath = sys.argv[1]
    output_filepath = sys.argv[2]
    n_partition = int(sys.argv[3])

    # Read the input file
    reviews = sc.textFile(input_filepath).map(lambda line: json.loads(line))

    # Default Partitioning

    default_start = time.time()

    top10_business_default_RDD = (reviews.map(lambda x: (x["business_id"], 1))
                                  .reduceByKey(lambda a, b: a + b)
                                  .sortBy(lambda x: (-x[1], x[0])))
    
    
    top10_business_default_RDD.take(10)
    default_end = time.time()

    default_n_partitions = top10_business_default_RDD.getNumPartitions()
    default_n_items = top10_business_default_RDD.glom().map(len).collect()

    default_duration = default_end - default_start

    # Custom Partitioning

    custom_start = time.time()

    reviews_custom = reviews.map(lambda x: (x["business_id"], 1)).partitionBy(n_partition, custom_partitioner)

    top10_business_custom_RDD = (reviews_custom
                                .reduceByKey(lambda a, b: a + b)
                                .sortBy(lambda x: (-x[1], x[0])))


    top10_business_custom_RDD.take(10)

    custom_end = time.time()

    custom_n_partitions = top10_business_custom_RDD.getNumPartitions()
    custom_n_items = top10_business_custom_RDD.glom().map(len).collect()

    custom_duration = custom_end - custom_start


    # Write results to JSON
    results = {
        "default": {
            "n_partition": default_n_partitions,
            "n_items": default_n_items,
            "exe_time": default_duration
        },
        "customized": {
            "n_partition": custom_n_partitions,
            "n_items": custom_n_items,
            "exe_time": custom_duration
        }
    }

    with open(output_filepath, 'w') as outfile:
        json.dump(results, outfile)

    sc.stop()

if __name__ == "__main__":
    main()
