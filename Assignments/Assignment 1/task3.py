
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import avg, col
import sys
import time
import json


def main():

    # Initialize Spark
    conf = SparkConf().setAppName("Assignment1_Task2")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # Set the input and output path
    review_filepath = sys.argv[1]
    business_filepath = sys.argv[2]
    output_filepath_question_a = sys.argv[3]
    output_filepath_question_b = sys.argv[4]

    # Read the input file
    review_data = sc.textFile(review_filepath).map(lambda line: json.loads(line))
    business_data = sc.textFile(business_filepath).map(lambda line: json.loads(line))

    # print("Number of cities in business data: ", business_data.map(lambda x: x['city']).distinct().count())

    # Extract necessary fields from both datasets
    reviews = review_data.map(lambda x: (x['business_id'], x['stars']))
    businesses = business_data.map(lambda x: (x['business_id'], x['city']))

    # Join datasets on business_id
    joined_data = reviews.join(businesses)

    # Compute average stars for each city
    sum_counts = joined_data.map(lambda x: (x[1][1], (x[1][0],1))).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    average_stars = sum_counts.mapValues(lambda x: x[0] / x[1])

    # # Write results for Task 3(A)
    # sorted_results = average_stars.sortBy(lambda x: (-x[1], x[0])).map(lambda x: f"{x[0]},{x[1]}")
    # header = sc.parallelize(["city,stars"])
    # final_results = header.union(sorted_results).coalesce(1)
    # final_results.saveAsTextFile(output_filepath_question_a)

    # Write results for Task 3(A)
    sorted_results = average_stars.sortBy(lambda x: (-x[1], x[0])).map(lambda x: f"{x[0]},{x[1]}")
    results_list = ["city,stars"] + sorted_results.collect()

    # print(results_list)

    # Write data to file using Python's I/O
    with open(output_filepath_question_a, 'w') as f:
        for line in results_list:
            f.write(f"{line}\n")

    # Method 1 (M1) for Task 3(B)
    start_m1 = time.time()
    top10_cities_m1 = sorted(average_stars.collect(), key=lambda x: (-x[1], x[0]))[:10]
    end_m1 = time.time()
    duration_m1 = end_m1 - start_m1

    # Method 2 (M2) for Task 3(B)
    start_m2 = time.time()
    top10_cities_m2 = average_stars.sortBy(lambda x: (-x[1], x[0])).take(10)

    end_m2 = time.time()
    duration_m2 = end_m2 - start_m2

    # Write results for Task 3(B)
    results = {
        "m1": duration_m1,
        "m2": duration_m2,
        "reason": "In small dataset, as our result for test_review.json, the python is quicker than Spark. This is because Python's sorting algorithms (such as Timsort) are optimized for data in memory without taking into account distributed processing and communication overhead. But I believe, sorting in native Python (M1) might be slower than Spark's distributed sorting (M2) especially for large datasets. The primary reason for this disparity is the underlying architecture and capabilities of the two methods. Spark is designed to process large datasets in a distributed manner across multiple nodes in a cluster. It breaks down the data into partitions and processes each partition in parallel on different nodes. This parallel processing capability allows Spark to leverage the combined power of multiple machines, making tasks like sorting more efficient, especially for substantial datasets. In contrast, native Python sorting typically runs on a single machine and might not be optimized for large-scale distributed processing. As the dataset size grows, the advantages of Spark's distributed architecture become more evident, leading to a noticeable difference in execution times."
    }

    with open(output_filepath_question_b, 'w') as outfile:
        json.dump(results, outfile)

    sc.stop()

    
if __name__ == "__main__":
    main()
