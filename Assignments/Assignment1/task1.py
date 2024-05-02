from pyspark import SparkConf, SparkContext
import json
import sys

def main():
    # Initialize Spark
    conf = SparkConf().setAppName("Assignment1_Task1")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # Set the input and output path
    input_filepath = sys.argv[1]
    output_filepath = sys.argv[2]
    
    # Read the input file
    reviews = sc.textFile(input_filepath).map(lambda line: json.loads(line))

    # A. The total number of reviews
    n_review = reviews.count()

    # B. The number of reviews in 2018
    n_review_2018 = reviews.filter(lambda x: x["date"][:4] == "2018").count()

    # C. The number of distinct users who wrote reviews
    n_user = reviews.map(lambda x: x["user_id"]).distinct().count()

    # D. The top 10 users who wrote the largest numbers of reviews
    top10_user = (reviews.map(lambda x: (x["user_id"], 1))
                        .reduceByKey(lambda a, b: a + b)
                        .sortBy(lambda x: (-x[1], x[0]))
                        .take(10))
    
    # E. The number of distinct businesses that have been reviewed
    n_business = reviews.map(lambda x: x["business_id"]).distinct().count()

    # F. The top 10 businesses that had the largest numbers of reviews
    top10_business = (reviews.map(lambda x: (x["business_id"], 1))
                             .reduceByKey(lambda a, b: a + b)
                             .sortBy(lambda x: (-x[1], x[0]))
                             .take(10))
                             
    result = {
        "n_review": n_review,
        "n_review_2018": n_review_2018,
        "n_user": n_user,
        "top10_user": top10_user,
        "n_business": n_business,
        "top10_business": top10_business
    }

    # print(result)

    with open(output_filepath, "w") as outfile:
        json.dump(result, outfile)

    sc.stop()

if __name__ == "__main__":
    main()
