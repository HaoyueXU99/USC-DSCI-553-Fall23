
from pyspark import SparkConf, SparkContext
import sys
import csv
import time

# Setup Spark Configuration for running the application
spark_conf = SparkConf().setAppName("Assignment3_Task2_1")
sc = SparkContext(conf=spark_conf)
sc.setLogLevel("ERROR")  # Reduce verbosity to only display error logs


def read_input(file_name):
    """Read the input file and return an RDD of tuples."""
    lines = sc.textFile(file_name)
    first_line = lines.first()

    Remain_lines = lines.filter(lambda line: line != first_line)
    # Check the number of columns based on the header
    num_columns = len(Remain_lines.first().split(","))
    
    def process_row(row):
        # Split the row and determine the structure based on the number of columns
        columns = row.split(",")
        if num_columns == 3:  # For train data with user_id, business_id, stars
            return (columns[0], columns[1], columns[2])
        elif num_columns == 2:  # For validation data with user_id, business_id
            return (columns[0], columns[1], None)
        else:
            raise ValueError("Unexpected number of columns in the row.")
    
    # Apply the process_row function only on rows that are not the first line
    rdd = Remain_lines.map(process_row)
    return rdd

def compute_pearson_similarity(item_a, item_b, train_data):
    
    """Calculate Pearson similarity between two items."""

    business_to_user = train_data["business_to_users"]
    business_avg_ratings = train_data["business_avg_ratings"]
    business_user_ratings = train_data["business_to_user_ratings"]

    
    common_users = business_to_user[item_a] & business_to_user[item_b]
    
    if len(common_users) <= 1:
        return (5.0 - abs(business_avg_ratings[item_a] - business_avg_ratings[item_b])) / 5
    elif len(common_users) == 2:
        weights = [(5.0 - abs(float(business_user_ratings[item_a][user]) - float(business_user_ratings[item_b][user]))) / 5 for user in common_users]
        return sum(weights) / len(weights)
    else:
        ratings_a = [float(business_user_ratings[item_a][user]) for user in common_users]
        ratings_b = [float(business_user_ratings[item_b][user]) for user in common_users]
        
        avg_a = sum(ratings_a) / len(ratings_a)
        avg_b = sum(ratings_b) / len(ratings_b)

        numerator = sum([(rating_a - avg_a) * (rating_b - avg_b) for rating_a, rating_b in zip(ratings_a, ratings_b)])
        denominator = (sum([(rating_a - avg_a) ** 2 for rating_a in ratings_a]) ** 0.5) * (sum([(rating_b - avg_b) ** 2 for rating_b in ratings_b]) ** 0.5)

        if denominator == 0:
            return 0
        return numerator / denominator


def get_top_similarities(user, business, train_data):
    """Get top Pearson similarities between a given business and businesses the user has rated."""
   
    user_to_businesses = train_data["user_to_businesses"]
    business_to_user_ratings = train_data["business_to_user_ratings"]

    def get_similarity(b1, b2):
        sorted_pair = tuple(sorted((b1, b2)))
        if sorted_pair not in similarity_cache:
            similarity_cache[sorted_pair] = compute_pearson_similarity(b1, b2, train_data)
        return similarity_cache[sorted_pair]

    similarity_cache = {}
    similarities = [
        (get_similarity(business_known, business), float(business_to_user_ratings[business_known][user]))
        for business_known in user_to_businesses[user]
    ]

    return sorted(similarities, key=lambda x: -x[0])[:15]


def compute_predicted_rating(top_similarities, train_data):
    """Compute the predicted rating based on top Pearson similarities."""
    global_avg_rating = train_data["global_avg_rating"]

    numerator = sum([similarity * rating for similarity, rating in top_similarities])
    denominator = sum([abs(similarity) for similarity, _ in top_similarities])
    
    if denominator == 0:
        return global_avg_rating
    return numerator / denominator

def predict(user, business, train_data):
    """Predict user's rating for a business."""
    
    user_to_businesses = train_data["user_to_businesses"]
    business_to_users = train_data["business_to_users"]
    user_avg_ratings = train_data["user_avg_ratings"]
    global_avg_rating = train_data["global_avg_rating"]
    
    if user not in user_to_businesses:
        return global_avg_rating
    if business not in business_to_users:
        return user_avg_ratings[user]

    top_similarities = get_top_similarities(user, business, train_data)
    return compute_predicted_rating(top_similarities, train_data)


def main():
    start_time = time.time()

    train_filename = sys.argv[1]
    test_filename = sys.argv[2]
    output_filename = sys.argv[3]

    train_records = read_input(train_filename)
    test_records = read_input(test_filename)


    # {'business_id': {user_id1, user_id2, ...}}
    business_to_users = train_records.map(lambda x: (x[1], {x[0]})).reduceByKey(lambda a, b: a | b).collectAsMap()

    # {'user_id': {business_id1, business_id2, ...}}
    user_to_businesses = train_records.map(lambda x: (x[0], {x[1]})).reduceByKey(lambda a, b: a | b).collectAsMap()

    # {'business_id': avg_rating}
    business_avg_ratings = train_records.map(lambda x: (x[1], (float(x[2]), 1))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda total_and_count: total_and_count[0] / total_and_count[1]).collectAsMap()

    # {'user_id': avg_rating}
    user_avg_ratings = train_records.map(lambda x: (x[0], (float(x[2]), 1))) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda total_and_count: total_and_count[0] / total_and_count[1]).collectAsMap()


    # {'business_id_1': {'user_id_1': rating_1, 'user_id_2': rating_2, ...},
    # 'business_id_2': {'user_id_3': rating_3, 'user_id_4': rating_4, ...},...}
    business_to_user_ratings = train_records.map(lambda x: (x[1], (x[0], x[2]))) \
        .groupByKey() \
        .mapValues(lambda pairs: {user: rating for user, rating in pairs}) \
        .collectAsMap()

    global_avg_rating = train_records.map(lambda x: float(x[2])).mean()
    # print(global_avg_rating)

    train_data = {
        "business_to_users": business_to_users,
        "user_to_businesses": user_to_businesses,
        "business_avg_ratings": business_avg_ratings,
        "user_avg_ratings": user_avg_ratings,
        "business_to_user_ratings": business_to_user_ratings,
        "global_avg_rating": global_avg_rating
    }   



    predictions = test_records.map(lambda x: (x[0], x[1], predict(x[0], x[1], train_data)))


    # Write predictions to output file
    with open(output_filename, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["user_id", "business_id", "prediction"])
        for row in predictions.collect():
            writer.writerow(row)
    end_time = time.time()
    print("Duration: ", end_time - start_time)


if __name__ == "__main__":
    main()
