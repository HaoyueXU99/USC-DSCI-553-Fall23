from pyspark import SparkContext, SparkConf

import time
import sys
import numpy as np
import xgboost as xgb
import json
import csv

# Setup Spark Configuration for running the application
spark_conf = SparkConf().setAppName("Assignment3_Task1")
sc = SparkContext(conf=spark_conf)
sc.setLogLevel("ERROR")  # Reduce verbosity to only display error logs

def read_folder_input(folder_path):
    """
    Read input data from a folder and return RDDs.

    Args:
        folder_path (str): Path to the folder containing data files.

    Returns:
        Tuple: RDDs for yelp_train, business, and user data.
    """

    # Read yelp_train data
    yelp_train = sc.textFile(f'{folder_path}/yelp_train.csv')
    first_line = yelp_train.first()
    yelp_train = yelp_train.filter(lambda line: line != first_line).map(lambda line: line.split(","))

    # Read business data from JSON
    business = sc.textFile(f'{folder_path}/business.json').map(json.loads)

    # Read user data from JSON
    user = sc.textFile(f'{folder_path}/user.json').map(json.loads).map(lambda x: (x['user_id'], x['average_stars'], x['review_count'], x['useful'], x['funny'], x['cool']))
    
    return yelp_train, business, user

def read_file_input(file_name):
    """
    Read input data from a file and return an RDD of tuples.

    Args:
        file_name (str): Name of the input file.

    Returns:
        RDD: RDD of input data tuples.
    """
    lines = sc.textFile(file_name)
    first_line = lines.first()

    remaining_lines = lines.filter(lambda line: line != first_line)
    num_columns = len(remaining_lines.first().split(","))

    def process_row(row):
        columns = row.split(",")
        if num_columns == 3:
            return (columns[0], columns[1], columns[2])
        elif num_columns == 2:
            return (columns[0], columns[1], None)
        else:
            raise ValueError("Unexpected number of columns in the row.")

    rdd = remaining_lines.map(process_row)
    return rdd

def preprocess_data(yelp_data, business_data, user_data, mode='train'):
    """
    Preprocess and join input data.

    Args:
        yelp_data (RDD): Yelp data RDD.
        business_data (RDD): Business data RDD.
        user_data (RDD): User data RDD.
        mode (str): Processing mode ('train' or 'test').

    Returns:
        RDD: Preprocessed data.
    """

    # Processing business and user data
    business_rdd = business_data.map(lambda x: (x['business_id'], (x['stars'], x['review_count'])))
    user_rdd = user_data.map(lambda x: (x[0], x[1:6]))

    # First step: Join yelp_data and business
    def process_first_step(x):
        business_id, values = x
        user_data, business_data = values
        user_id = user_data[0]
        user_star = float(user_data[2]) if mode == 'train' else None
        business_star, business_review_count = business_data
        return (user_id, (business_id, user_star, business_star, business_review_count))

    processed_rdd = yelp_data.map(lambda x: (x[1], x)).join(business_rdd).map(process_first_step)

    # Second step: Join the result with user
    def process_second_step(x):
        user_id, values = x
        business_data, user_data = values
        business_id, user_star, business_star, business_review_count = business_data
        avg_stars, review_count, useful, funny, cool = user_data
        if user_star is None:
            return (user_id, business_id, business_star, business_review_count, avg_stars, review_count, useful, funny, cool)

        return (user_id, business_id, user_star, business_star, business_review_count, avg_stars, review_count, useful, funny, cool)

    processed_rdd = processed_rdd.join(user_rdd).map(process_second_step)

    # Only for 'train' mode
    if mode == 'train':
        processed_rdd = processed_rdd.map(lambda x: (hash(x), x)).sortByKey().map(lambda x: x[1])

    return processed_rdd

def write_predictions_to_output(predictions, output_filename):
    """
    Write predictions to an output file.

    Args:
        predictions (RDD): Predicted data.
        output_filename (str): Name of the output file.
    """
    with open(output_filename, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["user_id", "business_id", "prediction"])
        for row in predictions.collect():
            writer.writerow(row)

if __name__ == '__main__':
    start_time = time.time()

    folder_path = sys.argv[1]
    test_file_name = sys.argv[2]
    output_file_name = sys.argv[3]

    yelp_train, business, user = read_folder_input(folder_path)
    yelp_val = read_file_input(test_file_name)

    train_rdd = preprocess_data(yelp_train, business, user, "train")
    features_and_labels = np.array(train_rdd.map(lambda x: x[2:]).collect())
    X_train = features_and_labels[:, 1:]
    y_train = features_and_labels[:, 0]

    test_rdd = preprocess_data(yelp_val, business, user, "test")
    X_test = np.array(test_rdd.collect())

    xgbr = xgb.XGBRegressor(n_estimators=100, 
                            verbosity=0, 
                            eta=0.25, 
                            max_depth=6, 
                            random_state=42)
                            
    xgbr.fit(X_train, y_train)

    y_pred = xgbr.predict(X_test[:, 2:].astype(float)).reshape(-1, 1)
    X_prediction = sc.parallelize(np.hstack((X_test[:, :2], y_pred)))

    write_predictions_to_output(X_prediction, output_file_name)

    end_time = time.time()
    print('Duration: ', end_time - start_time)
