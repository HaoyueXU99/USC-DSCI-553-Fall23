'''
Method Description:

This script uses PySpark and XGBoost to analyze and predict data from Yelp. Designed for large datasets, it 
leverages Spark's distributed computing for efficient processing. The script is structured into functions for 
data reading, preprocessing, and modeling, each tailored to handle specific aspects of the Yelp dataset.

Key Components:
1. Data Reading: 'read_folder_input' and 'read_file_input' functions read data in CSV and JSON formats. They 
   are integral in managing Yelp's diverse data structures, encompassing reviews, business information, and user data.

2. Data Preprocessing and Feature Engineering: 'preprocess_data' merges Yelp data with business and user information.
   The script innovatively includes features like total checkins, photo counts, and detailed tip data (like tip text length,
   likes, and tip submission date in Unix time). These features enrich the dataset, providing a nuanced understanding
   of user interactions and business characteristics.

3. Predictive Modeling with XGBoost: The script utilizes the XGBoost model for predictions. A grid search method optimized
   the model parameters (eta, max_depth, min_child_weight, gamma, subsample), significantly enhancing accuracy and efficiency.
   This rigorous parameter tuning process ensures the model is finely adjusted to the intricacies of the Yelp data.

4. Output and Performance: 'write_predictions_to_output' finalizes the process by saving predictions to a file. The script
   also logs execution time, emphasizing performance in data processing.

This script's approach, particularly in feature selection and model optimization, demonstrates a creative and effective 
method for handling complex, large-scale datasets. The inclusion of unique features like tip text length and submission 
date, along with traditional features like checkins and photo counts, provides a comprehensive view of the dataset, 
leading to more accurate predictions and insights.

Error Distribution:
>=0 and <1: 106910                                                                                                                                                              
>=1 and <2: 34256                                                                                                                                                               
>=2 and <3: 6396                                                                                                                                                                
>=3 and <4: 805                                                                                                                                                                 
>=4: 0    

RMSE: 
0.9758  

Execution Time:
131.79222226142883

'''





from pyspark import SparkContext, SparkConf

import time
import sys
import numpy as np
import xgboost as xgb
import json
import csv
from datetime import datetime

# Setup Spark Configuration for running the application
spark_conf = SparkConf().setAppName("Assignment3_Task1")
sc = SparkContext(conf=spark_conf)
sc.setLogLevel("ERROR")  # Reduce verbosity to only display error logs

import json

def read_folder_input(folder_path):
    """
    Read input data from a folder and return RDDs.

    Args:
        folder_path (str): Path to the folder containing data files.

    Returns:
        Tuple: RDDs for yelp_train, yelp_val, business, user, checkin, tip, and photo data.
    """

    # Read yelp_train data from CSV
    yelp_train = sc.textFile(f'{folder_path}/yelp_train.csv')
    first_line = yelp_train.first()
    yelp_train = yelp_train.filter(lambda line: line != first_line).map(lambda line: line.split(","))

    # Read yelp_val data from CSV
    yelp_val = sc.textFile(f'{folder_path}/yelp_val.csv')
    first_line_val = yelp_val.first()
    yelp_val = yelp_val.filter(lambda line: line != first_line_val).map(lambda line: line.split(","))

    # Read business data from JSON
    business = sc.textFile(f'{folder_path}/business.json').map(json.loads)

    # Read user data from JSON
    user = sc.textFile(f'{folder_path}/user.json').map(json.loads).map(lambda x: (x['user_id'], x['average_stars'], x['review_count'], x['useful'], x['funny'], x['cool']))

    # Read checkin data from JSON
    checkin = sc.textFile(f'{folder_path}/checkin.json').map(json.loads)

    # Read tip data from JSON
    tip = sc.textFile(f'{folder_path}/tip.json').map(json.loads)

    # Read photo data from JSON
    photo = sc.textFile(f'{folder_path}/photo.json').map(json.loads)

    return yelp_train, yelp_val, business, user, checkin, tip, photo


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

def convert_to_unix_time(date_str):
    return int(datetime.strptime(date_str, '%Y-%m-%d').timestamp())

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
    business_rdd = business_data.map(lambda x: (x['business_id'], (x['latitude'],x['longitude'],x['stars'], x['review_count'])))
    user_rdd = user_data.map(lambda x: (x[0], x[1:6]))

    # First step: Join yelp_data and business
    def process_first_step(x):
        business_id, values = x
        user_data, business_data = values
        user_id = user_data[0]
        user_star = float(user_data[2]) if mode == 'train' else None
        latitude, longitude, business_star, business_review_count = business_data
        return (user_id, (business_id, user_star, latitude, longitude, business_star, business_review_count))

    processed_rdd = yelp_data.map(lambda x: (x[1], x)).join(business_rdd).map(process_first_step)

    # Second step: Join the result with user
    def process_second_step(x):
        user_id, values = x
        business_data, user_data = values
        business_id, user_star, latitude, longitude, business_star, business_review_count = business_data
        avg_stars, review_count, useful, funny, cool = user_data
        if user_star is None:
            return (user_id, business_id, latitude, longitude, business_star, business_review_count, avg_stars, review_count, useful, funny, cool)

        return (user_id, business_id, user_star,latitude, longitude, business_star, business_review_count, avg_stars, review_count, useful, funny, cool)

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

    yelp_train, yelp_val, business, user, checkin, tip, photo = read_folder_input(folder_path)
    yelp_test = read_file_input(test_file_name)


    
    yelp_train = yelp_train.union(yelp_val)

   
    def get_total_checkins(checkin_data):
        time_data = checkin_data['time']
        total_checkins = sum(time_data.values()) 
        return (checkin_data['business_id'], total_checkins)

    
    def process_merged_row_checkin(row):
        business_id, (other_info, total_checkins) = row
        if total_checkins is None:
            total_checkins = 0  
        return (business_id, *other_info, total_checkins)
    
    def process_merged_row_photo(row):
        business_id, (other_info, photo_count) = row
        if photo_count is None:
            photo_count = 0  
        return (business_id, *other_info, photo_count)



    # count checkin number
    total_checkins_rdd = checkin.map(get_total_checkins).persist()
    # count photo number
    photo_count_rdd = photo.map(lambda x: (x['business_id'], 1)).reduceByKey(lambda a, b: a + b).persist()



    train_rdd = preprocess_data(yelp_train, business, user, "train")

    train_rdd = train_rdd.map(lambda x: (x[1], x[0]) + x[2:])


    # add checkins
    train_rdd = train_rdd.map(lambda x: (x[0], x[1:])).leftOuterJoin(total_checkins_rdd)
    train_rdd = train_rdd.map(process_merged_row_checkin)


    # add photo number
    train_rdd = train_rdd.map(lambda x: (x[0], x[1:])).leftOuterJoin(photo_count_rdd)
    train_rdd = train_rdd.map(process_merged_row_photo)
    

    train_rdd = train_rdd.map(lambda x: (x[1], x[0]) + x[2:])

    length_of_train_rdd = train_rdd.count()
    # print("Before Length of train_rdd is:", length_of_train_rdd)



    def add_tip_features(tip_record):
        user_id = tip_record['user_id']
        business_id = tip_record['business_id']

        date_str = tip_record.get('date', '')
        try:
            date_unix = convert_to_unix_time(date_str) if date_str else 0
        except Exception:
            date_unix = 0

        likes = tip_record.get('likes', 0)
        if likes in [None, '']:
            likes = 0

        text = tip_record.get('text', '')
        text_length = len(text) if text else 0

        # return (user_id, business_id), (date_unix)
    
        return (user_id, business_id), (date_unix, likes, text_length)



    # transfer tip RDD
    tip_features_rdd = tip.map(add_tip_features)

    length_of_tip_rdd = tip_features_rdd.count()
    # print("Length of tip_rdd is:", length_of_tip_rdd)


    # add tip features
    train_rdd = train_rdd.map(lambda x: ((x[0], x[1]), x[2:]))
      
    length_of_train_rdd = train_rdd.count()
    # print("Middle Length of train_rdd is:", length_of_train_rdd)


    train_rdd = train_rdd.leftOuterJoin(tip_features_rdd)

    length_of_train_rdd = train_rdd.count()
    # print("leftOuterJoin Length of train_rdd is:", length_of_train_rdd)

    train_rdd = train_rdd.map(lambda x: (x[0][0], x[0][1]) + x[1][0] + (x[1][1] if x[1][1] is not None else (0,0,0)))

    length_of_train_rdd = train_rdd.count()
    # print("After Length of train_rdd is:", length_of_train_rdd)

    # print("Sample train data:", train_rdd.take(5))

    features_and_labels = np.array(train_rdd.map(lambda x: x[2:]).collect())
    X_train = features_and_labels[:, 1:]
    y_train = features_and_labels[:, 0]

    length_of_train_rdd = train_rdd.count()
    # print("Length of train_rdd is:", length_of_train_rdd)


    ###### test ######

    test_rdd = preprocess_data(yelp_test, business, user, "test")
    
    test_rdd = test_rdd.map(lambda x: (x[1], x[0]) + x[2:])
    


    # add checkins
    test_rdd_checkins = test_rdd.map(lambda x: (x[0], x[1:])).leftOuterJoin(total_checkins_rdd)
    test_rdd = test_rdd_checkins.map(process_merged_row_checkin)


    # add photo number
    test_rdd = test_rdd.map(lambda x: (x[0], x[1:])).leftOuterJoin(photo_count_rdd)
    test_rdd = test_rdd.map(process_merged_row_photo)

    test_rdd = test_rdd.map(lambda x: (x[1], x[0]) + x[2:])

    length_of_test_rdd = test_rdd.count()
    # print("Before Length of test_rdd is:", length_of_test_rdd)

    # add tip features
    test_rdd = test_rdd.map(lambda x: ((x[0], x[1]), x[2:]))
    test_rdd = test_rdd.leftOuterJoin(tip_features_rdd).map(lambda x: (x[0][0], x[0][1]) + x[1][0] + (x[1][1] if x[1][1] is not None else (0,0,0)))

    length_of_test_rdd = test_rdd.count()
    #print("After Length of test_rdd is:", length_of_test_rdd)

    # sample_elements = test_rdd.take(5)
    # for element in sample_elements:
    #     print(type(element))
    #     print(element)


    X_test = np.array(test_rdd.collect())

    xgbr = xgb.XGBRegressor(objective ='reg:linear',
                            eta = 0.25,
                            max_depth = 6,
                            min_child_weight = 2,
                            gamma = 0.1,
                            subsample = 1.0,
                            n_estimators = 100)  
    
    xgbr.fit(X_train, y_train)

    y_pred = xgbr.predict(X_test[:, 2:].astype(float)).reshape(-1, 1)
    X_prediction = sc.parallelize(np.hstack((X_test[:, :2], y_pred)))

    # print("Sample predictions:", X_prediction.take(5))
    # print("Number of predictions:", X_prediction.count())

    paired_rdd = X_prediction.map(lambda x: ((x[0], x[1]), float(x[2])))

    paired_rdd = paired_rdd.mapValues(lambda x: (x, 1))

    sum_count_rdd = paired_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    average_rdd = sum_count_rdd.mapValues(lambda x: x[0] / x[1])

    formatted_rdd = average_rdd.map(lambda x: (x[0][0], x[0][1], x[1]))


    # print("Sample predictions:", formatted_rdd.take(5))
    # print("Number of predictions:", formatted_rdd.count())


    write_predictions_to_output(formatted_rdd, output_file_name)

    end_time = time.time()
    print('Duration: ', end_time - start_time)

