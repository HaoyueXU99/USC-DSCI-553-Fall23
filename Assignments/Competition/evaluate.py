import pandas as pd
import sys
import numpy as np

def compute_error_distribution(yelp_train_path, output_path):
    # Load the dataframes
    yelp_train_df = pd.read_csv(yelp_train_path)
    task2_1_output_df = pd.read_csv(output_path)

    # Rename columns in task2_1_output_df to remove leading and trailing spaces
    task2_1_output_df.columns = [col.strip() for col in task2_1_output_df.columns]

    # Merge the dataframes
    merged_df = pd.merge(yelp_train_df, task2_1_output_df, on=['user_id', 'business_id'])

    # Compute the absolute differences
    merged_df['abs_diff'] = np.abs(merged_df['prediction'] - merged_df['stars'])

    # Categorize differences into levels
    error_distribution = {
        '>=0 and <1': np.sum((merged_df['abs_diff'] >= 0) & (merged_df['abs_diff'] < 1)),
        '>=1 and <2': np.sum((merged_df['abs_diff'] >= 1) & (merged_df['abs_diff'] < 2)),
        '>=2 and <3': np.sum((merged_df['abs_diff'] >= 2) & (merged_df['abs_diff'] < 3)),
        '>=3 and <4': np.sum((merged_df['abs_diff'] >= 3) & (merged_df['abs_diff'] < 4)),
        '>=4': np.sum(merged_df['abs_diff'] >= 4)
    }

    return error_distribution


def compute_rmse(yelp_train_path, output_path):
    # Load the dataframes
    yelp_train_df = pd.read_csv(yelp_train_path)
    
    # print("yelp_train_df:", yelp_train_df.head())
    task2_1_output_df = pd.read_csv(output_path)
    # print("task2_1_output_df:", task2_1_output_df.head())

    # Rename columns in task2_1_output_df to remove leading and trailing spaces
    task2_1_output_df.columns = [col.strip() for col in task2_1_output_df.columns]
    

    # Merge the dataframes
    merged_df = pd.merge(yelp_train_df, task2_1_output_df, on=['user_id', 'business_id'])
    # print(merged_df.head())

    # Compute RMSE
    rmse = ((merged_df['prediction'] - merged_df['stars'])**2).mean()**0.5
    return rmse

if __name__ == "__main__":
    yelp_train_path = sys.argv[1]
    output_path = sys.argv[2]  # Fixed this line to get the correct second argument

    distribution = compute_error_distribution(yelp_train_path, output_path)

    for level, count in distribution.items():
        print(f"{level}: {count}")


    result = compute_rmse(yelp_train_path, output_path)
    print(f"RMSE: {result:.4f}")
