import pandas as pd
import sys

def compute_rmse(yelp_train_path, task2_1_output_path):
    # Load the dataframes
    yelp_train_df = pd.read_csv(yelp_train_path)
    task2_1_output_df = pd.read_csv(task2_1_output_path)

    # Rename columns in task2_1_output_df to remove leading and trailing spaces
    task2_1_output_df.columns = [col.strip() for col in task2_1_output_df.columns]

    # Merge the dataframes
    merged_df = pd.merge(yelp_train_df, task2_1_output_df, on=['user_id', 'business_id'])

    # Compute RMSE
    rmse = ((merged_df['prediction'] - merged_df['stars'])**2).mean()**0.5
    return rmse

if __name__ == "__main__":
    yelp_train_path = sys.argv[1]
    task2_output_path = sys.argv[2]  # Fixed this line to get the correct second argument
    result = compute_rmse(yelp_train_path, task2_output_path)
    print(f"RMSE: {result:.4f}")
