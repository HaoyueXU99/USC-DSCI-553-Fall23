# DSCI 553: Fall 2023 Competition - Advanced Recommendation System

## Overview

This repository contains my submission for the DSCI 553 competition project, aimed at enhancing the recommendation system developed in Assignment 3. The goal is to increase prediction accuracy and efficiency using various methods, including but not limited to hybrid recommendation systems.

## Files

The repository includes several Python scripts:

- `competition.py`: Main script to run the recommendation system.
- `compare_output.py`: Script to compare output with the ground truth and calculate error distribution.
- `evaluate.py`: Script to calculate the RMSE of the system's predictions.

### Competition Task

**Objective**: Improve the accuracy and efficiency of the recommendation system using Yelp data to predict user-business pair ratings.

**Execution**:

```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit competition.py <folder_path> <test_file_name> <output_file_name>
```

## Methodology

- **Data Handling**: Utilize Yelp data, including reviews, user metadata, business metadata, checkins, tips, and photo data to enhance recommendation accuracy.
- **Models Used**: Enhancements made to the model from Assignment 3, potentially including machine learning techniques and hybrid methods.
- **Validation**: Use the provided validation dataset to measure improvements and adjust parameters accordingly.

## Technologies Used

- Python 3.6
- Apache Spark 3.1.2
- Additional Python libraries as required

## Setup

Ensure your environment is configured correctly with Python 3.6 and Spark 3.1.2. This script is designed for execution on Vocareum but can be adapted for other environments with similar configurations.

## Usage

Replace `<folder_path>`, `<test_file_name>`, and `<output_file_name>` with your actual file paths and parameters. Ensure the dataset and environment settings are correct to replicate the expected setup on Vocareum.

## IMPORTANT HINTS

1. To achieve a lower RMSE, I recommend that you try adding more features from the existing training data. Based on my observations, the more features you add, the better your results will be.

2. Parameter Tuning: Try increasing the `n_estimators` parameter (more trees increase the model's complexity, allowing it to learn the data better) and the `max_depth` (greater depth enables the model to learn more detailed feature relationships in the data).