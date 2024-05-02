# DSCI 553: Assignment 1 - Data Exploration with Spark

## Overview
This folder contains the solutions for Assignment 1 of the DSCI 553 course, which involves performing data exploration tasks on the Yelp dataset using Apache Spark. The assignment is designed to familiarize students with Spark's RDD operations including transformations and actions.

## Files
The folder includes three Python scripts:
- `task1.py`: Analyzes review data to compute various statistics.
- `task2.py`: Demonstrates the effect of different data partition strategies on performance.
- `task3.py`: Explores reviews and business data to compute average ratings per city.

### Task 1
**Objective**: Analyze `test_review.json` to compute:
- Total number of reviews
- Number of reviews in 2018
- Number of distinct users and businesses reviewed
- Top 10 users and businesses with the most reviews

**Execution**:
```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task1.py <review_filepath> <output_filepath>
```

### Task 2
**Objective**: Examine and optimize the data partitioning of the RDD used in Task 1, Question F, and measure the performance impact.

**Execution**:
```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task2.py <review_filepath> <output_filepath> <n_partition>
```

### Task 3
**Objective**: Combine review and business data to compute the average stars for each city and compare two methods for calculating and sorting these averages.

**Execution**:
```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task3.py <review_filepath> <business_filepath> <output_filepath_question_a> <output_filepath_question_b>
```

## Technologies Used
- Python 3.6
- Apache Spark 3.1.2
- JDK 1.8
- Scala 2.12 (Optional for additional tasks)

## Setup
To run these scripts, you will need a Spark environment configured according to the versions mentioned above. The scripts are developed to be executed on Vocareum as part of the course's infrastructure but can be adapted for other environments that support Apache Spark.

## Usage
Replace `<review_filepath>`, `<business_filepath>`, `<output_filepath>`, `<output_filepath_question_a>`, and `<output_filepath_question_b>` with your actual file paths. Ensure that Spark is correctly installed and configured to use the provided scripts.

.