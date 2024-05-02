# DSCI 553: Assignment 2 - SON Algorithm with Spark

## Overview

This folder contains the Python scripts developed for Assignment 2 of the DSCI 553 course, focusing on the implementation of the SON Algorithm to identify frequent itemsets within large datasets. The assignment uses both simulated and real-world datasets to demonstrate distributed data processing capabilities.

## Files

This folder includes two primary scripts:

- `task1.py`: Implementation of the SON Algorithm for a simulated dataset.
- `task2.py`: Extension of the SON Algorithm for the real-world Ta Feng dataset.

### Task 1: Simulated Data Analysis

**Objective**: Apply the SON Algorithm to identify frequent business and user itemsets from a simulated dataset.

**Execution**:

```bash
spark-submit --executor-memory 4G --driver-memory 4G task1.py <case_number> <support> <input_file_path> <output_file_path>
```

**Input**:

- `case_number`: 1 for businesses, 2 for users.
- `support`: Minimum support threshold for frequent itemsets.
- `input_file_path`: Path to the input CSV file.
- `output_file_path`: Path for saving the output results.

**Output**:

- Duration of execution printed on the console.
- JSON formatted output file detailing both intermediate candidates and final frequent itemsets.

### Task 2: Real-World Data Analysis (Ta Feng Dataset)

**Objective**: Implement the SON Algorithm on the Ta Feng dataset to analyze customer purchasing patterns.

**Execution**:

```bash
spark-submit --executor-memory 4G --driver-memory 4G task2.py <filter_threshold> <support> <input_file_path> <output_file_path>
```

**Input**:

- `filter_threshold`: Threshold to filter users by number of transactions per day.
- `support`: Minimum support threshold for itemsets.
- `input_file_path`: Path to the processed CSV file.
- `output_file_path`: Path for saving the output results.

**Output**:

- Duration of execution printed on the console.
- JSON formatted output file with the identified frequent itemsets.

## Technologies Used

- Python 3.6
- Apache Spark 3.1.2
- JDK 1.8
- Scala 2.12 (Optional for additional implementation)

## Setup

To run these scripts, ensure your environment is configured with the specified versions of Python, JDK, Scala, and Spark. These scripts are optimized for execution on Vocareum as part of the course infrastructure but can be adapted for other environments that support Apache Spark.

## Usage

Replace placeholder paths in the execution commands with actual file paths. Verify that the Spark setup matches the course specifications before running the scripts.
