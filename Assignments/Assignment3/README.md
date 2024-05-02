# DSCI 553: Assignment 3 - LSH and Recommendation Systems

## Overview

This folder contains the solutions for Assignment 3 of the DSCI 553 course, which focuses on implementing Locality Sensitive Hashing (LSH) and building different types of recommendation systems using a subset of the Yelp dataset.

## Files

This folder includes four Python scripts corresponding to different components of the tasks:

- `task1.py`: Implements the Locality Sensitive Hashing (LSH) algorithm to find similar businesses based on Jaccard similarity.
- `task2_1.py`: Implements an item-based collaborative filtering recommendation system using Pearson similarity.
- `task2_2.py`: Develops a model-based recommendation system using a decision tree-based approach.
- `task2_3.py`: Combines the previous models to create a hybrid recommendation system.

### Task 1: Jaccard Based LSH

**Objective**: Identify pairs of businesses with a Jaccard similarity of at least 0.5 using the LSH algorithm.

**Execution**:

```bash
spark-submit --executor-memory 4G --driver-memory 4G task1.py <input_file_name> <output_file_name>
```

**Output**:

- A CSV file listing business pairs with their Jaccard similarity, sorted lexicographically.

### Task 2: Recommendation System

**Objective**: Predict user ratings for businesses using different types of recommendation systems.

#### Case 1: Item-based CF Recommendation System

**Execution**:

```bash
spark-submit --executor-memory 4G --driver-memory 4G task2_1.py <train_file_name> <test_file_name> <output_file_name>
```

#### Case 2: Model-based Recommendation System

**Execution**:

```bash
spark-submit --executor-memory 4G --driver-memory 4G task2_2.py <folder_path> <test_file_name> <output_file_name>
```

#### Case 3: Hybrid Recommendation System

**Execution**:

```bash
spark-submit --executor-memory 4G --driver-memory 4G task2_3.py <folder_path> <test_file_name> <output_file_name>
```

**Output for all cases**:

- A CSV file containing predictions for each user and business pair.

## Technologies Used

- Python 3.6
- Apache Spark 3.1.2
- JDK 1.8
- Scala 2.12 (Optional for additional tasks)

## Setup

Ensure your environment is configured with the specified versions of Python, JDK, Scala, and Spark. These scripts are designed to be executed on Vocareum but can be adapted for other environments.

## Usage

Replace placeholder paths in the execution commands with actual file paths. Ensure all files are correctly formatted and that the environment is properly set up as per the course requirements.
