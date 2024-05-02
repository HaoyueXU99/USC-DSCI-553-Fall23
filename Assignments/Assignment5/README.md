# DSCI 553: Assignment 5 - Streaming Algorithms

## Overview

This folder contains my solutions for Assignment 5 of the DSCI 553 course, focusing on the implementation of three different streaming algorithms to process simulated data streams using the Yelp dataset.

## Files

This folder includes three Python scripts:

- `task1.py`: Implements the Bloom Filtering algorithm.
- `task2.py`: Implements the Flajolet-Martin algorithm for estimating the number of unique users.
- `task3.py`: Implements the Reservoir Sampling algorithm.

### Task 1: Bloom Filtering

**Objective**: Estimate whether a `user_id` in the data stream has been seen before using a Bloom filter.

- **Hash Functions**: Implement independent and uniformly distributed hash functions.
- **Filter Size**: Global filter bit array length is 69997.

**Execution**:

```bash
python task1.py <input_filename> stream_size num_of_asks <output_filename>
```

### Task 2: Flajolet-Martin Algorithm

**Objective**: Estimate the number of unique users within a window of the data stream.

- **Hash Functions**: Implement suitable hash functions for the algorithm.
- **Stream Size**: 300 users per stream.

**Execution**:

```bash
python task2.py <input_filename> stream_size num_of_asks <output_filename>
```

### Task 3: Fixed Size Sampling (Reservoir Sampling)

**Objective**: Maintain a sample of users from the stream using a reservoir of fixed size.

- **Reservoir Size**: Limited to 100 users.
- **Random Seed**: 553 (important for reproducibility).

**Execution**:

```bash
python task3.py <input_filename> stream_size num_of_asks <output_filename>
```

## Technologies Used

- Python 3.6
- Apache Spark 3.1.2
- JDK 1.8
- Scala 2.12 (Optional for additional implementation)

## Setup

To run these scripts, ensure your Spark and Python environments are correctly configured with the specified versions. These scripts are optimized for execution on Vocareum but can be adapted for other Spark-supported platforms.

## Usage

Replace `<input_filename>`, `stream_size`, `num_of_asks`, and `<output_filename>` with your actual file paths and parameters. Ensure the random seed and hash functions are set up as specified to match the grading simulations.

## Output

- **Task 1**: Outputs a CSV file logging the false positive rate for each batch of data.
- **Task 2**: Outputs a CSV file with estimations of unique users against the actual count.
- **Task 3**: Outputs the current state of the reservoir after every 100 users processed.
