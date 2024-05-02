# DSCI 553: Assignment 6 - Clustering with the BFR Algorithm

## Overview

This folder contains my implementation of the Bradley-Fayyad-Reina (BFR) algorithm for Assignment 6 of the DSCI 553 course. This task involves clustering a synthetic dataset using specific distance measurements and handling outliers effectively.

## Files

The folder includes a single Python script:

- `task.py`: Implements the BFR algorithm to cluster data points and manage outliers.

### Task Description

**Objective**: Cluster synthetic data points using the BFR algorithm while accounting for outliers. The data is assumed to be normally distributed with independent dimensions.

**Execution**:

```bash
/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit task.py <input_file> <n_cluster> <output_file>
```

## Methodology

1. **Initial Clustering**: Load a fraction of the data and use K-Means to establish initial clusters.
2. **Outlier Handling**: Identify and manage outliers as Retained Set (RS).
3. **Refinement**: Periodically refine clusters using new data, merging clusters where appropriate.

## Technologies Used

- Python 3.6
- NumPy
- scikit-learn
- Apache Spark 3.1.2
- JDK 1.8

## Setup

Ensure your environment is correctly configured with the specified versions of Python, Spark, and other dependencies. This script is optimized for execution on Vocareum.

## Usage

Replace `<input_file>`, `<n_cluster>`, and `<output_file>` with actual file paths and parameters. Ensure the dataset and environment settings are correct to replicate the expected setup on Vocareum.

## Output

- **Intermediate Results**: Count of discard points, clusters in the compression set, compression points, and points in the retained set.
- **Clustering Results**: Data points indexed by their cluster assignments, with outliers marked as `-1`.
