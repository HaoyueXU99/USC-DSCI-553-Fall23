
import csv

def compute_metrics(output_file, ground_truth_file):
    with open(output_file, 'r') as f:
        output_pairs = set(f.readlines())

    with open(ground_truth_file, 'r') as f:
        ground_truth_pairs = set(f.readlines())

    # True Positives
    tp = len(output_pairs.intersection(ground_truth_pairs))

    # False Positives
    fp = len(output_pairs - ground_truth_pairs)

    # False Negatives
    fn = len(ground_truth_pairs - output_pairs)

    # Precision and Recall
    precision = tp / (tp + fp)
    recall = tp / (tp + fn)

    return precision, recall

if __name__ == "__main__":
    output_file = "output.csv"
    ground_truth_file = "pure_jaccard_similarity.csv"
    precision, recall = compute_metrics(output_file, ground_truth_file)
    
    print("Precision:", precision)
    print("Recall:", recall)

