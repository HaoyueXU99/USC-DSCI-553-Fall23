from sklearn.cluster import KMeans
import numpy as np
import time
import pyspark
import math
import sys
from itertools import combinations

SAMPLE_SIZE = [0.25, 0.333333, 0.5, 1.0]
#SAMPLE_SIZE = [0.25]
CLUSTER_PARAM = 5


class DS_or_CS:
    def __init__(self, data_points, data_indices):
        '''
        :para data_points: a list of data vector(list)
        '''
        self.N = len(data_points)
        self.SUM = [0] * len(data_points[0])
        self.SUMSQ = self.SUM.copy()
        for data_point in data_points:
            self.SUM = [a + b for a, b in zip(self.SUM, data_point)]
            self.SUMSQ = [a + b for a, b in zip(self.SUMSQ, [x_i ** 2 for x_i in data_point])]
        self.data_indices = data_indices

    def get_centroid(self):
        centroid = [sum_i / self.N for sum_i in self.SUM]
        return centroid

    def get_variance(self):
        centroid = self.get_centroid()
        return [(SUMSQ_i / self.N) - centroid_i ** 2 for SUMSQ_i, centroid_i in zip(self.SUMSQ, centroid)]

    def add_data_point(self, data_points, index):
        self.N += 1
        self.SUM = [a + b for a, b in zip(self.SUM, data_points)]
        self.SUMSQ = [a + b for a, b in zip(self.SUMSQ, [x_i ** 2 for x_i in data_points])]
        self.data_indices.append(index)

    def merge_ds_cs(self, other):
        self.N += other.N
        for i in range(len(self.SUM)):
            self.SUM[i] += other.SUM[i]
            self.SUMSQ[i] += other.SUMSQ[i]
        self.data_indices += other.data_indices


class RS:  # retained set
    def __init__(self, data_points_dict, rs_idx):
        self.data_points = {idx: data_points_dict[idx]
                            for idx in rs_idx}

    def add_data_points(self, data_points_dict):
        for index, data in data_points_dict.items():
            self.data_points[index] = data



def create_DS_CS(cluster_idx_dict, idx_list, data_dict):
    ds_cs_list = []
    for cluster in cluster_idx_dict:
        cluster_list = cluster_idx_dict[cluster]
        index_list = [idx_list[x] for x in cluster_list]
        data_point_list = [data_dict[x] for x in index_list]
        ds_cs_list.append(DS_or_CS(data_point_list, index_list))
    return ds_cs_list


def get_singles(arr):
    unique_elements, counts = np.unique(arr, return_counts=True)
    indices = np.where(counts == 1)[0]
    unique_indices = np.where(np.isin(arr, unique_elements[indices]))[0]
    return unique_indices


def assign_to_DS_CS(data_points_dict, DS_CS_list):
    if len(DS_CS_list) == 0:
        return data_points_dict
    used_data_indices = set()
    for idx, data_point in data_points_dict.items():
        distances = [get_mahalanobis_distance(data_point, DS_CS) for DS_CS in DS_CS_list]
        min_distance = min(distances)
        if min_distance < 2 * math.sqrt(len(data_point)):
            cluster_min = DS_CS_list[distances.index(min_distance)]
            cluster_min.add_data_point(data_point, idx)
            used_data_indices.add(idx)
    result_dict = {}
    for data in set(data_points_dict) - used_data_indices:
        result_dict[data] = data_points_dict[data]
    return result_dict


def get_mahalanobis_distance(data_point, DS_CS_set):
    centroid = DS_CS_set.get_centroid()
    variance = DS_CS_set.get_variance()
    distances = [(x_i - c_i) / math.sqrt(sigma_i)
                 for x_i, c_i, sigma_i in zip(data_point, centroid, variance)]
    distances_squared = [distance ** 2 for distance in distances]
    return math.sqrt(sum(distances_squared))


def merge_CS(cs_list):
    while len(cs_list) is not 1:
        data_dimension = len(cs_list[0].SUM)
        results = []
        for cs_1, cs_2 in combinations(cs_list, 2):
            distance = max(get_mahalanobis_distance(cs_1.get_centroid(), cs_2),
                           get_mahalanobis_distance(cs_2.get_centroid(), cs_1))
            results.append((cs_1, cs_2, distance))
        results.sort(key=lambda x: x[2])
        if results[0][2] < 2 * math.sqrt(data_dimension):
            results[0][0].merge_ds_cs(results[0][1])
            cs_list.remove(results[0][1])
        else:
            break
    return cs_list


def get_data_sum_DS_CS_list(DS_CS_list):
    return sum(map(lambda x: x.N, DS_CS_list))


if __name__ == "__main__":
    conf = pyspark.SparkConf().setAppName("BFR").setMaster("local[*]")
    sc = pyspark.SparkContext(conf=conf)
    sc.setLogLevel("WARN")


    start_time = time.time()

    # parse commandline argument
    input_file = sys.argv[1]
    n_cluster = int(sys.argv[2])
    output_file = sys.argv[3]

    dataRDD = sc.textFile(input_file).map(lambda x: x.split(","))
    # data idx, data value
    points_rdd = dataRDD.map(lambda x: (x[0], tuple(map(float, x[2:]))))
    # data idx, cluster idx
    truth_rdd = dataRDD.map((lambda x: (x[0], x[1])))

    # Step 1. Load 20% of the data randomly.
    init_sample_rdd = points_rdd.sample(withReplacement=False, fraction=0.2)
    init_sample_dict = init_sample_rdd.collectAsMap()
    init_sample_dict = {k: list(v) for k, v in init_sample_dict.items()}
    init_key_list = []
    init_val_list = []

    for k in init_sample_dict:
        init_key_list.append(k)
        init_val_list.append(init_sample_dict[k])

    # Step 2. Run K-Means (e.g., from sklearn) with a large K
    init_val_array = np.array(init_val_list)
    init_kmeans_obj = KMeans(n_clusters=CLUSTER_PARAM * n_cluster,
                             max_iter=100, random_state=233).fit(init_val_array)
    init_sample_label = init_kmeans_obj.labels_
    init_label_single = get_singles(init_sample_label)
    init_outlier = [init_key_list[x] for x in init_label_single]
    init_inlier = [x for x in init_sample_dict.keys() if x not in init_outlier]
    # Step 3. In the K-Means result from Step 2, move all the clusters that contain only one point to RS
    RS_init = RS(init_sample_dict, init_outlier)
    # Step 4. Run K-Means again to cluster the rest of the data points with K = the number of input clusters
    init_inlier_arr = np.array([init_sample_dict[x] for x in init_inlier])
    init_inlier_k = KMeans(n_clusters=n_cluster,
                           max_iter=100, random_state=233).fit(init_inlier_arr)
    init_inlier_label = init_inlier_k.labels_
    # Step 5. Use the K-Means result from Step 4 to generate the DS clusters
    inlier_clusters = dict()
    for index, cluster in enumerate(init_inlier_label):
        if cluster not in inlier_clusters:
            inlier_clusters[cluster] = []
        inlier_clusters[cluster].append(index)
    ds_list = create_DS_CS(inlier_clusters, init_inlier, init_sample_dict)
    # Step 6. Run K-Means on the points in the RS with a large K (e.g., 5 times of the number of the input
    # clusters) to generate CS (clusters with more than one points) and RS (clusters with only one point).
    RS_idx = []
    RS_data = []
    for i in RS_init.data_points:
        RS_idx.append(i)
        RS_data.append(RS_init.data_points[i])
    RS_data_arr = np.array(RS_data)

    kmeans_RS = KMeans(n_clusters=math.ceil(len(RS_init.data_points) / 2),
                       max_iter=100, random_state=233).fit(RS_data_arr)
    kmeans_RS_label = kmeans_RS.labels_

    RS_clusters = dict()
    for index, cluster in enumerate(kmeans_RS_label):
        if cluster not in RS_clusters:
            RS_clusters[cluster] = []
        RS_clusters[cluster].append(index)

    RS_outliers = []
    keys_to_delete = []
    for k, v in RS_clusters.items():
        if len(v) == 1:
            outlier_idx = v[0]
            RS_outliers.append(RS_idx[outlier_idx])
            keys_to_delete.append(k)

    for k in keys_to_delete:
        del RS_clusters[k]

    cs_list = create_DS_CS(RS_clusters, RS_idx, init_sample_dict)
    rs_set = RS(init_sample_dict, RS_outliers)
    # remove seen points
    points_rdd = points_rdd.subtract(init_sample_rdd)

    intermediate_result = [get_data_sum_DS_CS_list(ds_list), len(cs_list),
                           get_data_sum_DS_CS_list(cs_list), len(rs_set.data_points)]

    with open(output_file, 'w+') as f:
        f.write("The intermediate results:\n")
        f.write("Round {}: {}\n".format(str(1), ", ".join(map(str, intermediate_result))))
    # Step 7. Load another 20% of the data randomly.
        for iteration in range(len(SAMPLE_SIZE)):
            r = SAMPLE_SIZE[iteration]
            sampled_points_rdd = points_rdd.sample(withReplacement=False, fraction=r)
            sampled_dict = sampled_points_rdd.collectAsMap()
            # Step 8. For the new points, compare them to each of the DS using the Mahalanobis Distance and assign
            # them to the nearest DS clusters if the distance is < 2 𝑑.
            data_points = assign_to_DS_CS(sampled_dict, ds_list)
            # Step 9. For the new points that are not assigned to DS clusters, using the Mahalanobis Distance and
            # assign the points to the nearest CS clusters if the distance is < 2 �
            data_points = assign_to_DS_CS(data_points, cs_list)
            # Step 10. For the new points that are not assigned to a DS cluster or a CS cluster, assign them to RS.
            rs_set.add_data_points(data_points)
            # Step 11. Run K-Means on the RS with a large K (e.g., 5 times of the number of the input clusters) to
            # generate CS (clusters with more than one points) and RS (clusters with only one point).
            RS_idx = []
            RS_data = []
            for rs_i in rs_set.data_points:
                RS_idx.append(rs_i)
                RS_data.append(rs_set.data_points[rs_i])
            RS_data_arr = np.array(RS_data)
            # math.ceil(rs_set.N / 2)
            kmeans_RS = KMeans(n_clusters=math.ceil(len(rs_set.data_points) / 2),
                               max_iter=100, random_state=233).fit(RS_data_arr)
            kmeans_RS_label = kmeans_RS.labels_

            RS_clusters = dict()
            for index, cluster in enumerate(kmeans_RS_label):
                if cluster not in RS_clusters:
                    RS_clusters[cluster] = []
                RS_clusters[cluster].append(index)

            RS_outliers = []
            keys_to_delete = []
            for k, v in RS_clusters.items():
                if len(v) == 1:
                    outlier_idx = v[0]
                    RS_outliers.append(RS_idx[outlier_idx])
                    keys_to_delete.append(k)

            for k in keys_to_delete:
                del RS_clusters[k]

            cs_objs = create_DS_CS(RS_clusters, RS_idx, rs_set.data_points)
            cs_list.extend(cs_objs)
            rs_set = RS(rs_set.data_points, RS_outliers)
            # Step 12. Merge CS clusters that have a Mahalanobis Distance < 2 𝑑.
            cs_list = merge_CS(cs_list)

            rs_set_count = list(rs_set.data_points.keys())


            sampled_points_rdd = points_rdd.subtract(sampled_points_rdd)
            # If this is the last run (after the last chunk of data), merge CS clusters with DS clusters that have a
            # Mahalanobis Distance < 2 �
            if iteration == 3:
                cs_to_delete = []
                for c in range(len(cs_list)):
                    cs_obj = cs_list[c]
                    distances = [(ds_obj, get_mahalanobis_distance(cs_obj.get_centroid(),
                                                                   ds_obj)) for ds_obj in ds_list]
                    distances.sort(key=lambda x: x[1])
                    if distances[0][1] < 2 * math.sqrt(10.0):
                        distances[0][0].merge_ds_cs(cs_obj)
                        cs_to_delete.append(cs_obj)
                    else:
                        continue
                for cs in cs_to_delete:
                    if cs in cs_list:
                        cs_list.remove(cs)
                for cs in cs_list:
                    rs_set_count.extend(cs.data_indices)

            intermediate_result = [get_data_sum_DS_CS_list(ds_list), len(cs_list),
                                   get_data_sum_DS_CS_list(cs_list), len(rs_set_count)]
            round_num = str(iteration + 2)
            f.write("Round {}: {}\n".format(round_num, ",".join(map(str, intermediate_result))))
        f.write("\n")
        result = {}
        for i, one_DS in enumerate(ds_list):
            for data_index in one_DS.data_indices:
                result[data_index] = i
        for idx in rs_set_count:
            result[idx] = -1
        f.write("The clustering results:\n")
        for key, value in result.items():
            f.write("{},{}\n".format(key, value))