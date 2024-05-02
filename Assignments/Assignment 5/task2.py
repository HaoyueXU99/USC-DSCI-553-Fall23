from blackbox import BlackBox
from functools import partial
import random
import binascii
import csv
import statistics
import sys
import time

 
UPPER_BOUND = 3007
NUM_OF_HASH_FUNCS = 200
NUM_OF_PARTITIONS = 4

def myhashs(stream_user):

    def generate_prime_list(size):
        primes = []
        candidate = UPPER_BOUND
        while len(primes) < size:
            if is_prime(candidate):
                primes.append(candidate)
            candidate += 1
        return primes

    def is_prime(n):
        if n <= 1:
            return False
        if n <= 3:
            return True
        if n % 2 == 0 or n % 3 == 0:
            return False
        i = 5
        while i * i <= n:
            if n % i == 0 or n % (i + 2) == 0:
                return False
            i += 6
        return True


    def hash_function(key, a, b, p, m):
        return ((a * key + b) % p) % m

    list_of_hash_functions = [
        partial(hash_function, 
                a=random.randint(1, UPPER_BOUND), 
                b=random.randint(1, UPPER_BOUND), 
                p=p, 
                m=UPPER_BOUND) 
        for p in generate_prime_list(NUM_OF_HASH_FUNCS)
    ]

    stream_user_int = int(binascii.hexlify(stream_user.encode('utf8')), 16)
    return [function(stream_user_int) for function in list_of_hash_functions]


def flajolet_martin_algorithm(stream_users, NUM_OF_PARTITIONS):
    
    hash_values = [myhashs(user) for user in stream_users]

    # Initialize max positions for each hash function
    max_positions = [0] * NUM_OF_HASH_FUNCS

    # Find the maximum position of the rightmost 1-bit for each hash function
    for values in hash_values:
        for i, value in enumerate(values):
            # Convert to binary and find the position of the rightmost 1-bit
            binary_value = bin(value)[2:]  # Convert to binary and remove the '0b' prefix
            rightmost_one_position = len(binary_value) - binary_value.rfind('1') - 1
            max_positions[i] = max(max_positions[i], rightmost_one_position)

    # Partition hash functions and average within each partition
    partition_size = NUM_OF_HASH_FUNCS // NUM_OF_PARTITIONS
    partition_avgs = []
    for i in range(0, NUM_OF_HASH_FUNCS, partition_size):
        partition_avg = statistics.mean(max_positions[i:i + partition_size])
        partition_avgs.append(2 ** partition_avg)

    # Calculate the average across partitions
    estimate_2R = statistics.mean(partition_avgs)

    return int(estimate_2R)


def save_csv(rows, output_file_name):
    with open(output_file_name, "w", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)

if __name__ == '__main__':
    start_time = time.time()

    input_filename = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_filename = sys.argv[4]

    list_results = [['Time', 'Ground Truth', 'Estimation']]  

    bx = BlackBox()
    for i in range(num_of_asks):
        stream_users = bx.ask(input_filename, stream_size)
        estimate = flajolet_martin_algorithm(stream_users, NUM_OF_PARTITIONS)
        distinct_user_count = len({user for user in stream_users})
        list_results.append([i, distinct_user_count, estimate]) 

    save_csv(list_results, output_filename)
    end_time = time.time()
    print('Duration: ', end_time - start_time)