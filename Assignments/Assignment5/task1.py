from blackbox import BlackBox
from functools import partial
import random
import binascii
import csv
import sys
import time


SIZE_BLOOM_FILTER = 69997
UPPER_BOUND = 69997
NUM_OF_HASH_FUNCS = 10


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



def bloom_filter(stream_users, bit_array, previous_set):
    false_positives = 0
    true_negatives = 0

    for user in stream_users:

        user_hashes = myhashs(user)        

        is_in_filter = all(bit_array[hash_index] == 1 for hash_index in user_hashes)

        if user not in previous_set:
            if is_in_filter:
                false_positives += 1
            else:
                true_negatives += 1
                for hash_index in user_hashes:
                    bit_array[hash_index] = 1

            previous_set.add(user)

    if (false_positives + true_negatives) == 0:
        return 0  
    return false_positives / (false_positives + true_negatives)



def save_csv(rows, output_file_name):
    with open(output_file_name, "w", newline="") as f:
        writer = csv.writer(f)
        # Write the data rows
        for row in rows:
            writer.writerow(row)


if __name__ == '__main__':
    start_time = time.time()

    input_filename = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_filename = sys.argv[4]

    list_results = [['Time', 'FPR']]  # Initialize with header
    bit_array = [0] * SIZE_BLOOM_FILTER
    previous_set = set()

    bx = BlackBox()
    for i in range(num_of_asks):

        stream_users = bx.ask(input_filename, stream_size)
        fpr = bloom_filter(stream_users,bit_array,previous_set)
        list_results.append([i, fpr])  # Append each result as a new row

    save_csv(list_results, output_filename)

    end_time = time.time()
    print('Duration: ', end_time - start_time)