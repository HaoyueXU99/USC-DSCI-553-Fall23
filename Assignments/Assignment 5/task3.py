from blackbox import BlackBox
import random
import csv
import sys
import time

SIZE = 100
SEQ_NUM = 0  

def update_reservoir(user, reservoir):

    if len(reservoir) < SIZE:
        reservoir.append(user)
    elif random.random() < SIZE / SEQ_NUM:
        reservoir[random.randint(0, SIZE - 1)] = user

def reservoir_sampling_algorithm(stream_users, reservoir):
    
    global SEQ_NUM

    for user in stream_users:
        SEQ_NUM += 1
        update_reservoir(user, reservoir)
    return SEQ_NUM


def save_csv(row, output_file_name):
    with open(output_file_name, "a", newline='') as file:
        writer = csv.writer(file)
        writer.writerow(row)

if __name__ == '__main__':

    start_time = time.time()


    input_filename = sys.argv[1]
    stream_size = int(sys.argv[2])
    num_of_asks = int(sys.argv[3])
    output_filename = sys.argv[4]

    random.seed(553)

    bx = BlackBox()
    reservoir = []

    for i in range(num_of_asks):
        stream_users = bx.ask(input_filename, stream_size)
        reservoir_sampling_algorithm(stream_users, reservoir)
        
        # Prepare row for CSV
        row = [SEQ_NUM]
        for index in [0, 20, 40, 60, 80]:
            row.append(reservoir[index] if index < len(reservoir) else None)
        
        if i == 0:
            with open(output_filename, "w", newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['seqnum', '0_id', '20_id', '40_id', '60_id', '80_id'])
                writer.writerow(row)

        else:
            save_csv(row, output_filename)

    end_time = time.time()
    print('Duration: ', end_time - start_time)
