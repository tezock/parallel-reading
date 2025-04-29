import multiprocessing
import os
import time

# reads from size bytes from the start position in a file.
def read_chunk(path, start, size):

    end = start + size

    # open the given file
    with open(path, "rb") as f:

        # seek to the start position in the file
        f.seek(start)

        pos = f.tell()

        # if we're not at the beginning of the file, skip the incomplete line.
        if start != 0:
            f.readline()

        # read lines until we finish our chunk
        while pos < end:

            line = f.readline()

            # end early if we reach EOF
            if not line:
                break

            # data processing step COULD go here in a real use case.

            # update the current position in the file.
            pos = f.tell()
    

# function to execute serial reads on a given file
def read_file(file_path, iterations, num_available_cpus):
    
    times = []

    # read the files a given number of times.
    for _ in range(iterations):

        start_time = time.perf_counter()

        # based on the number of cpus, each thread should reach a specific amount
        # of the file.
        block_size = int(os.stat(file_path).st_size / num_available_cpus)

        offsets = [i * block_size for i in range(num_available_cpus)]

        args = [(file_path, offsets[i], block_size) for i in range(num_available_cpus)]
        with multiprocessing.Pool(num_available_cpus) as p:
            p.starmap(read_chunk, args)

        end_time = time.perf_counter()
        times.append(end_time - start_time)

    average_time = sum(times) / iterations
    
    print("Read from the file", iterations, "time(s) using", num_available_cpus, "core(s) in an average time of", average_time, "seconds.")
    
    for i in range(iterations):
        print("iteration", i, ": ", times[i])
    print()

if __name__ == '__main__':

    multiprocessing.freeze_support() 

    dataset_path = "taxi-data-sorted-small.csv"

    print("INPUT FILE:", dataset_path)
    print()
    for core_count in range(1, os.cpu_count() + 1):
        read_file(dataset_path, 5, core_count)

