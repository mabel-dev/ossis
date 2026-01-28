import os
import sys
import time

from ossis.bitarray import BitArray

sys.path.insert(1, os.path.join(sys.path[0], ".."))


# Test function to measure performance
def test_bit_array_performance(size, iterations):
    start_time = time.time()

    # Create a BitArray instance
    bit_array = BitArray(size)

    # Perform operations on the BitArray
    for i in range(iterations):
        bit_array.set(i % size, i % 2 == 0)
        bit_value = bit_array.get(i % size)

    for i in range(1400):
        bit_array.array

    end_time = time.time()
    execution_time = end_time - start_time

    return execution_time

# Parameters for testing
array_size = 10_000
num_iterations = 11_000_000

# Measure performance before optimizations
print("Testing performance...")
before_optimization_time = test_bit_array_performance(array_size, num_iterations)
print(f"Execution time: {before_optimization_time:.6f} seconds")

