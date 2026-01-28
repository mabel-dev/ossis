import collections
import cProfile
import os
import pstats

# Example usage
import random
import sys
import time

import numpy

import ossis

sys.path.insert(1, os.path.join(sys.path[0], ".."))


def group_by(lst, key_func):
    groups = collections.defaultdict(list)
    for item in lst:
        groups[key_func(item)].append(item)
    return dict(groups)


# Generate a large dataset with 5 million items and approximately 5000 groups
data = [(random.randint(0, 4999), random.random()) for _ in range(5000000)]
t = time.monotonic_ns()
# Group the data by the first element of each tuple
groups = group_by(data, lambda x: x[0])

print(len(groups), (time.monotonic_ns() - t) / 1e9)  # Output: 5000

data = ossis.DataFrame([{"value": random.randint(0, 4999), "val": random.random} for _ in range(5000000)])
t = time.monotonic_ns()
groups = list(data.group_by("value").count())
print(len(groups), (time.monotonic_ns() - t) / 1e9)  # Output: 5000



with cProfile.Profile(subcalls=False) as pr:
    list(data.group_by("value").count())
    stats = pstats.Stats(pr).sort_stats("cumtime")
    stats.print_stats(20)
