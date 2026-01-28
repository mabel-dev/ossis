import time

import opteryx

df = opteryx.query("SELECT * FROM 'scratch/tweets.arrow'")

t = time.monotonic_ns()
pr = df.arrow()
print((time.monotonic_ns() - t) / 1e9)

print(pr.shape)
