import os
import sys

from ossis.tools import *

sys.path.insert(1, os.path.join(sys.path[0], ".."))



@retry(exponential_backoff=True, max_backoff=3)
def doomed():
    raise NotImplementedError()


@monitor
def slow():
    import time
    time.sleep(0.05)

for i in range(10):
    slow()

print(slow.count, slow.stats())

@repeat(10)
def do_nothing():
    print("okay")

do_nothing()
