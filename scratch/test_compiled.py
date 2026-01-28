import os
import sys
import time

import numpy
import opteryx

from ossis.compute.compiled import extract_dict_columns

sys.path.insert(1, os.path.join(sys.path[0], ".."))


def test_collector():

    df = opteryx.query("SELECT * FROM FAKE(1000000, 1) AS f")
    start = time.monotonic_ns()
    c = df.collect(0)
    print((time.monotonic_ns() - start) / 1e9)

    df = opteryx.query("SELECT * FROM $missions").arrow().to_pylist()
    start = time.monotonic_ns()
    for i in range(1000):
        for r in df:
            d = extract_dict_columns(r, ("Company", "Location", "Price", "Rocket", "Rocket_Status", "Mission"))
    print((time.monotonic_ns() - start) / 1e9)
    print(d)


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    test_collector()

    run_tests()
