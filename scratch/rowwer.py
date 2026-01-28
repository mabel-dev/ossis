import os
import sys

from ossis import Row

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from ossis.tools import monitor  # isort: skip

@monitor()
def test():
    factory = Row.create_class(["a", "b"])
    for i in range(1_000_000):
        r = factory({"b": 1, "a": 2})
        #print(r)
    print(r)
test()
