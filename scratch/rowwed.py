import os
import sys

from ossis import DataFrame

#import ossis

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from ossis.tools import monitor  # isort: skip

@monitor()
def approach_one():
    import opteryx

    df = opteryx.query("SELECT * FROM $satellites")
    for i in range(100000):
        o = DataFrame(schema=df.column_names)
        for row in df:
            o.append(row)
    print(o.shape)

@monitor()
def approach_two():
    import opteryx

    df = opteryx.query("SELECT * FROM $satellites")
    for i in range(100000):
        rows = (row for row in df)
        o = DataFrame(schema=df.column_names, rows=rows)
    print(o.shape)

@monitor()
def approach_three():
    import opteryx

    df = opteryx.query("SELECT * FROM $satellites").arrow()
    for i in range(100000):
        rows = (row for row in df)
        o = DataFrame.from_arrow(df)
    print(o.shape)

if __name__ == "__main__":  # pragma: no cover
    approach_one()
    approach_two()
    approach_three()


# pre
# 21.2 / 110 / 172
# 19.2 / 118 / 167
# 17.5 / 111 / 172
# 18.6 / 109 / 167

# post
# 17.5 / 112 / 172
# 17.8 / 109 / 163
# 18.1 / 108 / 167
# 18.7 / 109 / 159

# post 2
# 15.6 / 109 / 163
# 19.7 / 109 / 161
# 17.9 / 110 / 169 
# 17.9 / 108 / 162
