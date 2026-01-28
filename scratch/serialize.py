import os
import sys
import time

import opteryx

from ossis import Row
from ossis.row import extract_columns

sys.path.insert(1, os.path.join(sys.path[0], ".."))






schema = {'id': {'type': 'SMALLINT', 'nullable': False},
          'planetId': {'type': 'SMALLINT', 'nullable': False},
          'name': {'type': 'VARCHAR', 'nullable': False},
          'gm': { 'type': 'FLOAT', 'nullable': False},
          'radius': { 'type': 'FLOAT', 'nullable': False},
          'density': {'type': 'FLOAT', 'nullable': True},
          'magnitude': { 'type': 'FLOAT', 'nullable': True},
          'albedo': { 'type': 'FLOAT', 'nullable': True},
          }

data = opteryx.query("SELECT * FROM $astronauts")

MyRow = Row.create_class(tuple(schema.keys()))
#data = [MyRow(d) for d in table_to_tuples(data)]

t = time.monotonic_ns()
for record in data:
    for i in range(1000):
        bytestring = record.as_bytes
        bytestring = MyRow.from_bytes(record.as_bytes)

print((time.monotonic_ns() - t) / 1e9, bytestring)

