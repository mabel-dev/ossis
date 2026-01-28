import cProfile
import datetime
import itertools
import os
import pstats
import sys
import time
import typing
from collections import deque

import numpy
import opteryx

from ossis import DataFrame
from ossis import Row
from ossis.compute.compiled import process_table
from ossis.converters import from_arrow
from ossis.exceptions import MissingDependencyError
from ossis.row import Row
from ossis.row import extract_columns
from ossis.schema import FlatColumn
from ossis.schema import RelationSchema
from ossis.tools import arrow_type_map

sys.path.insert(1, os.path.join(sys.path[0], ".."))








BATCH_SIZE = 10000

def _from_arrow(tables, size=None):
    """
    Convert an Arrow table or an iterable of Arrow tables to a generator of
    Python objects, handling each block one at a time.
    """
    if not isinstance(tables, (typing.Generator, list, tuple)):
        tables = [tables]

    if isinstance(tables, (list, tuple)):
        tables = iter(tables)

    BATCH_SIZE: int = 10_000
    if size:
        BATCH_SIZE = min(size, BATCH_SIZE)
    else:
        size = float("inf")

    # Extract schema information from the first table
    first_table = next(tables, None)
    if first_table is None:
        return iter([]), {}

    arrow_schema = first_table.schema
    ossis_schema = RelationSchema(
        name="arrow",
        columns=[FlatColumn.from_arrow(field) for field in arrow_schema],
    )
    row_factory = Row.create_class(ossis_schema, tuples_only=True)
    rows: typing.List[Row] = []

    for table in itertools.chain([first_table], tables):
        rows.extend(process_table(table, row_factory, 10_000))
        if len(rows) > size:
            break

    # Limit the number of rows to 'size'
    if isinstance(size, int):
        rows = itertools.islice(rows, size)
    return rows, ossis_schema



SQL = "SELECT * FROM (VALUES (1), (-1), (NULL)) AS tristatebooleans(bool) WHERE bool IS NULL;"
adf = opteryx.query("SELECT * FROM $astronauts CROSS JOIN $satellites CROSS JOIN $planets").arrow()
#adf = opteryx.query("SELECT * FROM (VALUES (1), (-1), (NULL)) AS tristatebooleans(bool) WHERE bool IS NULL;").arrow()


t = time.monotonic_ns()
for i in range(1):
    pass
    df = from_arrow(adf)

print("numpy time:", (time.monotonic_ns() - t) / 1e9, "records:", 0)
#d = DataFrame(rows=df[0], schema=df[1])

#print(d.head())




def process_set(set_with_nan):
    has_nan = any(item != item for item in set_with_nan)  # Check for NaN using NaN's property
    set_without_nan = {
        item for item in set_with_nan if item == item
    }  # Create a new set without NaNs
    return has_nan, set_without_nan


def compare_sets(set1, set2):
    if not set1 and not set2:
        return True

    s1_nan, s1_no_nan = process_set(set1)
    s2_nan, s2_no_nan = process_set(set2)

    return s1_nan == s2_nan and s1_no_nan == s2_no_nan

def test_null_semantics(statement, expected_result):
    """
    Test an battery of statements
    """

    cursor = opteryx.query(statement).arrow()
    rows, schema = from_arrow(cursor)
    cursor = DataFrame(rows = rows, schema=schema)
    result = {v[0] for v in cursor.fetchall()}
    assert compare_sets(
        result, expected_result
    ), f"Query returned {result} but {expected_result} was expected.\n{statement}"


test_null_semantics(
"""
-- Query 21: Expected rows: 1 (NULL)
SELECT * FROM (VALUES (1), (-1), (NULL)) AS tristatebooleans(bool) WHERE bool IS NULL;
""", {None})

quit()
with cProfile.Profile(subcalls=False) as pr:
    df = _from_arrow(adf)
    stats = pstats.Stats(pr).sort_stats("cumtime")
    stats.print_stats(100)
