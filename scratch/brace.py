import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx"))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx/opteryx"))
sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx/opteryx"))

from ossis.tools import monitor  # isort: skip
from ossis.schema import FlatColumn  # isort: skip

@monitor()
def main():
    import cProfile
    import pstats

    import opteryx

#    with cProfile.Profile(subcalls=False) as pr:

    SQL = "SELECT TRY_CAST(planetId AS TIMESTAMP) FROM $satellites"
    SQL = "SELECT * FROM 'scratch/tweets.arrow' AS A"
    SQL = "SELECT blob(name), name, '2020-01-01' - CURRENT_TIME FROM $planets"
    SQL = "SELECT * FROM 'scratch/tweets.arrow' -- $missions"
    SQL = "SELECT * FROM $astronauts"
#    SQL = "SELECT * FROM $missions"
    df = opteryx.query(SQL)
    #df = opteryx.query("SELECT '2023-02-02'")
    # df = opteryx.query("SELECT current_time")
    df = opteryx.query("SELECT now() - current_date, INTERVAL '10' HOUR")
    print(df)

    
#        stats = pstats.Stats(pr).sort_stats("cumtime")
#        stats.print_stats(25)
    #seri = df.schema.columns[0].to_json()
    #print(seri)
    #col = FlatColumn.from_json(seri)
    #print(repr(col))


if __name__ == "__main__":  # pragma: no cover
    main()
