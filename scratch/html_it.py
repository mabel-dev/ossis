import os
import sys

import opteryx

from ossis.display import html_table

sys.path.insert(1, os.path.join(sys.path[0], ".."))


df = opteryx.query("SELECT * FROM $astronauts")
print(html_table(df))
