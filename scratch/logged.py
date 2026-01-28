import os
import sys

from ossis.ossis_logging import get_logger

sys.path.insert(1, os.path.join(sys.path[0], ".."))
get_logger().audit("print me\r over the top")
