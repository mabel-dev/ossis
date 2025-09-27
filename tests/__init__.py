"""
Test configuration and setup for ossis tests.
"""

import os
import sys
import traceback
from pathlib import Path

# Add ossis package to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ossis import FlatColumn, RelationSchema, OrsoTypes

# Simple test schema similar to what orso used
cities_schema = RelationSchema(
    name="cities",
    columns=[
        FlatColumn(name="name", type=OrsoTypes.VARCHAR, nullable=False),
        FlatColumn(name="population", type=OrsoTypes.INTEGER),
        FlatColumn(name="country", type=OrsoTypes.VARCHAR),
        FlatColumn(name="founded", type=OrsoTypes.VARCHAR, nullable=True),
        FlatColumn(name="area", type=OrsoTypes.DOUBLE),
        FlatColumn(name="language", type=OrsoTypes.VARCHAR),
    ],
)


def run_tests():
    """Run all test functions in the module."""
    module = sys.modules[__name__]
    
    test_count = 0
    passed_count = 0
    failed_tests = []
    
    for name in dir(module):
        if name.startswith('test_'):
            test_func = getattr(module, name)
            if callable(test_func):
                test_count += 1
                try:
                    print(f"Running {name}...")
                    test_func()
                    print(f"✓ {name} passed")
                    passed_count += 1
                except Exception as e:
                    print(f"✗ {name} failed: {e}")
                    traceback.print_exc()
                    failed_tests.append(name)
    
    print(f"\nTest Results: {passed_count}/{test_count} passed")
    if failed_tests:
        print(f"Failed tests: {', '.join(failed_tests)}")
        sys.exit(1)
    else:
        print("All tests passed!")