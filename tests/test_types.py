"""
Test type functionality.
"""

import datetime
import decimal
from ossis.types import OrsoTypes, find_compatible_type


def test_types_is_numeric():
    """Test is_numeric classification"""
    assert not OrsoTypes.ARRAY.is_numeric()
    assert not OrsoTypes.BLOB.is_numeric()
    assert OrsoTypes.BOOLEAN.is_numeric()
    assert not OrsoTypes.DATE.is_numeric()
    assert OrsoTypes.DECIMAL.is_numeric()
    assert OrsoTypes.DOUBLE.is_numeric()
    assert OrsoTypes.INTEGER.is_numeric()
    assert not OrsoTypes.INTERVAL.is_numeric()
    assert not OrsoTypes.STRUCT.is_numeric()
    assert not OrsoTypes.TIME.is_numeric()
    assert not OrsoTypes.TIMESTAMP.is_numeric()
    assert not OrsoTypes.VARCHAR.is_numeric()


def test_types_is_temporal():
    """Test is_temporal classification"""
    assert not OrsoTypes.ARRAY.is_temporal()
    assert not OrsoTypes.BLOB.is_temporal()
    assert not OrsoTypes.BOOLEAN.is_temporal()
    assert OrsoTypes.DATE.is_temporal()
    assert not OrsoTypes.DECIMAL.is_temporal()
    assert not OrsoTypes.DOUBLE.is_temporal()
    assert not OrsoTypes.INTEGER.is_temporal()
    assert not OrsoTypes.INTERVAL.is_temporal()
    assert not OrsoTypes.STRUCT.is_temporal()
    assert OrsoTypes.TIME.is_temporal()
    assert OrsoTypes.TIMESTAMP.is_temporal()
    assert not OrsoTypes.VARCHAR.is_temporal()


def test_types_is_large_object():
    """Test is_large_object classification"""
    assert not OrsoTypes.ARRAY.is_large_object()
    assert OrsoTypes.BLOB.is_large_object()
    assert not OrsoTypes.BOOLEAN.is_large_object()
    assert not OrsoTypes.DATE.is_large_object()
    assert not OrsoTypes.DECIMAL.is_large_object()
    assert not OrsoTypes.DOUBLE.is_large_object()
    assert not OrsoTypes.INTEGER.is_large_object()
    assert not OrsoTypes.INTERVAL.is_large_object()
    assert not OrsoTypes.STRUCT.is_large_object()
    assert not OrsoTypes.TIME.is_large_object()
    assert not OrsoTypes.TIMESTAMP.is_large_object()
    assert OrsoTypes.VARCHAR.is_large_object()


def test_types_python_type():
    """Test python_type mapping"""
    assert OrsoTypes.ARRAY.python_type == list
    assert OrsoTypes.BLOB.python_type == bytes
    assert OrsoTypes.BOOLEAN.python_type == bool
    assert OrsoTypes.DATE.python_type == datetime.date
    assert OrsoTypes.DECIMAL.python_type == decimal.Decimal
    assert OrsoTypes.DOUBLE.python_type == float
    assert OrsoTypes.INTEGER.python_type == int
    assert OrsoTypes.VARCHAR.python_type == str


def test_type_to_numpy_dtype():
    """Test numpy dtype conversion"""
    import numpy
    assert OrsoTypes.INTEGER.numpy_dtype == numpy.dtype("int64")
    assert OrsoTypes.DOUBLE.numpy_dtype == numpy.dtype("float64")
    assert OrsoTypes.BOOLEAN.numpy_dtype == numpy.dtype("?")


def test_parsers():
    """Test type parsers"""
    from ossis.tools import parse_iso
    
    # Date parsing
    field = OrsoTypes.DATE
    parsed = field.parse("2023-01-01")
    assert isinstance(parsed, datetime.date), type(parsed)
    assert parsed == parse_iso("2023-01-01").date(), parsed

    # Decimal parsing
    field = OrsoTypes.DECIMAL
    parsed = field.parse("8.7")
    assert isinstance(parsed, decimal.Decimal), type(parsed)
    assert parsed == decimal.Decimal("8.7"), parsed

    # Double parsing
    field = OrsoTypes.DOUBLE
    parsed = field.parse("8.7")
    assert isinstance(parsed, float), type(parsed)
    assert parsed == 8.7, parsed

    # Integer parsing
    field = OrsoTypes.INTEGER
    parsed = field.parse("8")
    assert isinstance(parsed, int), type(parsed)
    assert parsed == 8, parsed

    # Timestamp parsing
    field = OrsoTypes.TIMESTAMP
    parsed = field.parse("2023-01-01T00:00:01")
    assert isinstance(parsed, datetime.datetime), type(parsed)
    assert parsed == parse_iso("2023-01-01 00:00:01"), parsed

    # VARCHAR parsing
    field = OrsoTypes.VARCHAR
    parsed = field.parse("1718530754")
    assert isinstance(parsed, str), type(parsed)
    assert parsed == "1718530754", parsed


def test_type_combinations():
    """Test compatible type finding"""
    assert find_compatible_type([OrsoTypes.INTEGER, OrsoTypes.INTEGER]) == OrsoTypes.INTEGER
    assert find_compatible_type([OrsoTypes.INTEGER, OrsoTypes.DOUBLE]) == OrsoTypes.DOUBLE
    assert find_compatible_type([OrsoTypes.DOUBLE, OrsoTypes.INTEGER]) == OrsoTypes.DOUBLE
    assert find_compatible_type([OrsoTypes.DOUBLE, OrsoTypes.DOUBLE]) == OrsoTypes.DOUBLE
    assert find_compatible_type([OrsoTypes.INTEGER, OrsoTypes.DECIMAL]) == OrsoTypes.DECIMAL
    assert find_compatible_type([OrsoTypes.DECIMAL, OrsoTypes.INTEGER]) == OrsoTypes.DECIMAL
    assert find_compatible_type([OrsoTypes.DECIMAL, OrsoTypes.DECIMAL]) == OrsoTypes.DECIMAL
    assert find_compatible_type([OrsoTypes.DECIMAL, OrsoTypes.DOUBLE]) == OrsoTypes.DECIMAL
    assert find_compatible_type([OrsoTypes.DOUBLE, OrsoTypes.DECIMAL]) == OrsoTypes.DECIMAL
    assert find_compatible_type([OrsoTypes.VARCHAR, OrsoTypes.VARCHAR]) == OrsoTypes.VARCHAR
    assert find_compatible_type([OrsoTypes.INTEGER, OrsoTypes.VARCHAR]) == OrsoTypes.VARCHAR
    assert find_compatible_type([OrsoTypes.VARCHAR, OrsoTypes.INTEGER]) == OrsoTypes.VARCHAR
    assert find_compatible_type([OrsoTypes.BLOB, OrsoTypes.BLOB]) == OrsoTypes.BLOB
    assert find_compatible_type([OrsoTypes.TIMESTAMP, OrsoTypes.TIMESTAMP]) == OrsoTypes.TIMESTAMP


def test_type_name_parser():
    """Test type name parsing"""
    (_type, _length, _precision, _scale, _element_type) = OrsoTypes.from_name("INTEGER")
    assert (_type, _length, _precision, _scale, _element_type) == (OrsoTypes.INTEGER, None, None, None, None)
    
    (_type, _length, _precision, _scale, _element_type) = OrsoTypes.from_name("VARCHAR")
    assert (_type, _length, _precision, _scale, _element_type) == (OrsoTypes.VARCHAR, None, None, None, None)
    
    (_type, _length, _precision, _scale, _element_type) = OrsoTypes.from_name("DECIMAL(10,2)")
    assert (_type, _length, _precision, _scale, _element_type) == (OrsoTypes.DECIMAL, None, 10, 2, None)
    
    (_type, _length, _precision, _scale, _element_type) = OrsoTypes.from_name("VARCHAR[12]")
    assert (_type, _length, _precision, _scale, _element_type) == (OrsoTypes.VARCHAR, 12, None, None, None)
    
    (_type, _length, _precision, _scale, _element_type) = OrsoTypes.from_name("ARRAY<INTEGER>")
    assert (_type, _length, _precision, _scale, _element_type) == (OrsoTypes.ARRAY, None, None, None, OrsoTypes.INTEGER)


if __name__ == "__main__":
    import sys
    import traceback
    
    # Simple test runner
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