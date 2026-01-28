import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from ossis.types import ossisTypes


def test_types_is_numeric():
    # is numeric
    assert not ossisTypes.ARRAY.is_numeric()
    assert not ossisTypes.BLOB.is_numeric()
    assert ossisTypes.BOOLEAN.is_numeric()
    assert not ossisTypes.DATE.is_numeric()
    assert ossisTypes.DECIMAL.is_numeric()
    assert ossisTypes.DOUBLE.is_numeric()
    assert ossisTypes.INTEGER.is_numeric()
    assert not ossisTypes.INTERVAL.is_numeric()
    assert not ossisTypes.STRUCT.is_numeric()
    assert not ossisTypes.TIME.is_numeric()
    assert not ossisTypes.TIMESTAMP.is_numeric()
    assert not ossisTypes.VARCHAR.is_numeric()


def test_types_is_temporal():
    # is temporal
    assert not ossisTypes.ARRAY.is_temporal()
    assert not ossisTypes.BLOB.is_temporal()
    assert not ossisTypes.BOOLEAN.is_temporal()
    assert ossisTypes.DATE.is_temporal()
    assert not ossisTypes.DECIMAL.is_temporal()
    assert not ossisTypes.DOUBLE.is_temporal()
    assert not ossisTypes.INTEGER.is_temporal()
    assert not ossisTypes.INTERVAL.is_temporal()
    assert not ossisTypes.STRUCT.is_temporal()
    assert ossisTypes.TIME.is_temporal()
    assert ossisTypes.TIMESTAMP.is_temporal()
    assert not ossisTypes.VARCHAR.is_temporal()


def test_types_is_large_object():
    # is temporal
    assert not ossisTypes.ARRAY.is_large_object()
    assert ossisTypes.BLOB.is_large_object()
    assert not ossisTypes.BOOLEAN.is_large_object()
    assert not ossisTypes.DATE.is_large_object()
    assert not ossisTypes.DECIMAL.is_large_object()
    assert not ossisTypes.DOUBLE.is_large_object()
    assert not ossisTypes.INTEGER.is_large_object()
    assert not ossisTypes.INTERVAL.is_large_object()
    assert not ossisTypes.STRUCT.is_large_object()
    assert not ossisTypes.TIME.is_large_object()
    assert not ossisTypes.TIMESTAMP.is_large_object()
    assert ossisTypes.VARCHAR.is_large_object()

def test_types_python_type():
    # don't need to test them all to provide the code
    assert ossisTypes.ARRAY.python_type == list
    assert ossisTypes.BLOB.python_type == bytes


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
