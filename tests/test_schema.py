import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from ossis.exceptions import DataValidationError
from ossis.exceptions import ExcessColumnsInDataError
from ossis.schema import RelationSchema
from ossis.schema import FlatColumn
from ossis.types import ossisTypes
from tests import cities


def test_find_column():
    column = cities.schema.find_column("language")
    assert column.name == "language", column.name

    column = cities.schema.find_column("geolocation")
    assert column is None, column


def test_all_column_names():
    column_names = cities.schema.all_column_names()
    assert "name" in column_names
    assert "language" in column_names
    assert "geolocation" not in column_names


def test_schema_persistance():
    def strip_expectations(rel):
        columns = []
        for column in rel.columns:
            column.expectations = []
            columns.append(column)
        rel.columns = columns
        return rel

    as_dict = strip_expectations(cities.schema).to_dict()
    from_dict = strip_expectations(RelationSchema.from_dict(as_dict))

    assert strip_expectations(from_dict) == strip_expectations(cities.schema), strip_expectations(
        from_dict
    )
    assert as_dict == from_dict.to_dict()


def test_validate_with_valid_data():
    # Test with valid data
    data = {
        "name": "New York",
        "population": 8623000,
        "country": "United States",
        "founded": "1624",
        "area": 783.8,
        "language": "English",
    }
    assert cities.schema.validate(data) == True


def test_struct_to_blob():
    import pyarrow 

    struct_type = pyarrow.struct([pyarrow.field('subfield', pyarrow.int32())])
    arrow_col = pyarrow.field(name="column", type=struct_type)

    assert arrow_col.type == struct_type, arrow_col.type

    ossis_reformed = FlatColumn.from_arrow(arrow_col)
    assert ossis_reformed.type == ossisTypes.STRUCT
    assert ossis_reformed.fields is not None
    assert [field.name for field in ossis_reformed.fields] == ["subfield"]
    assert ossis_reformed.fields[0].type == ossisTypes.INTEGER

    ossis_struct_as_blob_col = FlatColumn.from_arrow(arrow_col, mappable_as_binary=True)
    assert ossis_struct_as_blob_col.type == ossisTypes.BLOB, ossis_struct_as_blob_col.type

def test_validate_with_missing_column():
    # Test with missing column
    data = {
        "name": "London",
        "population": 8908081,
        "country": "United Kingdom",
        "founded": "43 AD",
        "language": "English",
    }
    with pytest.raises(DataValidationError) as err:
        cities.schema.validate(data)
    assert "area" in str(err)


def test_validate_with_nullable_column():
    # Test with nullable column and value is None
    data = {
        "name": "Paris",
        "population": 2187526,
        "country": "France",
        "founded": None,
        "area": 105.4,
        "language": "French",
    }
    assert cities.schema.validate(data)


def test_validate_with_non_nullable_column():
    # Test with non-nullable column and value is None
    data = {
        "name": None,
        "population": 13929286,
        "country": "Japan",
        "founded": "1457",
        "area": 2187.66,
        "language": "Japanese",
    }
    with pytest.raises(DataValidationError) as err:
        cities.schema.validate(data)
    assert "name" in str(err)


def test_validate_with_wrong_type():
    # Test with column value of wrong type
    data = {
        "name": "Berlin",
        "population": 3769495,
        "country": "Germany",
        "founded": "1237",
        "area": "891.8",  # Expected type is double
        "language": "German",
    }
    with pytest.raises(DataValidationError) as err:
        cities.schema.validate(data)
    assert "area" in str(err)


def test_validate_with_multiple_errors():
    data = {
        "name": None,  # not nullable
        "population": 3769495,
        # country is missing
        "founded": "1237",
        "area": "891.8",  # Expected type is double
        "language": "German",
    }
    with pytest.raises(DataValidationError) as err:
        cities.schema.validate(data)
    assert "name" in str(err)
    assert "country" in str(err)
    assert "area" in str(err)


def test_validate_with_invalid_data_type():
    # Test with invalid data type (not a MutableMapping)
    data = [1, 2, 3]
    with pytest.raises(TypeError):
        cities.schema.validate(data)


def test_validate_with_additional_columns():
    # Test with column value of wrong type
    data = {
        "name": "Berlin",
        "population": 3769495,
        "country": "Germany",
        "founded": "1237",
        "area": "891.8",  # Expected type is double
        "language": "German",
        "continent": "Europe",
        "religion": "Christianity",
    }
    with pytest.raises(ExcessColumnsInDataError) as err:
        cities.schema.validate(data)

    # check both excess columns are in the error message
    assert "continent" in str(err)
    assert "religion" in str(err)


def test_schema_iterations():
    schema = cities.schema

    assert schema.num_columns == 6

    for i, column in enumerate(schema.columns):
        assert column == schema.column(i)
        assert column == schema.column(column.name)


def test_pop_column():
    # Clone the initial schema for the test to not alter the original
    schema = RelationSchema.from_dict(cities.schema.to_dict())

    # Check the initial state
    initial_column_count = len(schema.columns)

    # Test popping an existing column (e.g., "population")
    popped_column = schema.pop_column("population")
    assert popped_column is not None
    assert popped_column.name == "population"
    assert len(schema.columns) == initial_column_count - 1

    # Validate that the "population" column is no longer in the schema
    assert "population" not in [col.name for col in schema.columns]

    # Test popping a non-existent column
    popped_column = schema.pop_column("nonexistent")
    assert popped_column is None
    assert len(schema.columns) == initial_column_count - 1  # No change in length

    # Assert that the original columns, minus "population", are still there
    remaining_column_names = [col.name for col in schema.columns]
    assert "name" in remaining_column_names
    assert "country" in remaining_column_names
    assert "founded" in remaining_column_names
    assert "area" in remaining_column_names
    assert "language" in remaining_column_names


import pytest


def test_add_method_combines_columns():
    # Arrange
    col1 = FlatColumn(name="col1")
    col2 = FlatColumn(name="col2")
    col3 = FlatColumn(name="col3")

    schema1 = RelationSchema(name="Schema1", columns=[col1, col2])
    schema2 = RelationSchema(name="Schema2", columns=[col2, col3])

    # Act
    combined_schema = schema1 + schema2

    # Assert
    expected_columns = [col1, col2, col3]
    assert combined_schema.columns == expected_columns


def test_add_method_preserves_original_schemas():
    # Arrange
    col1 = FlatColumn(name="col1")
    col2 = FlatColumn(name="col2")
    col3 = FlatColumn(name="col3")
    schema1 = RelationSchema(name="Schema1", columns=[col1, col2])
    schema2 = RelationSchema(name="Schema2", columns=[col2, col3])

    # Act
    combined_schema = schema1 + schema2

    # Assert
    assert schema1.columns == [col1, col2]
    assert schema2.columns == [col2, col3]


def test_add_method_with_duplicate_columns():
    # Arrange
    col1 = FlatColumn(name="col1", type=0)
    col2 = FlatColumn(name="col2", type=0)
    col3 = FlatColumn(name="col3", type=0)
    schema1 = RelationSchema(name="Schema1", columns=[col1, col2])
    schema2 = RelationSchema(name="Schema2", columns=[col1, col2, col3])

    # Act
    combined_schema = schema1 + schema2

    # Assert
    expected_columns = [col1, col2, col3]
    assert combined_schema.columns == expected_columns


def test_minimum_definition():
    rs = RelationSchema.from_dict({"name": "relation", "columns": ["apples"]})
    assert len(rs.column_names) == 1
    assert rs.column("apples").type == ossisTypes._MISSING_TYPE, rs.column("apples").type

    rs.validate({"apples": "none"})

    with pytest.raises(ExcessColumnsInDataError):
        rs.validate({"apples": "green", "oranges": "orange"})


def test_parsers():
    import datetime
    import decimal
    from ossis.tools import parse_iso

    field = ossisTypes.DATE
    parsed = field.parse("2023-01-01")
    assert isinstance(parsed, datetime.date), type(parsed)
    assert parsed == parse_iso("2023-01-01").date(), parsed

    field = ossisTypes.DATE
    parsed = field.parse("2023-01-01T00:00:01")
    assert isinstance(parsed, datetime.date), type(parsed)
    assert parsed == parse_iso("2023-01-01").date(), parsed

    field = ossisTypes.DATE
    with pytest.raises(Exception):
        parsed = field.parse("apples")

    field = ossisTypes.DECIMAL
    parsed = field.parse("8.7")
    assert isinstance(parsed, decimal.Decimal), type(parsed)
    assert parsed == decimal.Decimal("8.7"), parsed

    field = ossisTypes.DOUBLE
    parsed = field.parse("8.7")
    assert isinstance(parsed, float), type(parsed)
    assert parsed == 8.7, parsed

    field = ossisTypes.DOUBLE
    parsed = field.parse("8")
    assert isinstance(parsed, float), type(parsed)
    assert parsed == 8.0, parsed

    field = ossisTypes.INTEGER
    parsed = field.parse("8")
    assert isinstance(parsed, int), type(parsed)
    assert parsed == 8, parsed

    field = ossisTypes.INTEGER
    parsed = field.parse("-8")
    assert isinstance(parsed, int), type(parsed)
    assert parsed == -8, parsed

    field = ossisTypes.TIMESTAMP
    parsed = field.parse("2023-01-01T00:00:01")
    assert isinstance(parsed, datetime.datetime), type(parsed)
    assert parsed == parse_iso("2023-01-01 00:00:01"), parsed

    field = ossisTypes.TIMESTAMP
    parsed = field.parse("1718530754")
    assert isinstance(parsed, datetime.datetime), type(parsed)
    assert parsed == parse_iso("2024-06-16 09:39:14"), parsed

    field = ossisTypes.VARCHAR
    parsed = field.parse("1718530754")
    assert isinstance(parsed, str), type(parsed)
    assert parsed == "1718530754", parsed

def test_type_name_parsing():
    _type = FlatColumn(name="col", type="INTEGER")
    assert _type.type == ossisTypes.INTEGER, _type.type
    _type = FlatColumn(name="col", type="VARCHAR")
    assert _type.type == ossisTypes.VARCHAR, _type.type
    assert _type.length is None, _type.length
    _type = FlatColumn(name="col", type="BLOB")
    assert _type.type == ossisTypes.BLOB, _type.type
    assert _type.length is None, _type.length
    _type = FlatColumn(name="col", type="DOUBLE")
    assert _type.type == ossisTypes.DOUBLE, _type.type
    _type = FlatColumn(name="col", type="DECIMAL")
    assert _type.type == ossisTypes.DECIMAL, _type.type
    _type = FlatColumn(name="col", type="BOOLEAN")
    assert _type.type == ossisTypes.BOOLEAN, _type.type
    _type = FlatColumn(name="col", type="TIMESTAMP")
    assert _type.type == ossisTypes.TIMESTAMP, _type.type
    _type = FlatColumn(name="col", type="ARRAY")
    assert _type.type == ossisTypes.ARRAY, _type.type
    assert _type.element_type == ossisTypes.VARCHAR, _type.element_type
    _type = FlatColumn(name="col", type="ARRAY<INTEGER>")
    assert _type.type == ossisTypes.ARRAY, _type.type
    assert _type.element_type == ossisTypes.INTEGER, _type.element_type
    _type = FlatColumn(name="col", type="ARRAY<VARCHAR>")
    assert _type.type == ossisTypes.ARRAY, _type.type
    assert _type.element_type == ossisTypes.VARCHAR, _type.element_type
    with pytest.raises(ValueError):
        _type = FlatColumn(name="col", type="ARRAY<A")
    with pytest.raises(ValueError):
        _type = FlatColumn(name="col", type="ARRAY<BIT>")
    with pytest.raises(ValueError):
        _type = FlatColumn(name="col", type="ARRAY<ARRAY>")
    _type = FlatColumn(name="col", type="DECIMAL(10,2)")
    assert _type.type == ossisTypes.DECIMAL, _type.type
    assert _type.precision == 10, _type.precision
    assert _type.scale == 2, _type.scale
    _type = FlatColumn(name="col", type="VARCHAR[12]")
    assert _type.type == ossisTypes.VARCHAR, _type.type
    assert _type.length == 12, _type.length
    _type = FlatColumn(name="col", type="BLOB[12]")
    assert _type.type == ossisTypes.BLOB, _type.type
    assert _type.length == 12, _type.length
 

def test_type_name_parser():
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("INTEGER")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.INTEGER, None, None, None, None)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("VARCHAR")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.VARCHAR, None, None, None, None)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("BLOB")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.BLOB, None, None, None, None)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("DOUBLE")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.DOUBLE, None, None, None, None)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("DECIMAL")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.DECIMAL, None, None, None, None)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("BOOLEAN")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.BOOLEAN, None, None, None, None)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("TIMESTAMP")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.TIMESTAMP, None, None, None, None)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("ARRAY")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.ARRAY, None, None, None, ossisTypes.VARCHAR)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("ARRAY<INTEGER>")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.ARRAY, None, None, None, ossisTypes.INTEGER)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("ARRAY<VARCHAR>")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.ARRAY, None, None, None, ossisTypes.VARCHAR)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("DECIMAL(10,2)")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.DECIMAL, None, 10, 2, None), (_type, _length, _scale, _precision, _element_type)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("VARCHAR[12]")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.VARCHAR, 12, None, None, None)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("BLOB[12]")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.BLOB, 12, None, None, None)
    (_type, _length, _scale, _precision, _element_type) = ossisTypes.from_name("ARRAY<TIMESTAMP>")
    assert (_type, _length, _scale, _precision, _element_type) == (ossisTypes.ARRAY, None, None, None, ossisTypes.TIMESTAMP)

def test_type_combinations():

    from ossis.types import find_compatible_type

    assert find_compatible_type([ossisTypes.INTEGER, ossisTypes.INTEGER]) == ossisTypes.INTEGER
    assert find_compatible_type([ossisTypes.INTEGER, ossisTypes.DOUBLE]) == ossisTypes.DOUBLE
    assert find_compatible_type([ossisTypes.DOUBLE, ossisTypes.INTEGER]) == ossisTypes.DOUBLE
    assert find_compatible_type([ossisTypes.DOUBLE, ossisTypes.DOUBLE]) == ossisTypes.DOUBLE
    assert find_compatible_type([ossisTypes.INTEGER, ossisTypes.DECIMAL]) == ossisTypes.DECIMAL
    assert find_compatible_type([ossisTypes.DECIMAL, ossisTypes.INTEGER]) == ossisTypes.DECIMAL
    assert find_compatible_type([ossisTypes.DECIMAL, ossisTypes.DECIMAL]) == ossisTypes.DECIMAL
    assert find_compatible_type([ossisTypes.DECIMAL, ossisTypes.DOUBLE]) == ossisTypes.DECIMAL
    assert find_compatible_type([ossisTypes.DOUBLE, ossisTypes.DECIMAL]) == ossisTypes.DECIMAL
    assert find_compatible_type([ossisTypes.DOUBLE, ossisTypes.DOUBLE]) == ossisTypes.DOUBLE
    assert find_compatible_type([ossisTypes.INTEGER, ossisTypes.TIMESTAMP]) == ossisTypes.VARCHAR
    assert find_compatible_type([ossisTypes.TIMESTAMP, ossisTypes.INTEGER]) == ossisTypes.VARCHAR
    assert find_compatible_type([ossisTypes.TIMESTAMP, ossisTypes.TIMESTAMP]) == ossisTypes.TIMESTAMP
    assert find_compatible_type([ossisTypes.INTEGER, ossisTypes.VARCHAR]) == ossisTypes.VARCHAR
    assert find_compatible_type([ossisTypes.VARCHAR, ossisTypes.INTEGER]) == ossisTypes.VARCHAR
    assert find_compatible_type([ossisTypes.VARCHAR, ossisTypes.VARCHAR]) == ossisTypes.VARCHAR
    assert find_compatible_type([ossisTypes.INTEGER, ossisTypes.BLOB]) == ossisTypes.BLOB
    assert find_compatible_type([ossisTypes.BLOB, ossisTypes.INTEGER]) == ossisTypes.BLOB
    assert find_compatible_type([ossisTypes.BLOB, ossisTypes.BLOB]) == ossisTypes.BLOB
    assert find_compatible_type([ossisTypes.INTEGER, ossisTypes.BOOLEAN]) == ossisTypes.INTEGER
    assert find_compatible_type([ossisTypes.BOOLEAN, ossisTypes.INTEGER]) == ossisTypes.INTEGER
    assert find_compatible_type([ossisTypes.BOOLEAN, ossisTypes.BOOLEAN]) == ossisTypes.BOOLEAN
    assert find_compatible_type([ossisTypes.INTEGER, ossisTypes.DATE]) == ossisTypes.VARCHAR
    assert find_compatible_type([ossisTypes.DATE, ossisTypes.INTEGER]) == ossisTypes.VARCHAR
    assert find_compatible_type([ossisTypes.DATE, ossisTypes.DATE]) == ossisTypes.DATE
    assert find_compatible_type([ossisTypes.INTEGER, ossisTypes.ARRAY]) == ossisTypes.VARCHAR
    assert find_compatible_type([ossisTypes.ARRAY, ossisTypes.INTEGER]) == ossisTypes.VARCHAR
    assert find_compatible_type([ossisTypes.ARRAY, ossisTypes.ARRAY]) == ossisTypes.ARRAY
    assert find_compatible_type([ossisTypes.INTEGER, ossisTypes.STRUCT]) == ossisTypes.VARCHAR
    assert find_compatible_type([ossisTypes.STRUCT, ossisTypes.INTEGER]) == ossisTypes.VARCHAR
    assert find_compatible_type([ossisTypes.STRUCT, ossisTypes.STRUCT]) == ossisTypes.STRUCT
    assert find_compatible_type([ossisTypes.INTEGER, ossisTypes.JSONB]) == ossisTypes.VARCHAR

def test_type_to_numpy_dtype():
    import numpy
    assert ossisTypes.INTEGER.numpy_dtype == numpy.dtype("int64")


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
