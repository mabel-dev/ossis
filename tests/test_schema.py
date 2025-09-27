"""
Test basic schema functionality.
"""

from ossis import FlatColumn, RelationSchema, OrsoTypes
from ossis.exceptions import DataValidationError, ExcessColumnsInDataError

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


def test_create_column():
    """Test creating a basic column"""
    col = FlatColumn(name="test_col", type=OrsoTypes.INTEGER)
    assert col.name == "test_col"
    assert col.type == OrsoTypes.INTEGER
    assert col.nullable is True  # default


def test_create_schema():
    """Test creating a basic schema"""
    schema = RelationSchema(
        name="test_schema",
        columns=[
            FlatColumn(name="id", type=OrsoTypes.INTEGER),
            FlatColumn(name="name", type=OrsoTypes.VARCHAR),
        ]
    )
    assert schema.name == "test_schema"
    assert len(schema.columns) == 2
    assert schema.column_names == ["id", "name"]


def test_find_column():
    """Test finding columns by name"""
    column = cities_schema.find_column("language")
    assert column.name == "language"

    column = cities_schema.find_column("nonexistent")
    assert column is None


def test_validate_with_valid_data():
    """Test schema validation with valid data"""
    data = {
        "name": "New York",
        "population": 8623000,
        "country": "United States",
        "founded": "1624",
        "area": 783.8,
        "language": "English",
    }
    assert cities_schema.validate(data) == True


def test_validate_with_missing_column():
    """Test validation fails with missing required column"""
    data = {
        "name": "London",
        "population": 8908081,
        "country": "United Kingdom",
        "founded": "43 AD",
        "language": "English",
        # missing 'area'
    }
    try:
        cities_schema.validate(data)
        assert False, "Should have raised DataValidationError"
    except DataValidationError as e:
        assert "area" in str(e)


def test_validate_with_non_nullable_column():
    """Test validation fails with null value for non-nullable column"""
    data = {
        "name": None,  # non-nullable
        "population": 13929286,
        "country": "Japan",
        "founded": "1457",
        "area": 2187.66,
        "language": "Japanese",
    }
    try:
        cities_schema.validate(data)
        assert False, "Should have raised DataValidationError"
    except DataValidationError as e:
        assert "name" in str(e)


def test_validate_with_additional_columns():
    """Test validation fails with extra columns"""
    data = {
        "name": "Berlin",
        "population": 3769495,
        "country": "Germany",
        "founded": "1237",
        "area": 891.8,
        "language": "German",
        "continent": "Europe",  # extra column
    }
    try:
        cities_schema.validate(data)
        assert False, "Should have raised ExcessColumnsInDataError"
    except ExcessColumnsInDataError as e:
        assert "continent" in str(e)


def test_type_parsing():
    """Test type name parsing"""
    col = FlatColumn(name="col", type="INTEGER")
    assert col.type == OrsoTypes.INTEGER
    
    col = FlatColumn(name="col", type="VARCHAR[12]")
    assert col.type == OrsoTypes.VARCHAR
    assert col.length == 12
    
    col = FlatColumn(name="col", type="DECIMAL(10,2)")
    assert col.type == OrsoTypes.DECIMAL
    assert col.precision == 10
    assert col.scale == 2


def test_schema_serialization():
    """Test schema to/from dict conversion"""
    original = RelationSchema(
        name="test",
        columns=[
            FlatColumn(name="id", type=OrsoTypes.INTEGER),
            FlatColumn(name="name", type=OrsoTypes.VARCHAR),
        ]
    )
    
    # Convert to dict and back
    schema_dict = original.to_dict()
    restored = RelationSchema.from_dict(schema_dict)
    
    assert restored.name == original.name
    assert len(restored.columns) == len(original.columns)
    assert restored.column_names == original.column_names


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