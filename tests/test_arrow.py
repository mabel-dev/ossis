"""
Test PyArrow integration.
"""

import pyarrow as pa
from ossis import FlatColumn, RelationSchema, OrsoTypes
from ossis.schema import convert_arrow_schema_to_orso_schema, convert_orso_schema_to_arrow_schema


def test_arrow_field_conversion():
    """Test converting PyArrow field to FlatColumn"""
    # String field
    arrow_field = pa.field("test_string", pa.string())
    orso_col = FlatColumn.from_arrow(arrow_field)
    assert orso_col.name == "test_string"
    assert orso_col.type == OrsoTypes.VARCHAR
    
    # Integer field
    arrow_field = pa.field("test_int", pa.int64())
    orso_col = FlatColumn.from_arrow(arrow_field)
    assert orso_col.name == "test_int"
    assert orso_col.type == OrsoTypes.INTEGER
    
    # Boolean field
    arrow_field = pa.field("test_bool", pa.bool_())
    orso_col = FlatColumn.from_arrow(arrow_field)
    assert orso_col.name == "test_bool"
    assert orso_col.type == OrsoTypes.BOOLEAN


def test_orso_to_arrow_field():
    """Test converting FlatColumn to PyArrow field"""
    # String column
    col = FlatColumn(name="test_string", type=OrsoTypes.VARCHAR)
    arrow_field = col.arrow_field
    assert arrow_field.name == "test_string"
    assert arrow_field.type == pa.string()
    
    # Integer column
    col = FlatColumn(name="test_int", type=OrsoTypes.INTEGER)
    arrow_field = col.arrow_field
    assert arrow_field.name == "test_int"
    assert arrow_field.type == pa.int64()
    
    # Boolean column
    col = FlatColumn(name="test_bool", type=OrsoTypes.BOOLEAN)
    arrow_field = col.arrow_field
    assert arrow_field.name == "test_bool"
    assert arrow_field.type == pa.bool_()


def test_schema_conversion():
    """Test converting between Arrow and Orso schemas"""
    # Create Arrow schema
    arrow_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float64()),
        pa.field("active", pa.bool_()),
    ])
    
    # Convert to Orso
    orso_schema = convert_arrow_schema_to_orso_schema(arrow_schema)
    assert orso_schema.name == "arrow"
    assert len(orso_schema.columns) == 4
    assert orso_schema.column("id").type == OrsoTypes.INTEGER
    assert orso_schema.column("name").type == OrsoTypes.VARCHAR
    assert orso_schema.column("score").type == OrsoTypes.DOUBLE
    assert orso_schema.column("active").type == OrsoTypes.BOOLEAN
    
    # Convert back to Arrow
    arrow_schema2 = convert_orso_schema_to_arrow_schema(orso_schema)
    assert len(arrow_schema2) == 4
    assert arrow_schema2.field("id").type == pa.int64()
    assert arrow_schema2.field("name").type == pa.string()
    assert arrow_schema2.field("score").type == pa.float64()
    assert arrow_schema2.field("active").type == pa.bool_()


def test_decimal_conversion():
    """Test decimal type conversion"""
    # Arrow decimal to Orso
    arrow_field = pa.field("amount", pa.decimal128(10, 2))
    orso_col = FlatColumn.from_arrow(arrow_field)
    assert orso_col.name == "amount"
    assert orso_col.type == OrsoTypes.DECIMAL
    assert orso_col.precision == 10
    assert orso_col.scale == 2
    
    # Orso decimal to Arrow
    orso_col = FlatColumn(name="price", type=OrsoTypes.DECIMAL, precision=10, scale=2)
    arrow_field = orso_col.arrow_field
    assert arrow_field.name == "price"
    assert arrow_field.type == pa.decimal128(10, 2)


def test_array_conversion():
    """Test array type conversion"""
    # Arrow list to Orso array
    arrow_field = pa.field("tags", pa.list_(pa.string()))
    orso_col = FlatColumn.from_arrow(arrow_field)
    assert orso_col.name == "tags"
    assert orso_col.type == OrsoTypes.ARRAY
    assert orso_col.element_type == OrsoTypes.VARCHAR
    
    # Orso array to Arrow list
    orso_col = FlatColumn(name="numbers", type=OrsoTypes.ARRAY, element_type=OrsoTypes.INTEGER)
    arrow_field = orso_col.arrow_field
    assert arrow_field.name == "numbers"
    assert isinstance(arrow_field.type, pa.ListType)


def test_struct_to_blob():
    """Test struct mapping to blob"""
    struct_type = pa.struct([pa.field('subfield', pa.int32())])
    arrow_col = pa.field(name="column", type=struct_type)

    assert arrow_col.type == struct_type

    orso_reformed = FlatColumn.from_arrow(arrow_col)
    assert orso_reformed.type == OrsoTypes.STRUCT

    orso_struct_as_blob_col = FlatColumn.from_arrow(arrow_col, mappable_as_binary=True)
    assert orso_struct_as_blob_col.type == OrsoTypes.BLOB


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