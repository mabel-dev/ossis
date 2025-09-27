#!/usr/bin/env python3
"""
Example demonstrating ossis schema library usage.

This example shows how to define schemas, validate data, and work with PyArrow.
"""

from ossis import FlatColumn, RelationSchema, OrsoTypes
from ossis.exceptions import DataValidationError, ExcessColumnsInDataError


def main():
    print("=== ossis Schema Library Example ===\n")
    
    # Define a schema for a user table
    schema = RelationSchema(
        name="users",
        columns=[
            FlatColumn(name="id", type=OrsoTypes.INTEGER, nullable=False),
            FlatColumn(name="name", type=OrsoTypes.VARCHAR, nullable=False),
            FlatColumn(name="email", type=OrsoTypes.VARCHAR),
            FlatColumn(name="age", type=OrsoTypes.INTEGER),
            FlatColumn(name="balance", type=OrsoTypes.DECIMAL, precision=10, scale=2),
            FlatColumn(name="tags", type=OrsoTypes.ARRAY, element_type=OrsoTypes.VARCHAR),
            FlatColumn(name="active", type=OrsoTypes.BOOLEAN, default=True),
        ],
    )
    
    print(f"Created schema '{schema.name}' with {schema.num_columns} columns:")
    for col in schema.columns:
        nullable = "" if col.nullable else " (NOT NULL)"
        print(f"  - {col.name}: {col.type}{nullable}")
    
    print("\n=== Data Validation Examples ===\n")
    
    # Valid data
    valid_data = {
        "id": 1,
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30,
        "balance": 100.50,
        "tags": ["developer", "python"],
        "active": True,
    }
    
    try:
        schema.validate(valid_data)
        print("✓ Valid data passed validation")
    except DataValidationError as e:
        print(f"✗ Validation failed: {e}")
    
    # Invalid data - missing required field
    print("\nTesting validation with missing required field:")
    invalid_data1 = {
        # missing "id" which is not nullable
        "name": "Jane Doe",
        "email": "jane@example.com",
        "age": 25,
        "balance": 200.75,
        "tags": ["designer", "ui"],
        "active": True,
    }
    
    try:
        schema.validate(invalid_data1)
        print("✓ Data passed validation")
    except DataValidationError as e:
        print(f"✗ Expected validation failure: {e}")
    
    # Invalid data - extra field
    print("\nTesting validation with extra field:")
    invalid_data2 = {
        "id": 2,
        "name": "Bob Smith", 
        "email": "bob@example.com",
        "age": 35,
        "balance": 300.25,
        "tags": ["manager"],
        "active": True,
        "department": "Engineering",  # extra field not in schema
    }
    
    try:
        schema.validate(invalid_data2)
        print("✓ Data passed validation")
    except ExcessColumnsInDataError as e:
        print(f"✗ Expected validation failure: {e}")
    
    print("\n=== Type Parsing Examples ===\n")
    
    # Parse types from strings
    examples = [
        ("INTEGER", "Simple integer type"),
        ("VARCHAR[100]", "Variable character with length limit"),
        ("DECIMAL(10,2)", "Decimal with precision and scale"),
        ("ARRAY<VARCHAR>", "Array of strings"),
        ("BOOLEAN", "Boolean type"),
    ]
    
    for type_str, description in examples:
        col = FlatColumn(name="example", type=type_str)
        print(f"'{type_str}' -> {col.type} - {description}")
        if hasattr(col, 'length') and col.length:
            print(f"  Length: {col.length}")
        if hasattr(col, 'precision') and col.precision:
            print(f"  Precision: {col.precision}, Scale: {col.scale}")
        if hasattr(col, 'element_type') and col.element_type:
            print(f"  Element Type: {col.element_type}")
    
    print("\n=== PyArrow Integration ===\n")
    
    try:
        import pyarrow as pa
        from ossis.schema import convert_orso_schema_to_arrow_schema
        
        # Convert schema to PyArrow
        arrow_schema = convert_orso_schema_to_arrow_schema(schema)
        print("✓ Successfully converted ossis schema to PyArrow schema:")
        print(f"  Arrow schema: {arrow_schema}")
        
        # Show field types
        for field in arrow_schema:
            print(f"  {field.name}: {field.type}")
            
    except ImportError:
        print("PyArrow not available for integration demo")
    
    print("\n=== Schema Serialization ===\n")
    
    # Convert to dictionary
    schema_dict = schema.to_dict()
    print("✓ Schema serialized to dictionary")
    print(f"  Keys: {list(schema_dict.keys())}")
    
    # Recreate from dictionary
    restored_schema = RelationSchema.from_dict(schema_dict)
    print("✓ Schema restored from dictionary")
    print(f"  Original: {len(schema.columns)} columns")
    print(f"  Restored: {len(restored_schema.columns)} columns")
    
    print("\n=== Example Complete ===")


if __name__ == "__main__":
    main()