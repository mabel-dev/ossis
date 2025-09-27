# ossis

**ossis** is a common schema library for mabel-dev projects, providing type-safe schema definitions and data validation capabilities across draken, opteryx, and rugo.

This library contains the schema code extracted from [orso](https://github.com/mabel-dev/orso), orso's successor, to serve as a shared foundation for schema management across multiple data processing projects.

## Features

- **Type-safe schema definitions** - Define column types with validation and constraints
- **Data validation** - Validate data records against schema definitions
- **PyArrow integration** - Seamless conversion between ossis and PyArrow schemas
- **JSON serialization** - Serialize and deserialize schemas to/from JSON
- **Flexible type system** - Support for primitives, arrays, decimals, and complex types
- **Column metadata** - Track aliases, descriptions, constraints, and statistics

## Installation

```bash
pip install ossis
```

## Quick Start

```python
from ossis import FlatColumn, RelationSchema, OrsoTypes

# Define a schema
schema = RelationSchema(
    name="users",
    columns=[
        FlatColumn(name="id", type=OrsoTypes.INTEGER, nullable=False),
        FlatColumn(name="name", type=OrsoTypes.VARCHAR, nullable=False),
        FlatColumn(name="email", type=OrsoTypes.VARCHAR),
        FlatColumn(name="age", type=OrsoTypes.INTEGER),
        FlatColumn(name="balance", type=OrsoTypes.DECIMAL, precision=10, scale=2),
    ],
)

# Validate data
data = {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30,
    "balance": 100.50,
}

if schema.validate(data):
    print("Data is valid!")

# Convert to PyArrow
arrow_schema = schema.convert_to_arrow()
```

## Supported Types

ossis supports the following data types:

- **INTEGER** - 64-bit signed integers
- **DOUBLE** - 64-bit floating point numbers
- **DECIMAL(p,s)** - Fixed-point decimal numbers with precision and scale
- **BOOLEAN** - Boolean true/false values
- **VARCHAR** - Variable-length text strings
- **VARCHAR[n]** - Fixed-length text strings
- **BLOB** - Binary large objects
- **DATE** - Date values
- **TIME** - Time values
- **TIMESTAMP** - Date and time values
- **ARRAY<type>** - Arrays of a specific type
- **STRUCT** - Structured data (objects/dictionaries)
- **JSONB** - JSON binary data
- **INTERVAL** - Time intervals

## Schema Definition

### Basic Column

```python
from ossis import FlatColumn, OrsoTypes

# Simple column
col = FlatColumn(name="age", type=OrsoTypes.INTEGER)

# Column with constraints
col = FlatColumn(
    name="email",
    type=OrsoTypes.VARCHAR,
    nullable=False,
    description="User email address"
)

# Decimal column
col = FlatColumn(
    name="price",
    type=OrsoTypes.DECIMAL,
    precision=10,
    scale=2
)
```

### Schema Definition

```python
from ossis import RelationSchema, FlatColumn, OrsoTypes

schema = RelationSchema(
    name="products",
    columns=[
        FlatColumn(name="id", type=OrsoTypes.INTEGER, nullable=False),
        FlatColumn(name="name", type=OrsoTypes.VARCHAR, nullable=False),
        FlatColumn(name="price", type=OrsoTypes.DECIMAL, precision=10, scale=2),
        FlatColumn(name="tags", type=OrsoTypes.ARRAY, element_type=OrsoTypes.VARCHAR),
    ],
    primary_key="id",
)
```

## Data Validation

```python
# Valid data
data = {
    "id": 1,
    "name": "Widget",
    "price": 19.99,
    "tags": ["electronics", "gadget"]
}

try:
    schema.validate(data)
    print("Data is valid!")
except DataValidationError as e:
    print(f"Validation failed: {e}")
```

## PyArrow Integration

ossis provides seamless integration with PyArrow for efficient data processing:

```python
import pyarrow as pa
from ossis.schema import convert_arrow_schema_to_orso_schema, convert_orso_schema_to_arrow_schema

# Convert PyArrow schema to ossis
arrow_schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("name", pa.string()),
])

ossis_schema = convert_arrow_schema_to_orso_schema(arrow_schema)

# Convert ossis schema to PyArrow
arrow_schema = convert_orso_schema_to_arrow_schema(ossis_schema)
```

## Type Parsing

ossis can parse type names from strings:

```python
from ossis import FlatColumn

# Parse type from string
col = FlatColumn(name="col", type="DECIMAL(10,2)")
print(col.precision)  # 10
print(col.scale)      # 2

col = FlatColumn(name="col", type="VARCHAR[100]")
print(col.length)     # 100

col = FlatColumn(name="col", type="ARRAY<INTEGER>")
print(col.element_type)  # OrsoTypes.INTEGER
```

## Serialization

Schemas can be serialized to and from JSON:

```python
# Serialize to JSON
json_str = schema.to_json()

# Serialize to dictionary
schema_dict = schema.to_dict()

# Deserialize from dictionary
restored_schema = RelationSchema.from_dict(schema_dict)
```

## Development

This library is extracted from orso and is designed to be lightweight and focused on schema definitions and validation. It serves as the common foundation for schema management across multiple mabel-dev projects.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.