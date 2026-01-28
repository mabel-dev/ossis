import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import pyarrow
from ossis.types import ossisTypes

from ossis.schema import FlatColumn


def test_column_to_field():
    column = FlatColumn(name="test", type=ossisTypes.VARCHAR)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.string()

    column = FlatColumn(name="test", type=ossisTypes.INTEGER)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.int64()

    column = FlatColumn(name="test", type=ossisTypes.DOUBLE)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.float64()

    column = FlatColumn(name="test", type=ossisTypes.BOOLEAN)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.bool_()

    column = FlatColumn(name="test", type=ossisTypes.TIMESTAMP)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.timestamp("us"),  arrow_field.type

    column = FlatColumn(name="test", type=ossisTypes.DATE)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.date64(), arrow_field.type

    column = FlatColumn(name="test", type=ossisTypes.BLOB)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.binary()

    column = FlatColumn(name="test", type=ossisTypes.DECIMAL)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.decimal128(28, 21), arrow_field.type


def test_struct_column_to_field_metadata():
    nested = [FlatColumn(name="value", type=ossisTypes.INTEGER)]
    column = FlatColumn(name="payload", type=ossisTypes.STRUCT, fields=nested)
    arrow_field = column.arrow_field

    assert arrow_field.type == pyarrow.struct([pyarrow.field("value", pyarrow.int64())])
    assert arrow_field.nullable is True

    fallback_field = FlatColumn(name="missing", type=ossisTypes.STRUCT).arrow_field
    assert fallback_field.type == pyarrow.binary()

def test_array_column_to_field():
    column = FlatColumn(name="test", type=ossisTypes.ARRAY, element_type=ossisTypes.VARCHAR)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.string())

    column = FlatColumn(name="test", type=ossisTypes.ARRAY, element_type=ossisTypes.INTEGER)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.int64())

    column = FlatColumn(name="test", type=ossisTypes.ARRAY, element_type=ossisTypes.DOUBLE)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.float64())

    column = FlatColumn(name="test", type=ossisTypes.ARRAY, element_type=ossisTypes.BOOLEAN)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.bool_())

    column = FlatColumn(name="test", type=ossisTypes.ARRAY, element_type=ossisTypes.TIMESTAMP)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.timestamp("us"))

    column = FlatColumn(name="test", type=ossisTypes.ARRAY, element_type=ossisTypes.BLOB)
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.binary())


def test_column_to_field_name():
    column = FlatColumn(name="test", type="ARRAY<VARCHAR>")
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.string())

    column = FlatColumn(name="test", type="ARRAY<INTEGER>")
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.int64())

    column = FlatColumn(name="test", type="ARRAY<DOUBLE>")
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.float64())

    column = FlatColumn(name="test", type="ARRAY<BOOLEAN>")
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.bool_())

    column = FlatColumn(name="test", type="ARRAY<TIMESTAMP>")
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.timestamp("us"))

    column = FlatColumn(name="test", type="ARRAY<BLOB>")
    arrow_field = column.arrow_field
    assert arrow_field.type == pyarrow.list_(pyarrow.binary())

def test_field_to_column():
    arrow_field = pyarrow.field("test", pyarrow.list_(pyarrow.int64()))
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == ossisTypes.ARRAY
    assert column.element_type == ossisTypes.INTEGER, column.element_type

    arrow_field = pyarrow.field("test", pyarrow.string())
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == ossisTypes.VARCHAR
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.int64())
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == ossisTypes.INTEGER
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.float64())
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == ossisTypes.DOUBLE
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.bool_())
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == ossisTypes.BOOLEAN
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.timestamp("us"))
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == ossisTypes.TIMESTAMP
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.date32())
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == ossisTypes.DATE, column.type
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.binary())
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == ossisTypes.BLOB
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.decimal128(28, 21))
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == ossisTypes.DECIMAL
    assert column.element_type is None

    arrow_field = pyarrow.field("test", pyarrow.list_(pyarrow.string()))
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == ossisTypes.ARRAY
    assert column.element_type == ossisTypes.VARCHAR

    arrow_field = pyarrow.field("test", pyarrow.list_(pyarrow.binary()))
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == ossisTypes.ARRAY
    assert column.element_type == ossisTypes.BLOB

    arrow_field = pyarrow.field(
        "test",
        pyarrow.struct([pyarrow.field("subfield", pyarrow.int32(), nullable=False)])
    )
    column = FlatColumn.from_arrow(arrow_field)
    assert column.name == "test"
    assert column.type == ossisTypes.STRUCT
    assert column.fields is not None
    assert column.fields[0].name == "subfield"
    assert column.fields[0].type == ossisTypes.INTEGER


def test_ossis_types_to_arrow():
    # DECIMAL with explicit precision/scale
    dt = ossisTypes.DECIMAL.to_arrow(precision=28, scale=21)
    assert dt == pyarrow.decimal128(28, 21)

    # ARRAY of INTEGER
    at = ossisTypes.ARRAY.to_arrow(element_type=ossisTypes.INTEGER)
    assert at == pyarrow.list_(pyarrow.int64())


if __name__ == "__main__":  # prgama: nocover
    from tests import run_tests

    run_tests()
