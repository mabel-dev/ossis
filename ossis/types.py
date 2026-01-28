# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import decimal
import re
from enum import Enum
from typing import Any
from typing import Iterable
from typing import Tuple
from typing import Type
from typing import Union
from warnings import warn

import orjson

from ossis.compute import parse_float16
from ossis.compute import parse_float32
from ossis.compute import parse_float64
from ossis.tools import parse_iso


def _parse_type(type_str: str) -> Union[str, Tuple[str, Tuple[int, ...]]]:
    """
    Parses a SQL type string into its base type and optional parameters.

    Parameters:
        type_str (str): The type definition string (e.g., 'DECIMAL(10,2)', 'VARCHAR[255]', 'ARRAY<VARCHAR>').

    Returns:
        Union[str, Tuple[str, Tuple[int, ...]]]:
            - Just the base type (e.g., "INTEGER", "TEXT").
            - A tuple with the base type and a tuple of integer parameters if applicable (e.g., ("DECIMAL", (10, 2))).
    """

    # Match ARRAY<TYPE>
    array_match = re.match(r"ARRAY<([\w\s\[\]\(\)]+)>", type_str)
    if array_match:
        return "ARRAY", (array_match.group(1),)

    # Match DECIMAL(p,s)
    decimal_match = re.match(r"DECIMAL\((\d+),\s*(\d+)\)", type_str)
    if decimal_match:
        precision, scale = map(int, decimal_match.groups())
        return "DECIMAL", (precision, scale)

    # Match VARCHAR[n]
    varchar_match = re.match(r"VARCHAR\[(\d+)\]", type_str)
    if varchar_match:
        length = int(varchar_match.group(1))
        return "VARCHAR", (length,)

    # Match VARBINARY[n]
    varbinary_match = re.match(r"VARBINARY\[(\d+)\]", type_str)
    if varbinary_match:
        size = int(varbinary_match.group(1))
        return "VARBINARY", (size,)

    # Match BLOB[n] (deprecated alias for VARBINARY)
    blob_match = re.match(r"BLOB\[(\d+)\]", type_str)
    if blob_match:
        size = int(blob_match.group(1))
        warn("Column type BLOB is deprecated; treating as VARBINARY. Use VARBINARY instead.")
        return "VARBINARY", (size,)

    # If no parameters, return base type as a string
    return type_str.upper()


def get_ossis_type(type_str: str) -> "ossisTypes":
    """
    Convert a type string to an ossisType enum value with full type information.

    This function parses a type string and returns an ossisType enum value with
    all relevant attributes set (precision, scale, length, element types).

    Parameters:
        type_str (str): The type definition string (e.g., 'INTEGER', 'ARRAY<INTEGER>', 'DECIMAL(10,2)').

    Returns:
        ossisTypes: The corresponding ossisType enum value with all attributes properly set.

    Raises:
        ValueError: If the type string is not recognized.

    Examples:
        >>> t = get_ossis_type("INTEGER")
        >>> t == ossisTypes.INTEGER
        True

        >>> t = get_ossis_type("DECIMAL(10,2)")
        >>> t._precision
        10
        >>> t._scale
        2

        >>> t = get_ossis_type("VARCHAR[255]")
        >>> t._length
        255

        >>> t = get_ossis_type("ARRAY<INTEGER>")
        >>> t._element_type == ossisTypes.INTEGER
        True
    """
    if not type_str:
        raise ValueError("Type string cannot be empty")

    # Use the existing from_name method which handles all type attributes
    _type, _length, _precision, _scale, _element_type = ossisTypes.from_name(type_str)

    if _type == 0 or _type is None:
        raise ValueError(f"Unknown type '{type_str}'")

    # Attach all the metadata to the returned type instance
    # The __init__ method initializes these as None, so we just update them
    object.__setattr__(_type, "_length", _length)
    object.__setattr__(_type, "_precision", _precision)
    object.__setattr__(_type, "_scale", _scale)
    object.__setattr__(_type, "_element_type", _element_type)

    return _type


class ossisTypes(str, Enum):
    """
    The names of the types supported by ossis
    """

    ARRAY = "ARRAY"
    BLOB = "BLOB"  # deprecated; use VARBINARY
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    DECIMAL = "DECIMAL"
    DOUBLE = "DOUBLE"  # mark as deprecated, use FLOAT64
    FLOAT16 = "FLOAT16"  # new
    FLOAT32 = "FLOAT32"  # new
    FLOAT64 = "FLOAT64"  # new
    INTEGER = "INTEGER"  # mark as deprecated (sized ints)
    INT8 = "INT8"  # new
    UINT8 = "UINT8"  # new
    INT16 = "INT16"  # new
    UINT16 = "UINT16"  # new
    INT32 = "INT32"  # new
    UINT32 = "UINT32"  # new
    INT64 = "INT64"  # new
    UINT64 = "UINT64"  # new
    INTERVAL = "INTERVAL"
    STRUCT = "STRUCT"  # mark as deprecated, use JSONB
    TIMESTAMP = "TIMESTAMP"
    TIME = "TIME"
    VARCHAR = "VARCHAR"
    VARBINARY = "VARBINARY"  # new
    NULL = "NULL"
    JSONB = "JSONB"
    _MISSING_TYPE = 0

    def __init__(self, *args, **kwargs):
        self._precision: int = None
        self._scale: int = None
        self._element_type: "ossisTypes" = None
        self._length: int = None

        str.__init__(self)
        Enum.__init__(self)

    def is_numeric(self):
        """is the typle number-based"""
        return self in (
            self.INTEGER,
            self.DOUBLE,
            self.DECIMAL,
            self.BOOLEAN,
            self.INT8,
            self.UINT8,
            self.INT16,
            self.UINT16,
            self.INT32,
            self.UINT32,
            self.INT64,
            self.UINT64,
            self.FLOAT16,
            self.FLOAT32,
            self.FLOAT64,
        )

    def is_temporal(self):
        """is the type time-based"""
        return self in (self.DATE, self.TIME, self.TIMESTAMP)

    def is_large_object(self):
        """is the type arbitrary length string"""
        return self in (self.VARCHAR, self.BLOB, self.VARBINARY)

    def is_complex(self):
        return self in (self.ARRAY, self.STRUCT, self.JSONB, self.INTERVAL)

    def __str__(self):
        if self.value == self.ARRAY and self._element_type is not None:
            return f"ARRAY<{self._element_type}>"
        if self.value == self.DECIMAL and self._precision is not None and self._scale is not None:
            return f"DECIMAL({self._precision}, {self._scale})"
        if self.value == self.VARCHAR and self._length is not None:
            return f"VARCHAR[{self._length}]"
        if self.value in (self.BLOB, self.VARBINARY) and self._length is not None:
            return f"VARBINARY[{self._length}]"
        return self.value

    def parse(self, value: Any, **kwargs) -> Any:
        kwargs["length"] = kwargs.get("length", self._length)
        kwargs["precision"] = kwargs.get("precision", self._precision)
        kwargs["scale"] = kwargs.get("scale", self._scale)
        kwargs["element_type"] = kwargs.get("element_type", self._element_type)

        if value is None:
            return None
        return ossis_TO_PYTHON_PARSER[self.value](value, **kwargs)

    @property
    def python_type(self) -> Type:
        return ossis_TO_PYTHON_MAP.get(self)

    @property
    def numpy_dtype(self):
        import numpy

        MAP = {
            ossisTypes.ARRAY: numpy.dtype("O"),
            ossisTypes.BLOB: numpy.dtype("S"),
            ossisTypes.VARBINARY: numpy.dtype("S"),
            ossisTypes.BOOLEAN: numpy.dtype("?"),
            ossisTypes.DATE: numpy.dtype("datetime64[D]"),  # [2.5e16 BC, 2.5e16 AD]
            ossisTypes.DECIMAL: numpy.dtype("O"),
            ossisTypes.DOUBLE: numpy.dtype("float64"),
            ossisTypes.FLOAT16: numpy.dtype("float16"),
            ossisTypes.FLOAT32: numpy.dtype("float32"),
            ossisTypes.FLOAT64: numpy.dtype("float64"),
            ossisTypes.INTEGER: numpy.dtype("int64"),
            ossisTypes.INT8: numpy.dtype("int8"),
            ossisTypes.UINT8: numpy.dtype("uint8"),
            ossisTypes.INT16: numpy.dtype("int16"),
            ossisTypes.UINT16: numpy.dtype("uint16"),
            ossisTypes.INT32: numpy.dtype("int32"),
            ossisTypes.UINT32: numpy.dtype("uint32"),
            ossisTypes.INT64: numpy.dtype("int64"),
            ossisTypes.UINT64: numpy.dtype("uint64"),
            ossisTypes.INTERVAL: numpy.dtype("m"),
            ossisTypes.STRUCT: numpy.dtype("O"),
            ossisTypes.TIMESTAMP: numpy.dtype("datetime64[us]"),  # [290301 BC, 294241 AD]
            ossisTypes.TIME: numpy.dtype("O"),
            ossisTypes.VARCHAR: numpy.dtype("U"),
            ossisTypes.NULL: numpy.dtype("O"),
        }
        return MAP.get(self)

    def to_arrow(
        self, *, element_type: "ossisTypes" = None, precision: int = None, scale: int = None
    ):
        """
        Return a pyarrow DataType corresponding to this ossisType.

        Parameters:
            element_type: Optional[ossisTypes] - element type for ARRAY overrides
            precision: Optional[int] - precision override for DECIMAL
            scale: Optional[int] - scale override for DECIMAL
        """
        from decimal import getcontext as _getcontext

        import pyarrow as pa

        _precision = (
            precision
            if precision is not None
            else (self._precision if self._precision is not None else _getcontext().prec)
        )
        _scale = scale if scale is not None else (self._scale if self._scale is not None else 10)

        type_map = {
            ossisTypes.BOOLEAN: pa.bool_(),
            ossisTypes.BLOB: pa.binary(),
            ossisTypes.VARBINARY: pa.binary(),
            ossisTypes.DATE: pa.date64(),
            ossisTypes.TIMESTAMP: pa.timestamp("us"),
            ossisTypes.TIME: pa.time32("ms"),
            ossisTypes.INTERVAL: pa.month_day_nano_interval(),
            ossisTypes.DECIMAL: pa.decimal128(_precision, _scale),
            ossisTypes.DOUBLE: pa.float64(),
            ossisTypes.INTEGER: pa.int64(),
            ossisTypes.INT8: pa.int8(),
            ossisTypes.UINT8: pa.uint8(),
            ossisTypes.INT16: pa.int16(),
            ossisTypes.UINT16: pa.uint16(),
            ossisTypes.INT32: pa.int32(),
            ossisTypes.UINT32: pa.uint32(),
            ossisTypes.INT64: pa.int64(),
            ossisTypes.UINT64: pa.uint64(),
            ossisTypes.FLOAT16: pa.float16(),
            ossisTypes.FLOAT32: pa.float32(),
            ossisTypes.FLOAT64: pa.float64(),
            ossisTypes.VARCHAR: pa.string(),
            ossisTypes.JSONB: pa.binary(),
            ossisTypes.NULL: pa.null(),
        }

        if self == ossisTypes.ARRAY:
            elem = element_type or self._element_type or ossisTypes.VARCHAR
            if isinstance(elem, ossisTypes):
                elem_pa_type = elem.to_arrow()
            elif isinstance(elem, str) and elem in ossisTypes.__members__:
                elem_pa_type = ossisTypes[elem].to_arrow()
            else:
                elem_pa_type = pa.string()
            return pa.list_(elem_pa_type)

        # For STRUCT we don't have child field information here; return binary as a sensible fallback.
        if self == ossisTypes.STRUCT:
            return pa.binary()

        return type_map.get(self, pa.string())

    @staticmethod
    def from_name(name: str) -> tuple:
        _length = None
        _precision = None
        _scale = None
        _element_type = None

        if name is None:
            return (ossisTypes._MISSING_TYPE, _length, _precision, _scale, _element_type)

        type_name = str(name).upper()
        parsed_types = _parse_type(type_name)
        if isinstance(parsed_types, str):
            if parsed_types == "ARRAY":
                warn("Column type ARRAY without element_type, defaulting to VARCHAR.")
                _type = ossisTypes.ARRAY
                _element_type = ossisTypes.VARCHAR
            elif parsed_types in ("NUMERIC", "BSON", "STRUCT", "LIST"):
                raise ValueError(f"Column type {parsed_types} is deprecated.")
            elif parsed_types in ossisTypes.__members__:
                _type = ossisTypes[parsed_types]
            elif parsed_types == "VARBINARY":
                _type = ossisTypes.VARBINARY
            elif parsed_types == "BLOB":
                # This branch is mostly defensive; _parse_type handles BLOB[...] already.
                warn("Column type BLOB is deprecated; use VARBINARY instead.")
                _type = ossisTypes.VARBINARY
            elif parsed_types == "DOUBLE":
                warn("Column type DOUBLE is deprecated; use FLOAT64 instead.")
                _type = ossisTypes.FLOAT64
            elif parsed_types == "INTEGER":
                warn("Column type INTEGER is deprecated; use INT64 instead.")
                _type = ossisTypes.INT64
            elif (
                type_name == "0"
                or type_name == 0
                or type_name == "VARIANT"
                or type_name == "MISSING"
            ):
                _type = 0
            else:
                raise ValueError(f"Unknown column type '{name}''.")
        elif parsed_types[0] == "ARRAY":
            _type = ossisTypes.ARRAY
            _element_type = parsed_types[1][0]
            if not _element_type.startswith(
                (
                    "INT",
                    "UINT",
                    "FLOAT",
                    "VARCHAR",
                    "VARBINARY",
                    "BOOLEAN",
                    "DATE",
                    "TIMESTAMP",
                    "TIME",
                )
            ):
                raise ValueError(f"Invalid element type '{_element_type}' for ARRAY type.")
            if _element_type in ossisTypes.__members__:
                _type = ossisTypes.ARRAY
                _element_type = ossisTypes[_element_type]
            else:
                raise ValueError(f"Unknown column type '{_element_type}'.")
        elif parsed_types[0] == "DECIMAL":
            _type = ossisTypes.DECIMAL
            _precision, _scale = parsed_types[1]
            if _precision < 0 or _precision > 38:
                raise ValueError(f"Invalid precision '{_precision}' for DECIMAL type.")
            if _scale < 0 or _scale > 38:
                raise ValueError(f"Invalid scale '{_scale}' for DECIMAL type.")
            if _precision < _scale:
                raise ValueError(
                    "Precision must be equal to or greater than scale for DECIMAL type."
                )
        elif parsed_types[0] == "VARCHAR":
            _type = ossisTypes.VARCHAR
            _length = parsed_types[1][0]
        elif parsed_types[0] == "VARBINARY":
            _type = ossisTypes.VARBINARY
            _length = parsed_types[1][0]
        elif parsed_types[0] == "BLOB":
            # Deprecated alias
            warn("Column type BLOB is deprecated; use VARBINARY instead.")
            _type = ossisTypes.VARBINARY
            _length = parsed_types[1][0]
        else:
            raise ValueError(f"Unknown column type '{_type}'.")

        return (_type, _length, _precision, _scale, _element_type)


BOOLEAN_STRINGS = (
    "TRUE",
    "ON",
    "YES",
    "1",
    "1.0",
    "T",
    "Y",
    b"TRUE",
    b"ON",
    b"YES",
    b"1",
    b"1.0",
    b"T",
    b"Y",
)


def parse_decimal(value, *, precision=None, scale=None, **kwargs):
    from ossis.tools import DecimalFactory

    if value is None:
        return None

    scale = 21 if scale is None else int(scale)
    precision = 38 if precision is None else int(precision)
    value = (
        value.as_py()
        if hasattr(value, "as_py")
        else (
            value.item()
            if hasattr(value, "item") and not isinstance(value, (list, dict, tuple))
            else value
        )
    )
    if isinstance(value, float):
        value = format(value, ".99g")
    elif isinstance(value, int):
        value = str(value)
    elif isinstance(value, bytes):
        value = value.decode("utf-8")
    value = value.strip()
    factory = DecimalFactory.new_factory(precision, scale)
    return factory(value)


ossis_TO_PYTHON_MAP: dict = {
    ossisTypes.BOOLEAN: bool,
    ossisTypes.BLOB: bytes,
    ossisTypes.VARBINARY: bytes,
    ossisTypes.DATE: datetime.date,
    ossisTypes.TIMESTAMP: datetime.datetime,
    ossisTypes.TIME: datetime.time,
    ossisTypes.INTERVAL: datetime.timedelta,
    ossisTypes.STRUCT: dict,
    ossisTypes.DECIMAL: decimal.Decimal,
    ossisTypes.DOUBLE: float,
    ossisTypes.FLOAT16: float,
    ossisTypes.FLOAT32: float,
    ossisTypes.FLOAT64: float,
    ossisTypes.INTEGER: int,
    ossisTypes.INT8: int,
    ossisTypes.UINT8: int,
    ossisTypes.INT16: int,
    ossisTypes.UINT16: int,
    ossisTypes.INT32: int,
    ossisTypes.UINT32: int,
    ossisTypes.INT64: int,
    ossisTypes.UINT64: int,
    ossisTypes.ARRAY: list,
    ossisTypes.VARCHAR: str,
    ossisTypes.JSONB: bytes,
    ossisTypes.NULL: None,
}

PYTHON_TO_ossis_MAP: dict = {
    value: key for key, value in ossis_TO_PYTHON_MAP.items() if key != ossisTypes.JSONB
}
PYTHON_TO_ossis_MAP.update({tuple: ossisTypes.ARRAY, set: ossisTypes.ARRAY})  # map other python types

# Prefer the generic INTEGER mapping for plain Python ints to avoid ambiguity
# with smaller integer types like INT8/UINT8 which also map to int.
PYTHON_TO_ossis_MAP[int] = ossisTypes.INT64
# Prefer BLOB when mapping raw bytes -> ossisTypes (binary arrow types should become BLOB)
PYTHON_TO_ossis_MAP[bytes] = ossisTypes.VARBINARY


def parse_boolean(x, **kwargs):
    return (x if isinstance(x, (bytes, str)) else str(x)).upper() in BOOLEAN_STRINGS


def parse_bytes(x, **kwargs):
    length = kwargs.get("length")
    if isinstance(x, (dict, list, tuple, set)):
        value = orjson.dumps(x)
    else:
        value = str(x).encode("utf-8") if not isinstance(x, bytes) else x
    if length:
        value = value[:length]
    return value


def parse_date(x, **kwargs):
    result = parse_iso(x)
    if result is None:
        raise ValueError("Invalid date.")
    return result.date()


def parse_time(x, **kwargs):
    result = parse_iso(x)
    if result is None:
        raise ValueError("Invalid date.")
    return result.time()


def parse_varchar(x, **kwargs):
    byte_version = parse_bytes(x, **kwargs)
    if isinstance(byte_version, bytes):
        return byte_version.decode("utf-8")
    return str(byte_version)


def parse_array(x, **kwargs):
    element_type = kwargs.get("element_type")
    if not isinstance(x, (list, tuple, set)):
        x = orjson.loads(x)
    if element_type is None:
        return x
    parser = element_type.parse
    return [parser(v) for v in x]


def parse_int8(x, **kwargs):
    val = int(x)
    if val < -128 or val > 127:
        raise ValueError(f"INT8 value out of range: {val}")
    return val


def parse_uint8(x, **kwargs):
    val = int(x)
    if val < 0 or val > 255:
        raise ValueError(f"UINT8 value out of range: {val}")
    return val


def parse_int16(x, **kwargs):
    val = int(x)
    if val < -32768 or val > 32767:
        raise ValueError(f"INT16 value out of range: {val}")
    return val


def parse_uint16(x, **kwargs):
    val = int(x)
    if val < 0 or val > 65535:
        raise ValueError(f"UINT16 value out of range: {val}")
    return val


def parse_int32(x, **kwargs):
    val = int(x)
    if val < -2147483648 or val > 2147483647:
        raise ValueError(f"INT32 value out of range: {val}")
    return val


def parse_uint32(x, **kwargs):
    val = int(x)
    if val < 0 or val > 4294967295:
        raise ValueError(f"UINT32 value out of range: {val}")
    return val


def parse_int64(x, **kwargs):
    val = int(x)
    if val < -9223372036854775808 or val > 9223372036854775807:
        raise ValueError(f"INT64 value out of range: {val}")
    return val


def parse_uint64(x, **kwargs):
    val = int(x)
    if val < 0 or val > 18446744073709551615:
        raise ValueError(f"UINT64 value out of range: {val}")
    return val


def parse_null(x, **kwargs):
    return None


def parse_timestamp(x, **kwargs):
    result = parse_iso(x)
    if result is None:
        raise ValueError(f"Invalid timestamp.")
    return result


def parse_interval(x, **kwargs):
    return datetime.timedelta(x)


ossis_TO_PYTHON_PARSER: dict = {
    ossisTypes.BOOLEAN: parse_boolean,
    ossisTypes.BLOB: parse_bytes,
    ossisTypes.VARBINARY: parse_bytes,
    ossisTypes.DATE: parse_date,
    ossisTypes.TIMESTAMP: parse_timestamp,
    ossisTypes.TIME: parse_time,
    ossisTypes.INTERVAL: parse_interval,
    ossisTypes.STRUCT: parse_bytes,
    ossisTypes.DECIMAL: parse_decimal,
    ossisTypes.DOUBLE: parse_float64,
    ossisTypes.FLOAT16: parse_float16,
    ossisTypes.FLOAT32: parse_float32,
    ossisTypes.FLOAT64: parse_float64,
    ossisTypes.INTEGER: parse_int64,
    ossisTypes.INT8: parse_int8,
    ossisTypes.UINT8: parse_uint8,
    ossisTypes.INT16: parse_int16,
    ossisTypes.UINT16: parse_uint16,
    ossisTypes.INT32: parse_int32,
    ossisTypes.UINT32: parse_uint32,
    ossisTypes.INT64: parse_int64,
    ossisTypes.UINT64: parse_uint64,
    ossisTypes.ARRAY: parse_array,
    ossisTypes.VARCHAR: parse_varchar,
    ossisTypes.JSONB: parse_bytes,
    ossisTypes.NULL: parse_null,
}


def find_compatible_type(types: Iterable[ossisTypes], default=ossisTypes.VARCHAR) -> ossisTypes:
    """
    Find the most compatible type that can represent all input types.

    Parameters:
        types (list): List of ossisTypes to find a compatible type for

    Returns:
        ossisTypes: The most compatible type that can represent all input types

    Examples:
        >>> ossisTypes.find_compatible_type([ossisTypes.INTEGER, ossisTypes.DOUBLE])
        ossisTypes.DOUBLE
        >>> ossisTypes.find_compatible_type([ossisTypes.BLOB, ossisTypes.VARCHAR])
        ossisTypes.VARCHAR
    """
    if not types:
        return ossisTypes.NULL

    # Handle single type case
    if len(set(types)) == 1:
        return types[0]

    # Define type promotion hierarchy
    type_hierarchy = {
        # Numeric promotion
        ossisTypes.BOOLEAN: 1,
        ossisTypes.INTEGER: 2,
        ossisTypes.INT8: 2,
        ossisTypes.UINT8: 2,
        ossisTypes.INT16: 2,
        ossisTypes.UINT16: 2,
        ossisTypes.INT32: 2,
        ossisTypes.UINT32: 2,
        ossisTypes.INT64: 2,
        ossisTypes.UINT64: 2,
        ossisTypes.DOUBLE: 3,
        ossisTypes.FLOAT16: 3,
        ossisTypes.FLOAT32: 3,
        ossisTypes.FLOAT64: 3,
        ossisTypes.DECIMAL: 4,
        # Temporal promotion
        ossisTypes.DATE: 1,
        ossisTypes.TIMESTAMP: 2,
        # String/binary promotion
        ossisTypes.BLOB: 1,
        ossisTypes.VARBINARY: 1,
        ossisTypes.VARCHAR: 2,
    }

    # First check if all types are in the same category
    if all(t.is_numeric() for t in types):
        return max(types, key=lambda t: type_hierarchy.get(t, 0))
    if all(t.is_temporal() for t in types):
        return max(types, key=lambda t: type_hierarchy.get(t, 0))
    if all(t.is_large_object() for t in types):
        return max(types, key=lambda t: type_hierarchy.get(t, 0))
    if all(
        t
        in (
            ossisTypes.BLOB,
            ossisTypes.VARBINARY,
            ossisTypes.STRUCT,
            ossisTypes.JSONB,
            ossisTypes.VARCHAR,
        )
        for t in types
    ):
        return ossisTypes.VARBINARY

    # For heterogeneous types, default to the most flexible type
    if any(t in (ossisTypes.BLOB, ossisTypes.VARBINARY) for t in types):
        return ossisTypes.VARBINARY
    return default
