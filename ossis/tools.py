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
from random import getrandbits
from typing import Type, Union

from .exceptions import MissingDependencyError


class DecimalFactory(decimal.Decimal):
    """
    DecimalFactory class extending Python's built-in decimal.Decimal.
    It allows for custom precision and scale settings.
    """

    def __call__(self, value: Union[float, int, str]) -> decimal.Decimal:
        """
        Create a Decimal from value using the configured precision and scale.

        Parameters:
            value: Union[float, int, str]
                The value to be converted to a decimal.

        Returns:
            decimal.Decimal: The quantized decimal.
        """
        context = decimal.Context(
            prec=self.precision,
            rounding=decimal.ROUND_HALF_EVEN,
        )

        if isinstance(value, float):
            value = format(value, ".99g")
        elif isinstance(value, int):
            value = str(value)
        elif isinstance(value, bytes):
            value = value.decode("utf-8")
        value = value.strip()

        if isinstance(value, str) and value.isdigit():
            value += "." + "0"  # minimal fractional part to allow quantize

        decimal_value = context.create_decimal(value)

        safe_scale = min(self.scale, 28)
        factor = decimal.Decimal("10") ** -safe_scale

        # Perform quantization with proper error handling
        try:
            quantized_value = decimal_value.quantize(factor, context=context)
            return quantized_value
        except decimal.InvalidOperation:
            # Fallback if quantization fails
            return decimal_value

    def __str__(self):
        """
        Overridden str method to provide a human-readable string representation.

        Returns:
            str: Description of the DecimalFactory object.
        """
        return f"Decimal({self.scale},{self.precision})"

    @classmethod
    def new_factory(
        cls, precision: int = None, scale: int = None
    ) -> "DecimalFactory":
        """
        Create a new instance of DecimalFactory with attached precision and scale.

        Parameters:
            precision: int
            scale: int

        Returns:
            DecimalFactory instance with .__call__ behavior
        """
        # Create an inert Decimal (value doesn't matter, won't be used)
        instance = decimal.Decimal.__new__(cls, "0")
        object.__setattr__(instance, "scale", scale)
        object.__setattr__(instance, "precision", precision)
        return instance


def arrow_type_map(parquet_type) -> Union[Type, None]:
    """
    Maps PyArrow types to corresponding Python types.

    Parameters:
        parquet_type: lib.DataType
            PyArrow DataType object.

    Returns:
        Type or None: Corresponding Python type for the PyArrow DataType or None if not recognized.

    Raises:
        ValueError: If the PyArrow DataType is not recognized.
    """

    try:
        import pyarrow.lib as lib
    except ImportError as import_error:
        raise MissingDependencyError(import_error.name) from import_error

    type_map = {
        lib.Type_NA: None,
        lib.Type_BOOL: bool,
        lib.Type_INT8: int,
        lib.Type_INT16: int,
        lib.Type_INT32: int,
        lib.Type_INT64: int,
        lib.Type_UINT8: int,
        lib.Type_UINT16: int,
        lib.Type_UINT32: int,
        lib.Type_UINT64: int,
        lib.Type_HALF_FLOAT: float,
        lib.Type_FLOAT: float,
        lib.Type_DOUBLE: float,
        lib.Type_STRING: str,
        lib.Type_LARGE_STRING: str,
        lib.Type_DATE32: datetime.date,
        lib.Type_DATE64: datetime.datetime,
        lib.Type_TIME32: datetime.time,
        lib.Type_TIME64: datetime.time,
        lib.Type_INTERVAL_MONTH_DAY_NANO: datetime.timedelta,
        lib.Type_DURATION: datetime.timedelta,
        lib.Type_LIST: list,
        lib.Type_LARGE_LIST: list,
        lib.Type_FIXED_SIZE_LIST: list,
        lib.Type_STRUCT: dict,
        lib.Type_MAP: dict,
        lib.Type_BINARY: bytes,
        lib.Type_LARGE_BINARY: bytes,
        lib.Type_STRING_VIEW: str,
    }

    if parquet_type.id in type_map:
        return type_map[parquet_type.id]
    elif parquet_type.id in {lib.Type_DECIMAL128, lib.Type_DECIMAL256}:
        return DecimalFactory.new_factory(parquet_type.precision, parquet_type.scale)
    elif parquet_type.id == 18:  # not sure what 18 maps to
        return datetime.datetime

    raise ValueError(f"Unable to map parquet type {parquet_type} ({parquet_type.id})")


def random_string(width: int = 16) -> str:
    """
    Generates a random hexadecimal string of a specified width.

    Parameters:
        width: int, optional
            Length of the output string. Default is 16.

    Returns:
        str: Random hexadecimal string of the given length.

    Note:
        This function generates a random hex string by first converting the
        desired width into the number of random bits needed, and then
        formatting it as a hexadecimal string.
    """
    num_chars = ((width + 1) >> 1) << 3  # Convert length to number of bits
    rand_bytes = getrandbits(num_chars)  # Generate random bits
    # Convert to hex string, clip '0x' prefix, and zero-fill as needed
    return ("000000" + hex(rand_bytes)[2:])[-width:]


def parse_iso(value):
    """
    Parse ISO date/timestamp strings efficiently.
    
    This is a fast but strict parser that assumes the input is either a valid
    ISO format or clearly not a date. It's optimized for speed over flexibility.
    """
    try:
        import numpy

        input_type = type(value)

        if isinstance(value, bytes):
            value = value.decode("utf-8")
            input_type = str

        if input_type == str and value.isdigit():
            value = int(value)
            input_type = int

        if input_type == numpy.datetime64:
            # this can create dates rather than datetimes, so don't return yet
            value = value.astype(datetime.datetime)
            input_type = type(value)
            if input_type is int:
                value /= 1000000000

        if input_type in (int, numpy.int64, float, numpy.float64):
            return datetime.datetime.fromtimestamp(int(value), tz=datetime.timezone.utc).replace(
                tzinfo=None
            )

        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime()

        if input_type == datetime.datetime:
            return value.replace(microsecond=0)
        if input_type == datetime.date:
            return datetime.datetime.combine(value, datetime.time.min)

        # if we're here, we're doing string parsing
        if input_type == str and 10 <= len(value) <= 33:
            if value[-1] == "Z":
                value = value[:-1]
            if "+" in value:
                value = value.split("+")[0]
                if not 10 <= len(value) <= 28:
                    return None
            val_len = len(value)
            if value[4] != "-" or value[7] != "-":
                return None
            if val_len == 10:
                # YYYY-MM-DD
                return datetime.datetime(
                    *map(int, [value[:4], value[5:7], value[8:10]])  # type:ignore
                )
            if val_len >= 16:
                if value[10] not in ("T", " ") and value[13] != ":":
                    return None
                if val_len >= 19 and value[16] == ":":
                    # YYYY-MM-DD HH:MM:SS
                    return datetime.datetime(
                        *map(  # type:ignore
                            int,
                            [
                                value[:4],  # YYYY
                                value[5:7],  # MM
                                value[8:10],  # DD
                                value[11:13],  # HH
                                value[14:16],  # MM
                                value[17:19],  # SS
                            ],
                        )
                    )
                if val_len == 16:
                    # YYYY-MM-DD HH:MM
                    return datetime.datetime(
                        *map(  # type:ignore
                            int,
                            [
                                value[:4],
                                value[5:7],
                                value[8:10],
                                value[11:13],
                                value[14:16],
                            ],
                        )
                    )
        return None
    except (ValueError, TypeError):
        return None