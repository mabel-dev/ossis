# cython: infer_types=True
# cython: embedsignature=True
# cython: binding=False
# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: overflowcheck=False

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

from cpython.bytes cimport PyBytes_AsString, PyBytes_GET_SIZE
from cpython.unicode cimport PyUnicode_GET_LENGTH
from cpython.object cimport PyObject_Str
from cython cimport int
from datetime import datetime
from ormsgpack import unpackb
from ossis.exceptions import DataError
import numpy as np
cimport numpy as cnp
from numpy cimport ndarray
from libc.stdint cimport int32_t, int64_t
from cpython.dict cimport PyDict_GetItem
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.object cimport PyObject
from libc.stdint cimport uint16_t, uint32_t
from libc.string cimport memcpy

cnp.import_array()

HEADER_PREFIX = b"\x10\x00"
MAXIMUM_RECORD_SIZE = 8 * 1024 * 1024


cpdef from_bytes_cython(bytes data):
    cdef const char* data_ptr = PyBytes_AsString(data)
    cdef Py_ssize_t length = PyBytes_GET_SIZE(data)

    HEADER_SIZE = 14
    # Validate header and size, now using pointer arithmetic
    if length < HEADER_SIZE or (data_ptr[0] & 0xF0 != 0x10):
        raise DataError("Data malformed")

    # Deserialize record bytes
    cdef Py_ssize_t record_size = (
        (<unsigned char>data_ptr[2]) << 24 |
        (<unsigned char>data_ptr[3]) << 16 |
        (<unsigned char>data_ptr[4]) << 8 |
        (<unsigned char>data_ptr[5])
    )

    if record_size != length - HEADER_SIZE:
        raise DataError("Data malformed - incorrect length")

    # Deserialize and post-process
    cdef list raw_tuple = unpackb(data[HEADER_SIZE:])
    cdef list processed_list = []
    cdef object item

    for item in raw_tuple:
        if isinstance(item, list) and len(item) == 2 and item[0] == "__datetime__":
            processed_list.append(datetime.fromtimestamp(item[1]))
        else:
            processed_list.append(item)

    return tuple(processed_list)

cpdef tuple extract_dict_columns(dict data, tuple fields):
    """
    Extracts the given fields from a dictionary and returns them as a tuple.

    Parameters:
        data: dict
            The dictionary to extract fields from.
        fields: tuple
            The field names to extract.

    Returns:
        A tuple containing values from the dictionary for the requested fields.
        Missing fields will have None.
    """
    cdef Py_ssize_t i, num_fields = len(fields)
    cdef PyObject* value_ptr
    cdef list field_data = [None] * num_fields

    for i in range(num_fields):
        value_ptr = PyDict_GetItem(data, fields[i])
        (<list>field_data)[i] = <object>value_ptr if value_ptr is not NULL else None

    return tuple(field_data)


cpdef cnp.ndarray collect_cython(list rows, int32_t[:] columns, int limit=-1):
    """
    Collects columns from a list of tuples (rows).
    """
    cdef Py_ssize_t i, j
    cdef Py_ssize_t num_rows = len(rows)
    cdef Py_ssize_t num_cols = columns.shape[0]
    cdef int32_t col_idx
    cdef object row
    cdef tuple tuple_row
    
    # Early exit if no rows or columns
    if num_rows == 0 or num_cols == 0:
        return np.empty((num_cols, 0), dtype=object)
    
    cdef Py_ssize_t row_width = len(<tuple>rows[0])
    
    # Check if limit is set and within bounds
    if limit >= 0 and limit < num_rows:
        num_rows = limit
    
    # Check if columns are within bounds (only need to check once)
    for j in range(num_cols):
        col_idx = columns[j]
        if col_idx < 0 or col_idx >= row_width:
            raise IndexError(f"Column index out of bounds (0 <= {col_idx} < {row_width})")
    
    # Create result array directly
    cdef cnp.ndarray result_arr = np.empty((num_cols, num_rows), dtype=object)
    cdef object[:, :] result = result_arr
    
    # Specialized fast paths for common column counts
    if num_cols == 1:
        # Single column case (very common)
        col_idx = columns[0]
        for i in range(num_rows):
            tuple_row = <tuple>rows[i]
            result[0, i] = tuple_row[col_idx]
        return result_arr
    elif num_cols == 2:
        # Two column case (also common)
        col_idx0 = columns[0]
        col_idx1 = columns[1]
        for i in range(num_rows):
            tuple_row = <tuple>rows[i]
            result[0, i] = tuple_row[col_idx0]
            result[1, i] = tuple_row[col_idx1]
        return result_arr

    # General case for any number of columns
    for i in range(num_rows):
        tuple_row = <tuple>rows[i]
        for j in range(num_cols):
            result[j, i] = tuple_row[columns[j]]
    return result_arr


cpdef int calculate_data_width(cnp.ndarray column_values):
    """
    Estimate the maximum display width of a column based on string conversion.
    """
    cdef Py_ssize_t i, n = column_values.shape[0]
    cdef int width, max_width = 4
    cdef object value, string_value

    for i in range(n):
        value = column_values[i]
        if value is not None:
            string_value = PyObject_Str(value)  # returns a unicode object
            width = PyUnicode_GET_LENGTH(string_value)
            if width > max_width:
                max_width = width

    return max_width


cpdef list calculate_column_widths(list rows):
    """
    Calculate display widths for all columns at once.
    More efficient than calling calculate_data_width for each column separately.
    
    Parameters:
        rows: list of tuples
            Row data
    
    Returns:
        list of int: Maximum display width for each column
    """
    cdef Py_ssize_t i, j
    cdef Py_ssize_t num_rows = len(rows)
    
    if num_rows == 0:
        return []
    
    cdef tuple first_row = <tuple>rows[0]
    cdef Py_ssize_t num_cols = len(first_row)
    
    # Initialize widths to minimum of 4
    cdef list widths = [4] * num_cols
    cdef int width
    cdef tuple row
    cdef object value, string_value
    
    for i in range(num_rows):
        row = <tuple>rows[i]
        for j in range(num_cols):
            value = row[j]
            if value is not None:
                string_value = PyObject_Str(value)
                width = PyUnicode_GET_LENGTH(string_value)
                if width > widths[j]:
                    widths[j] = width
    
    return widths


cpdef list extract_columns_to_lists(list rows, int limit=-1):
    """
    Fast column extraction for Arrow table conversion.
    Converts row-oriented data to column-oriented lists.
    
    Parameters:
        rows: list of tuples
            Row-oriented data
        limit: int
            Maximum number of rows to materialize (-1 means all rows)
    
    Returns:
        list of lists: Column-oriented data
    """
    cdef Py_ssize_t i, j
    cdef Py_ssize_t num_rows = len(rows)
    cdef Py_ssize_t empty_cols

    if limit >= 0 and limit < num_rows:
        num_rows = limit

    if num_rows == 0:
        if len(rows) == 0:
            return []
        empty_cols = len(<tuple>rows[0])
        return [[] for _ in range(empty_cols)]
    
    cdef tuple first_row = <tuple>rows[0]
    cdef Py_ssize_t num_cols = len(first_row)
    
    # Pre-allocate result list and column lists with the right size
    cdef list columns = []
    cdef list col_data
    cdef tuple row
    cdef object value
    
    # Create pre-allocated lists for each column
    for j in range(num_cols):
        col_data = [None] * num_rows
        columns.append(col_data)
    
    # Fill the column lists - direct indexing is faster than append
    for i in range(num_rows):
        row = <tuple>rows[i]
        for j in range(num_cols):
            (<list>columns[j])[i] = row[j]
    
    return columns





def process_table(table, row_factory, int max_chunksize) -> list:
    """
    Processes a PyArrow table and applies a row factory function to each row.

    Parameters:
        table: PyArrow Table
            The input table to process.
        row_factory: function
            A function applied to each row.
        max_chunksize: int
            The batch size to process at a time.

    Returns:
        A list of transformed rows.
    """
    cdef list rows = [None] * table.num_rows
    cdef int64_t i = 0, k
    cdef list columns
    cdef int num_cols
    cdef int batch_size
    cdef object column_method
    cdef object row_iter
    cdef object row_values
    cdef object factory = row_factory

    for batch in table.to_batches(max_chunksize):
        # Convert batch columns to Python lists (column-oriented)
        # This is faster than converting to dicts first
        num_cols = batch.num_columns
        batch_size = batch.num_rows

        if batch_size == 0:
            continue

        if num_cols == 0:
            for k in range(batch_size):
                rows[i] = factory(())
                i += 1
            continue

        column_method = batch.column
        columns = [column_method(col_idx).to_pylist() for col_idx in range(num_cols)]
        row_iter = zip(*columns)

        for row_values in row_iter:
            rows[i] = factory(row_values)
            i += 1
    return rows


# Single-file float quantizers:
#   - parse_float16(x): quantize to IEEE-754 binary16 ("half") precision, return as Python float (binary64)
#   - parse_float32(x): quantize to IEEE-754 binary32 precision, return as Python float (binary64)
#   - parse_float64(x): Python float (binary64)
#   - parse_float(x, bits=16|32|64): dispatch
#
# Notes:
#   * Python has only binary64 float. These functions QUANTIZE to the target precision,
#     then widen back to binary64.
#   * float16 quantization is implemented here without NumPy.
#
# Build tip (example):
#   cythonize -i this_file.pyx


cdef inline uint32_t _float_to_u32(float f) nogil:
    cdef uint32_t u
    memcpy(&u, &f, sizeof(uint32_t))
    return u


cdef inline float _u32_to_float(uint32_t u) nogil:
    cdef float f
    memcpy(&f, &u, sizeof(uint32_t))
    return f


cdef inline uint16_t _float32_to_half_bits(float f) nogil:
    """
    Convert IEEE-754 binary32 to binary16 bit-pattern (round-to-nearest-even).

    This handles:
      - zeros
      - subnormals
      - normals
      - infinities
      - NaNs (quiet NaN with payload best-effort)
    """
    cdef uint32_t x = _float_to_u32(f)
    cdef uint32_t sign = (x >> 16) & 0x8000u
    cdef int exp = <int>((x >> 23) & 0xFFu)
    cdef uint32_t mant = x & 0x007FFFFFu

    cdef uint16_t h
    cdef int new_exp
    cdef uint32_t mant_rounded
    cdef uint32_t round_bit
    cdef uint32_t sticky
    cdef uint32_t mant_11  # includes implicit leading 1 for normals
    cdef uint32_t shift

    # NaN / Inf
    if exp == 0xFF:
        if mant == 0:
            return <uint16_t>(sign | 0x7C00u)  # infinity
        # NaN: preserve some payload; ensure mantissa non-zero
        mant = mant >> 13
        if mant == 0:
            mant = 1
        return <uint16_t>(sign | 0x7C00u | <uint16_t>(mant))

    # Convert exponent from bias 127 -> bias 15
    new_exp = exp - 127 + 15

    # Underflow to subnormal or zero
    if new_exp <= 0:
        if new_exp < -10:
            # Too small: becomes signed zero
            return <uint16_t>sign

        # Subnormal half: exponent = 0, mantissa derived from normal float mantissa
        # For normal float32 (exp != 0), implicit leading 1. For exp==0, it's already subnormal.
        if exp == 0:
            # float32 subnormal: treat as having exponent 1 (so implicit leading 0)
            mant_11 = mant
        else:
            mant_11 = mant | 0x00800000u  # add implicit leading 1 (1.mant)

        # shift right to align to half subnormal mantissa (10 bits)
        # new_exp <= 0 => we need to shift by (1 - new_exp) plus 13 to drop to 10 bits
        shift = <uint32_t>(1 - new_exp)
        # total shift from 23-bit mantissa (plus leading 1) to 10-bit half mantissa:
        # 23 - 10 = 13, plus additional shift for subnormal alignment.
        shift = shift + 13

        # Round-to-nearest-even while shifting.
        # We want mantissa = mant_11 >> shift, with rounding based on bits shifted out.
        if shift >= 32:
            # Extreme case: everything shifts out
            return <uint16_t>sign

        mant_rounded = mant_11 >> shift
        round_bit = (mant_11 >> (shift - 1)) & 1u if shift > 0 else 0u
        sticky = 0u
        if shift > 1:
            sticky = mant_11 & ((1u << (shift - 1)) - 1u)

        # Round ties to even
        if round_bit and (sticky or (mant_rounded & 1u)):
            mant_rounded += 1u

        return <uint16_t>(sign | <uint16_t>(mant_rounded & 0x03FFu))

    # Overflow to infinity
    if new_exp >= 31:
        return <uint16_t>(sign | 0x7C00u)

    # Normal half
    # Drop mantissa from 23 bits to 10 bits with round-to-nearest-even.
    mant_rounded = mant >> 13
    round_bit = (mant >> 12) & 1u
    sticky = mant & 0x0FFFu

    if round_bit and (sticky or (mant_rounded & 1u)):
        mant_rounded += 1u
        if mant_rounded == 0x0400u:
            # mantissa overflow: increment exponent, mantissa becomes 0
            mant_rounded = 0u
            new_exp += 1
            if new_exp >= 31:
                return <uint16_t>(sign | 0x7C00u)

    h = <uint16_t>(sign | (<uint16_t>(new_exp) << 10) | <uint16_t>(mant_rounded & 0x03FFu))
    return h


cdef inline float _half_bits_to_float32(uint16_t h) nogil:
    """
    Convert IEEE-754 binary16 bit-pattern to binary32 float.
    """
    cdef uint32_t sign = (<uint32_t>(h & 0x8000u)) << 16
    cdef uint32_t exp = (h >> 10) & 0x001Fu
    cdef uint32_t mant = h & 0x03FFu

    cdef uint32_t out
    cdef int e
    cdef uint32_t m

    if exp == 0:
        if mant == 0:
            # +/- zero
            out = sign
            return _u32_to_float(out)
        # subnormal: normalize
        e = -14
        m = mant
        while (m & 0x0400u) == 0:
            m <<= 1
            e -= 1
        m &= 0x03FFu
        out = sign | (<uint32_t>(e + 127) << 23) | (m << 13)
        return _u32_to_float(out)

    if exp == 0x1Fu:
        # Inf/NaN
        if mant == 0:
            out = sign | 0x7F800000u
            return _u32_to_float(out)
        # NaN: propagate payload into float32 mantissa
        out = sign | 0x7F800000u | (mant << 13)
        return _u32_to_float(out)

    # normal
    out = sign | (<uint32_t>(exp - 15 + 127) << 23) | (mant << 13)
    return _u32_to_float(out)


def parse_float16(object x, **kwargs) -> float:
    """
    Quantize input to float16 precision (IEEE-754 binary16), then return as Python float (binary64).
    """
    cdef double d = float(x)
    cdef float f32 = <float>d
    cdef uint16_t h = _float32_to_half_bits(f32)
    cdef float back = _half_bits_to_float32(h)
    return <double>back


def parse_float32(object x, **kwargs) -> float:
    """
    Quantize input to float32 precision (IEEE-754 binary32), then return as Python float (binary64).
    """
    cdef double d = float(x)
    cdef float f32 = <float>d
    return <double>f32


def parse_float64(object x, **kwargs) -> float:
    """
    Python float is IEEE-754 binary64 already.
    """
    return float(x)
