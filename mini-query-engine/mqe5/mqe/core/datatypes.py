from dataclasses import dataclass
from typing import Any

import pyarrow as pa

# Convenience aliases for common Arrow data types.
# These are used when defining schema fields and literal columns.
BOOLEAN = pa.bool_()
INT8 = pa.int8()
INT16 = pa.int16()
INT32 = pa.int32()
INT64 = pa.int64()
UINT8 = pa.uint8()
UINT16 = pa.uint16()
UINT32 = pa.uint32()
UINT64 = pa.uint64()
FLOAT32 = pa.float32()
FLOAT64 = pa.float64()
STRING = pa.string()


class ColumnData:
    """
    Abstract column interface used by the query engine.

    Implementations of this class provide a uniform way to access
    columnar data, regardless of where it comes from:

    - Arrow-backed columns (ArrowColumn)
    - Virtual / computed columns
    - Literal columns (LiteralColumn)

    The executor and physical operators should depend on this interface,
    not on pyarrow.Array directly.
    """

    def get_type(self) -> pa.DataType:
        """
        Return the Arrow data type of this column.
        """
        raise NotImplementedError

    def get_value(self, i: int) -> Any:
        """
        Return the Python value at row index `i`.

        Implementations are expected to raise IndexError if `i`
        is out of bounds.
        """
        raise NotImplementedError

    def get_size(self) -> int:
        """
        Return the number of rows (values) in this column.
        """
        raise NotImplementedError


@dataclass
class LiteralColumn(ColumnData):
    """
    Column implementation that represents a single literal value repeated
    `size` times.

    This is useful for expression evaluation: instead of treating literals
    as a special case everywhere, the engine can model them as "virtual"
    columns that behave like any other ColumnData.
    """

    data_type: pa.DataType
    value: Any
    size: int

    def get_type(self) -> pa.DataType:
        """Return the Arrow data type of the literal."""
        return self.data_type

    def get_value(self, i: int) -> Any:
        """
        Return the literal value for row index `i`.

        Since this is a literal column, every row returns the same value.
        An IndexError is raised if `i` is out of range.
        """
        if i < 0 or i >= self.size:
            raise IndexError("Index out of bounds")
        return self.value

    def get_size(self) -> int:
        """Return the number of rows this literal column pretends to have."""
        return self.size


@dataclass
class ArrowColumn(ColumnData):
    """
    Column implementation backed by a pyarrow.Array.

    This is the main bridge between the engine's ColumnData abstraction
    and Arrow's in-memory columnar representation.
    """

    array: pa.Array

    def __post_init__(self) -> None:
        """
        Validate that `array` is indeed a pyarrow.Array instance.

        This keeps the type assumptions in get_type/get_value/get_size safe.
        """
        if not isinstance(self.array, pa.Array):
            raise TypeError(f"Expected pyarrow.Array, got {type(self.array)}")

    def get_type(self) -> pa.DataType:
        """Return the Arrow data type of the underlying array."""
        return self.array.type

    def get_value(self, i: int) -> Any:
        """
        Return the Python value at row index `i`.

        Values are converted from Arrow scalars to native Python objects
        using `.as_py()`.
        """
        return self.array[i].as_py()

    def get_size(self) -> int:
        """Return the number of elements in the underlying array."""
        return len(self.array)
