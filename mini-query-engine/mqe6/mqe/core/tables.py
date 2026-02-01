from dataclasses import dataclass
from typing import Sequence

import pyarrow as pa

from core.datatypes import ColumnData


@dataclass
class SchemaField:
    """
    Represents a single field (column) in a table schema.

    This is a thin wrapper over an Arrow data type with a name.
    It is used by the query engine as part of TableSchema, but can be
    converted back into a native pyarrow.Field when needed.
    """

    name: str
    data_type: pa.DataType

    def to_arrow(self) -> pa.Field:
        """
        Convert this SchemaField into a pyarrow.Field instance.
        """
        return pa.field(self.name, self.data_type)


@dataclass
class TableSchema:
    """
    Describes the structure of a table: a list of named, typed fields.

    This is the logical schema used inside the engine. It mirrors
    pyarrow.Schema, but is kept as a separate type so that higher-level
    components do not depend directly on Arrow everywhere.
    """

    fields: list[SchemaField]

    def __post_init__(self) -> None:
        """
        Validate that all field names are unique.

        Duplicate column names would make planning and expression binding
        ambiguous, so they are rejected early.
        """
        names = [f.name for f in self.fields]
        if len(names) != len(set(names)):
            raise ValueError("TableSchema contains duplicate field names")

    def select(self, names: list[str]) -> "TableSchema":
        """
        Return a new TableSchema containing only fields listed in `names`.

        - preserves the order of `names`
        - raises a clear error if any column is missing
        """
        if not names:
            return TableSchema([])

        index: dict[str, SchemaField] = {f.name: f for f in self.fields}

        missing: list[str] = [name for name in names if name not in index]
        if missing:
            raise ValueError(
                f"Unknown columns in projection: {missing}. "
                f"Available columns: {sorted(index.keys())}"
            )

        selected_fields: list[SchemaField] = [index[name] for name in names]
        return TableSchema(selected_fields)

    def to_arrow(self) -> pa.Schema:
        """
        Convert this TableSchema into a pyarrow.Schema.
        """
        return pa.schema([field.to_arrow() for field in self.fields])

    def __str__(self) -> str:
        return ", ".join(f"{f.name}:{f.data_type}" for f in self.fields)


@dataclass
class DataBatch:
    """
    A small columnar batch of data: schema + a sequence of ColumnData.

    This is the main unit of data that physical operators pass between
    each other. All columns in a DataBatch:

    - share the same TableSchema,
    - have the same number of rows,
    - are exposed via the ColumnData abstraction (Arrow-backed, literal, etc.).

    Conceptually, this is similar to a pyarrow.RecordBatch, but adapted
    to the engine's own schema and column abstractions.
    """

    schema: TableSchema
    fields: Sequence[ColumnData]

    def __post_init__(self) -> None:
        """
        Validate that the number of columns matches the schema and that
        all columns have the same length.
        """
        if len(self.schema.fields) != len(self.fields):
            raise ValueError(
                f"TableSchema has {len(self.schema.fields)} fields, "
                f"but DataBatch has {len(self.fields)} columns"
            )
        if self.fields:
            _size = self.fields[0].get_size()
            for idx, col in enumerate(self.fields[1:], start=1):
                if col.get_size() != _size:
                    raise ValueError(
                        f"Column {idx} has size {col.get_size()}, expected {_size}"
                    )

    def row_count(self) -> int:
        """
        Return the number of rows in this batch.
        """
        return self.fields[0].get_size() if self.fields else 0

    def column_count(self) -> int:
        """
        Return the number of columns in this batch.
        """
        return len(self.fields)

    def field(self, i: int) -> ColumnData:
        """
        Return the i-th column (as ColumnData).
        """
        return self.fields[i]

    def _to_tab_table_str(self) -> str:
        """
        Render the batch as a tab-separated table with column names, types and values.

        Intended for debugging / logging, not for production CSV export.

        Format:

            ———
            col1<TAB>col2<TAB>...
            type1<TAB>type2<TAB>...
            ———
            v11<TAB>v12<TAB>...
            v21<TAB>v22<TAB>...
            ...

        Where:
        - the first line is a visual separator,
        - the second line lists column names,
        - the third line lists column types,
        - the rest are data rows.
        """
        schema = self.schema
        fields = self.fields
        row_count = self.row_count()
        column_count = self.column_count()

        def fmt(value: object) -> str:
            """
            Convert a value to a human-readable string for debugging.
            """
            if value is None:
                return "null"
            if isinstance(value, bytes):
                return value.decode("utf-8")
            return str(value)

        # Column headers
        col_names = [field.name for field in schema.fields]
        # col_types = [str(field.data_type) for field in schema.fields]

        lines: list[str] = []
        header_col_names: str = "\t".join(col_names)

        # Visual separator
        separator: str = "-" * len(header_col_names) + "-" * len(col_names) * 2
        lines.append(separator)
        # Column names
        lines.append(header_col_names)
        # Visual separator
        lines.append(separator)

        # Data rows
        for row_index in range(row_count):
            row_values = [
                fmt(fields[col_index].get_value(row_index))
                for col_index in range(column_count)
            ]
            lines.append("\t".join(row_values))

        return "\n".join(lines)

    def __str__(self) -> str:
        """
        Return a compact textual summary of the batch, including
        row/column counts and a tabular view of the data.
        """
        return (
            f"Rows:    {self.row_count()}\n"
            f"Columns: {self.column_count()}\n"
            f"Data:\n"
            f"{self._to_tab_table_str()}"
        )
