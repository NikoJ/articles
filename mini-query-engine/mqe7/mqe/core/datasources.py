from abc import ABC, abstractmethod
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional

import pyarrow.csv as pacsv
import pyarrow.parquet as papq

from core.tables import ColumnData, DataBatch, TableSchema


class DataSource(ABC):
    """
    Abstract base class for data sources.

    Implementations (InMemoryDataSource, CSVDataSource, ParquetDataSource)
    know how to expose a schema and stream data as DataBatch.
    """

    @abstractmethod
    def schema(self) -> TableSchema:
        """
        Return the schema for the underlying data source.
        """
        ...

    @abstractmethod
    def scan(self, projection: list[str]) -> Iterator[DataBatch]:
        """Scan the data source, selecting the specified columns"""
        ...


@dataclass
class InMemoryDataSource(DataSource):
    """
    In-memory DataSource for demos and unit tests.

    If schema is not provided, it is inferred from the first DataBatch.
    """

    data: list[DataBatch]
    _schema: Optional[TableSchema] = None

    _name_to_index: dict[str, int] = field(init=False)

    def __post_init__(self) -> None:
        if self._schema is None:
            if not self.data:
                raise ValueError(
                    "Cannot infer schema: data is empty and schema was not provided"
                )
            self._schema: TableSchema = self.data[0].schema

        self._name_to_index: dict[str, int] = {
            f.name: i for i, f in enumerate(self._schema.fields)
        }

    def schema(self) -> TableSchema:
        assert self._schema is not None
        return self._schema

    def scan(self, projection: list[str]) -> Iterator[DataBatch]:
        if not projection:
            yield from self.data
            return

        schema: TableSchema = self.schema()

        indices: list[int] = []
        for name in projection:
            idx = self._name_to_index.get(name)
            if idx is None:
                raise ValueError(f"Column '{name}' not found in schema")
            indices.append(idx)

        projected_schema: TableSchema = schema.select(projection)

        for batch in self.data:
            projected_fields: list[ColumnData] = [batch.field(i) for i in indices]
            yield DataBatch(projected_schema, projected_fields)


@dataclass
class CSVDataSource(DataSource):
    """
    CSV-backed data source implemented with PyArrow's streaming CSV reader.

    The source stores only the file path and parsing options. Actual data is
    read lazily when scan() is consumed.

    Notes:
        - block_size is measured in bytes, not rows;
        - an empty projection means all columns;
        - when has_header=False, PyArrow generates names f0, f1, ...
    """

    path: Path
    has_header: bool = True
    delimiter: str = ","
    block_size: int = 65_536
    encoding: str = "utf8"

    _schema: TableSchema | None = field(
        init=False,
        default=None,
        repr=False,
    )

    def __post_init__(self) -> None:
        self.path = Path(self.path)

        if len(self.delimiter) != 1:
            raise ValueError(
                f"CSV delimiter must be exactly one character, got {self.delimiter!r}"
            )

        if self.block_size <= 0:
            raise ValueError(
                f"CSV block_size must be greater than zero, got {self.block_size}"
            )

    def _read_options(self) -> pacsv.ReadOptions:
        return pacsv.ReadOptions(
            block_size=self.block_size,
            encoding=self.encoding,
            autogenerate_column_names=not self.has_header,
        )

    def _parse_options(self) -> pacsv.ParseOptions:
        return pacsv.ParseOptions(
            delimiter=self.delimiter,
        )

    def _open_reader(
        self,
        projection: list[str] | None = None,
    ) -> pacsv.CSVStreamingReader:
        convert_options = pacsv.ConvertOptions(
            include_columns=projection,
        )

        return pacsv.open_csv(
            self.path,
            read_options=self._read_options(),
            parse_options=self._parse_options(),
            convert_options=convert_options,
        )

    def schema(self) -> TableSchema:
        """
        Return and cache the full schema of the CSV source.

        PyArrow infers the schema from the first input block. Calling this
        method performs schema discovery, but does not materialize the whole
        file.
        """
        if self._schema is None:
            with closing(self._open_reader()) as reader:
                self._schema = TableSchema.from_arrow(reader.schema)

        return self._schema

    def scan(
        self,
        projection: list[str],
    ) -> Iterator[DataBatch]:
        """
        Stream CSV data as DataBatch.

        An empty projection means all columns. A non-empty projection is
        passed to PyArrow so excluded columns are not materialized.
        """
        selected_columns: list[str] | None = projection or None

        if selected_columns is not None:
            # Validate against the full source schema before opening a
            # projected reader. This produces an engine-level error rather
            # than exposing a lower-level Arrow exception.
            source_schema = self.schema()
            available_columns = {field.name for field in source_schema.fields}

            missing_columns = [
                name for name in selected_columns if name not in available_columns
            ]

            if missing_columns:
                formatted = ", ".join(repr(name) for name in missing_columns)
                raise ValueError(f"Columns not found in CSV schema: {formatted}")

            if len(selected_columns) != len(set(selected_columns)):
                raise ValueError("CSV projection contains duplicate column names")

        with closing(self._open_reader(selected_columns)) as reader:
            for record_batch in reader:
                yield DataBatch.from_arrow(record_batch)


@dataclass
class ParquetDataSource(DataSource):
    """
    Data source backed by a single Parquet file.

    Data is read lazily as Arrow RecordBatch objects. Column projection
    is passed directly to the Parquet reader.

    Unlike CSV block_size, Parquet batch_size is measured in rows.
    """

    path: Path
    batch_size: int = 65_536
    use_threads: bool = True

    _schema: TableSchema | None = field(
        init=False,
        default=None,
        repr=False,
    )

    def __post_init__(self) -> None:
        self.path = Path(self.path)

        if self.batch_size <= 0:
            raise ValueError(
                f"Parquet batch_size must be greater than zero, got {self.batch_size}"
            )

    def schema(self) -> TableSchema:
        """
        Return and cache the Arrow-compatible schema stored in the file.
        """
        if self._schema is None:
            with papq.ParquetFile(self.path) as parquet_file:
                self._schema = TableSchema.from_arrow(parquet_file.schema_arrow)

        return self._schema

    def scan(
        self,
        projection: list[str],
    ) -> Iterator[DataBatch]:
        """
        Stream the Parquet file as DataBatch objects.

        An empty projection means all columns. A non-empty projection is
        pushed into the Parquet reader.
        """
        selected_columns: list[str] | None = projection or None

        if selected_columns is not None:
            self._validate_projection(selected_columns)

        with papq.ParquetFile(self.path) as parquet_file:
            for record_batch in parquet_file.iter_batches(
                batch_size=self.batch_size,
                columns=selected_columns,
                use_threads=self.use_threads,
            ):
                yield DataBatch.from_arrow(record_batch)

    def _validate_projection(
        self,
        projection: list[str],
    ) -> None:
        if len(projection) != len(set(projection)):
            raise ValueError("Parquet projection contains duplicate column names")

        available_columns = {field.name for field in self.schema().fields}

        missing_columns = [name for name in projection if name not in available_columns]

        if missing_columns:
            formatted_columns = ", ".join(repr(name) for name in missing_columns)

            raise ValueError(
                f"Columns not found in Parquet schema: {formatted_columns}"
            )
