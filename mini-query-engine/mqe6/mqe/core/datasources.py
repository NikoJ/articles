from dataclasses import dataclass, field
from typing import Iterator, Optional

from core.tables import DataBatch, TableSchema


class DataSource:
    """
    Mock implementation. The implementation will be in future articles.
    """

    def schema(self) -> TableSchema:
        """
        Return the schema for the underlying data source.
        """
        raise NotImplementedError

    def scan(self, projection: list[str]) -> Iterator[DataBatch]:
        """Scan the data source, selecting the specified columns"""
        raise NotImplementedError


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
            self._schema = self.data[0].schema

        self._name_to_index = {f.name: i for i, f in enumerate(self._schema.fields)}

    def schema(self) -> TableSchema:
        assert self._schema is not None
        return self._schema

    def scan(self, projection: list[str]) -> Iterator[DataBatch]:
        if not projection:
            yield from self.data
            return

        schema = self.schema()

        indices: list[int] = []
        for name in projection:
            idx = self._name_to_index.get(name)
            if idx is None:
                raise ValueError(f"Column '{name}' not found in schema")
            indices.append(idx)

        projected_schema = schema.select(projection)

        for batch in self.data:
            projected_fields = [batch.field(i) for i in indices]
            yield DataBatch(projected_schema, projected_fields)
