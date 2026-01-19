from typing import Iterator

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
