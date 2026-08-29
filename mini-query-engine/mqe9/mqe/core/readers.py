from pathlib import Path
from typing import TYPE_CHECKING

from core.datasources import CSVDataSource, ParquetDataSource
from core.frames import LazyFrame

if TYPE_CHECKING:
    from core.context import ExecutionContext


class DataReader:
    """
    Entry point for creating LazyFrames from external data sources.

    Example:
        ctx.read.csv("data.csv")
        ctx.read.parquet("data.parquet")
    """

    def __init__(self, context: "ExecutionContext") -> None:
        self._context = context

    def csv(
        self,
        path: str | Path,
        *,
        block_size: int = 65_536,
        has_header: bool = True,
        delimiter: str = ",",
    ) -> LazyFrame:
        data_source = CSVDataSource(
            path=Path(path),
            block_size=block_size,
            has_header=has_header,
            delimiter=delimiter,
        )

        return self._context._from_data_source(
            data_source=data_source,
            source_uri=str(path),
        )

    def parquet(
        self,
        path: str | Path,
        *,
        batch_size: int = 65_536,
        use_threads: bool = True,
    ) -> LazyFrame:
        source = ParquetDataSource(
            path=Path(path),
            batch_size=batch_size,
            use_threads=use_threads,
        )

        return self._context._from_data_source(
            data_source=source,
            source_uri=str(path),
        )
