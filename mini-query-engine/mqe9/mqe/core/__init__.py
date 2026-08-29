from typing import Any

from core.readers import DataReader

from .context import ExecutionContext
from .datatypes import ArrowColumn
from .frames import DataFrame, LazyFrame
from .logical_expr import col
from .tables import DataBatch, SchemaField, TableSchema

_default_ctx = ExecutionContext()

read: DataReader = _default_ctx.read


def get_context() -> ExecutionContext:
    return _default_ctx


def from_dict(data: dict[str, list[Any]]) -> LazyFrame:
    return _default_ctx.from_dict(data)


def from_batches(
    batches: list[DataBatch], schema: TableSchema | None = None
) -> LazyFrame:
    return _default_ctx.from_batches(batches, schema=schema)


__all__ = [
    "ArrowColumn",
    "DataBatch",
    "DataFrame",
    "ExecutionContext",
    "LazyFrame",
    "SchemaField",
    "TableSchema",
    "col",
    "from_batches",
    "from_dict",
    "get_context",
    "read",
]
