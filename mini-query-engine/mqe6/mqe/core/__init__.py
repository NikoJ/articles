from typing import Any, Optional

from .context import ExecutionContext
from .datatypes import ArrowColumn
from .frames import DataFrame, LazyFrame
from .logical_expr import col
from .tables import DataBatch, SchemaField, TableSchema

_default_ctx = ExecutionContext()


def get_context() -> ExecutionContext:
    return _default_ctx


def from_dict(data: dict[str, list[Any]]) -> LazyFrame:
    return _default_ctx.from_dict(data)


def from_batches(
    batches: list[DataBatch], schema: Optional[TableSchema] = None
) -> LazyFrame:
    return _default_ctx.from_batches(batches, schema=schema)


__all__ = [
    "ExecutionContext",
    "DataFrame",
    "LazyFrame",
    "DataBatch",
    "TableSchema",
    "SchemaField",
    "ArrowColumn",
    "col",
    "get_context",
    "from_dict",
    "from_batches",
]
