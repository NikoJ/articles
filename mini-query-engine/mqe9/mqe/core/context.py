from collections.abc import Iterator
from typing import Any

import pyarrow as pa  # type: ignore

from core.datasources import DataSource, InMemoryDataSource
from core.datatypes import ArrowColumn
from core.frames import LazyFrame
from core.logical_plan import LogicalPlan, Scan
from core.optimizer import Optimizer
from core.physical_plan import PhysicalPlan
from core.planner import Planner
from core.readers import DataReader
from core.tables import DataBatch, SchemaField, TableSchema


class ExecutionContext:
    """
    Main entry point, similar to Spark/Polars context.

    Provides:
      - read.csv(...) / read.parquet(...)
      - from_batches(...)
      - from_dict(...)
      - execute(plan)
    """

    def __init__(self) -> None:
        self.read: DataReader = DataReader(self)
        self.optimizer: Optimizer = Optimizer()

    def _from_data_source(
        self,
        data_source: DataSource,
        source_uri: str,
    ) -> LazyFrame:
        plan = Scan(
            source_uri=source_uri,
            data_source=data_source,
            projection=[],
        )
        return LazyFrame(plan, self)

    def from_batches(
        self, batches: list[DataBatch], schema: TableSchema | None = None
    ) -> LazyFrame:
        """
        Build a LazyFrame on top of in-memory DataBatches.
        If schema is None, it will be inferred from the first batch.
        """
        ds: DataSource = InMemoryDataSource(data=batches, _schema=schema)
        return self._from_data_source(
            data_source=ds,
            source_uri="in_memory",
        )

    def from_dict(self, data: dict[str, list[Any]]) -> LazyFrame:
        """
        Build a LazyFrame from Python dict-of-lists.

        Example:
          ctx.from_dict({"id":[1,2], "name":["a","b"]})
        """
        if not data:
            raise ValueError("from_dict() expects a non-empty dict of columns")

        lengths: set[int] = {len(v) for v in data.values()}

        if len(lengths) != 1:
            raise ValueError(f"All columns must have the same length, got: {lengths}")

        fields: list[SchemaField] = []
        arr_cols: list[ArrowColumn] = []

        for name, values in data.items():
            arr: pa.Array = pa.array(values)
            fields.append(SchemaField(name, arr.type))
            arr_cols.append(ArrowColumn(arr))

        schema: TableSchema = TableSchema(fields=fields)
        batch: DataBatch = DataBatch(schema=schema, fields=arr_cols)

        return self.from_batches([batch], schema=schema)

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        """
        Run the logical plan through the optimizer's rule pipeline,
        returning an equivalent, cheaper plan.
        """
        return self.optimizer.optimize(plan)

    def generate_physical_plan(
        self, plan: LogicalPlan, optimized: bool = True
    ) -> PhysicalPlan:
        target_plan: LogicalPlan = self.optimize(plan) if optimized else plan
        return Planner().create_physical_plan(target_plan)

    def execute(self, plan: LogicalPlan) -> Iterator[DataBatch]:
        """
        Execute a logical plan:
          logical plan -> optimized logical plan -> physical plan -> execute
        """
        physical_plan: PhysicalPlan = self.generate_physical_plan(plan)
        return physical_plan.execute()
