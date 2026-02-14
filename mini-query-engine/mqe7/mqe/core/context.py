from typing import Any, Iterator, Optional

import pyarrow as pa

from core.datasources import DataSource, InMemoryDataSource
from core.datatypes import ArrowColumn
from core.frames import LazyFrame
from core.logical_plan import LogicalPlan, Scan
from core.optimizer import Optimizer
from core.physical_plan import PhysicalPlan
from core.planner import Planner
from core.tables import DataBatch, SchemaField, TableSchema


class ExecutionContext:
    """
    Main entry point, similar to Spark/Polars context.

    Provides:
      - from_batches(...)
      - from_dict(...)
      - execute(plan)
    """

    def from_batches(
        self, batches: list[DataBatch], schema: Optional[TableSchema] = None
    ) -> LazyFrame:
        """
        Build a LazyFrame on top of in-memory DataBatches.
        If schema is None, it will be inferred from the first batch.
        """
        ds: DataSource = InMemoryDataSource(data=batches, _schema=schema)
        plan: Scan = Scan(source_uri="in_memory", data_source=ds, projection=[])
        return LazyFrame(plan, self)

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

    def execute(self, plan: LogicalPlan) -> Iterator[DataBatch]:
        """
        Execute a logical plan:
          logical plan -> optimized logical plan -> physical plan -> execute
        """

        optimized: LogicalPlan = Optimizer().optimize(plan)  # just a dummy
        physical_plan: PhysicalPlan = Planner().create_physical_plan(optimized)
        return physical_plan.execute()

    def generate_physical_plan(self, plan: LogicalPlan) -> PhysicalPlan:
        optimized: LogicalPlan = Optimizer().optimize(plan)  # just a dummy
        return Planner().create_physical_plan(optimized)
