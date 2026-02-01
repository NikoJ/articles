from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterator, Union

if TYPE_CHECKING:
    from core.context import ExecutionContext

from core.logical_expr import Column, LogicalExprNode
from core.logical_plan import Filter, LogicalPlan, Projection
from core.tables import DataBatch, TableSchema

ExprLike = Union[str, LogicalExprNode]


@dataclass(frozen=True)
class LazyFrame:
    """
    Lazy DataFrame-like API.

    Holds a LogicalPlan and builds a query tree without executing it.
    Execution happens on collect().
    """

    _plan: LogicalPlan
    _ctx: "ExecutionContext"

    def select(self, *exprs: ExprLike) -> "LazyFrame":
        def _to_expr(x: ExprLike) -> LogicalExprNode:
            match x:
                case LogicalExprNode():
                    return x
                case str():
                    return Column(x)
                case _:
                    raise TypeError(f"Unsupported expression type: {type(x)}")

        # Allow select(["a", "b"]) as convenience
        if len(exprs) == 1 and isinstance(exprs[0], list):
            expr_list: list[LogicalExprNode] = [_to_expr(x) for x in exprs[0]]
        else:
            expr_list = [_to_expr(x) for x in exprs]

        return LazyFrame(Projection(self._plan, expr_list), self._ctx)

    def filter(self, predicate: LogicalExprNode) -> "LazyFrame":
        return LazyFrame(Filter(self._plan, predicate), self._ctx)

    def where(self, predicate: LogicalExprNode) -> "LazyFrame":
        return self.filter(predicate)

    def schema(self) -> TableSchema:
        return self._plan.schema()

    def _execute(self) -> Iterator[DataBatch]:
        return self._ctx.execute(self._plan)

    def _collect_batches(self) -> list[DataBatch]:
        return list(self._execute())

    def collect(self) -> "DataFrame":
        batches: list[DataBatch] = self._collect_batches()
        return DataFrame(batches=batches, _ctx=self._ctx)

    def explain(self, verbose: bool = False):
        print("\n===== LOGICAL PLAN =====\n")
        print(self._plan.explain(verbose))

        print("\n===== PHYSICAL PLAN =====\n")
        print(self._ctx.generate_physical_plan(self._plan).explain(verbose))


@dataclass
class DataFrame:
    """
    Eager DataFrame.

    Represents materialized results in memory.
    """

    batches: list[DataBatch]
    _ctx: "ExecutionContext"

    def select(self, *exprs: ExprLike) -> "DataFrame":
        return self.lazy().select(*exprs).collect()

    def filter(self, predicate: LogicalExprNode) -> "DataFrame":
        return self.lazy().filter(predicate).collect()

    def where(self, predicate: LogicalExprNode) -> "DataFrame":
        return self.filter(predicate)

    def schema(self) -> TableSchema:
        if not self.batches:
            raise ValueError("DataFrame is empty, schema is unknown")
        return self.batches[0].schema

    def lazy(self) -> LazyFrame:
        return self._ctx.from_batches(self.batches)

    def __str__(self) -> str:
        total_rows: int = sum(b.row_count() for b in self.batches)
        total_cols: int = self.batches[0].column_count()
        schema: str = str(self.schema())
        header: str = (
            f"DataFrame Summary\n"
            f"Rows:    {total_rows}\n"
            f"Columns: {total_cols}\n"
            f"Batches: {len(self.batches)}\n"
            f"Schema:  {schema}\n" + "=" * (len(str(schema)) + 10)
        )

        body = "\n\n".join(f"[Batch {i}]\n{b}" for i, b in enumerate(self.batches))

        return f"{header}\n{body}"
