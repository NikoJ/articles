from dataclasses import dataclass, field
from typing import Iterator, Sequence

import pyarrow as pa
import pyarrow.compute as pc

from core.datasources import DataSource
from core.datatypes import ArrowColumn, ColumnData, LiteralColumn
from core.tables import DataBatch, TableSchema

# -----------------------------------------------------------------------------
# PhysicalPlan base
# -----------------------------------------------------------------------------


class PhysicalPlan:
    """
    A physical plan is an executable query plan.

    Unlike a logical plan (intent), a physical plan knows how to produce DataBatches.
    """

    def schema(self) -> TableSchema:
        """
        Return the output schema produced by this physical operator.
        """
        raise NotImplementedError

    def execute(self) -> Iterator[DataBatch]:
        """
        Execute this operator and stream DataBatches.
        """
        raise NotImplementedError

    def children(self) -> list["PhysicalPlan"]:
        """
        Return input physical operators (children) of this node.
        """
        raise NotImplementedError

    def explain(self, verbose: bool = False) -> str:
        """
        Print the physical plan tree.

        If verbose=True, print schema at each node.
        """
        return print_physical_plan(self, verbose=verbose)


class PhysicalExpr:
    """
    Executable (physical) expression evaluated over a DataBatch.

    Operates in a vectorized way: evaluates the whole batch and returns a
    result ColumnData with the same row count. Used by physical operators
    (e.g., FilterExec, ProjectionExec). Arrow kernels are preferred when possible.
    """

    def evaluate(self, input: DataBatch) -> ColumnData:
        """
        Evaluate this expression on the given DataBatch.
        """
        raise NotImplementedError


# -----------------------------------------------------------------------------
# ScanExec
# -----------------------------------------------------------------------------


@dataclass
class ScanExec(PhysicalPlan):
    """
    Physical scan operator.

    Reads batches from a DataSource.
    """

    data_source: DataSource
    projection: list[str] = field(default_factory=list)
    _schema: TableSchema = field(init=False)

    def __post_init__(self) -> None:
        base_schema = self.data_source.schema()

        # Convention:
        # projection == [] means "read all columns"
        if not self.projection:
            self._schema = base_schema
        else:
            self._schema = base_schema.select(self.projection)

    def schema(self) -> TableSchema:
        return self._schema

    def children(self) -> list[PhysicalPlan]:
        return []

    def execute(self) -> Iterator[DataBatch]:
        # DataSource.scan([]) should mean "read all"
        yield from self.data_source.scan(self.projection)

    def __str__(self) -> str:
        proj = None if not self.projection else self.projection
        return f"ScanExec: projection={proj}, source={type(self.data_source).__name__})"


# -----------------------------------------------------------------------------
# FilterExec
# -----------------------------------------------------------------------------


@dataclass
class FilterExec(PhysicalPlan):
    """
    Physical filter operator.

    Evaluates a predicate expression into a boolean mask and filters every column.

    Important: predicate is a physical Expression (already bound and executable).
    """

    input: PhysicalPlan
    predicate: PhysicalExpr
    # cache
    _schema: TableSchema = field(init=False)
    _empty: DataBatch = field(init=False)

    def __post_init__(self) -> None:
        self._schema = self.input.schema()
        self._empty = self._empty_batch()

    def schema(self) -> TableSchema:
        return self._schema

    def children(self) -> list[PhysicalPlan]:
        return [self.input]

    def execute(self) -> Iterator[DataBatch]:
        for batch in self.input.execute():
            pred_col = self.predicate.evaluate(batch)

            # Validate boolean predicate
            if not pa.types.is_boolean(pred_col.get_type()):
                raise TypeError(
                    f"Filter predicate must return boolean, got: {pred_col.get_type()}"
                )

            # Fast path: literal boolean predicate
            if isinstance(pred_col, LiteralColumn):
                if bool(pred_col.value):
                    yield batch
                else:
                    yield self._empty
                continue

            # Arrow boolean mask
            if not isinstance(pred_col, ArrowColumn):
                # fallback: materialize predicate into Arrow array
                pred_col = ArrowColumn(_materialize(pred_col))

            mask: pa.BooleanArray = pred_col.array
            keep_count: int = count_true(mask)

            filtered_fields: list[ColumnData] = [
                filter_column(col, mask, keep_count) for col in batch.fields
            ]

            yield DataBatch(self.schema(), filtered_fields)

    def _empty_batch(self) -> DataBatch:
        """
        Create an empty DataBatch with the given schema.
        """
        fields: list[ColumnData] = [
            ArrowColumn(pa.array([], type=f.data_type)) for f in self._schema.fields
        ]
        return DataBatch(self._schema, fields)

    def __str__(self) -> str:
        return f"FilterExec: ({self.predicate})"


def filter_column(col: ColumnData, mask: pa.Array, keep_count: int) -> ColumnData:
    """
    Filter a single ColumnData using an Arrow boolean mask.
    """
    if isinstance(col, ArrowColumn):
        out = pc.filter(col.array, mask)
        if isinstance(out, pa.ChunkedArray):
            out = out.combine_chunks()
        return ArrowColumn(out)

    if isinstance(col, LiteralColumn):
        # literal stays literal, only its effective size changes
        return LiteralColumn(col.data_type, col.value, keep_count)

    # generic fallback: materialize
    out = pc.filter(_materialize(col), mask)
    if isinstance(out, pa.ChunkedArray):
        out = out.combine_chunks()
    return ArrowColumn(out)


def count_true(mask: pa.Array) -> int:
    """
    Count number of True values in a boolean mask.
    Nulls are treated as False.
    """
    # cast bool -> int64, fill nulls with 0, then sum
    ints = pc.cast(mask, pa.int64())
    ints = pc.fill_null(ints, 0)
    total = pc.sum(ints)
    return int(total.as_py() or 0)


def _materialize(col: ColumnData) -> pa.Array:
    """
    Convert ColumnData into a pyarrow.Array.
    Used only in fallbacks.
    """
    if isinstance(col, ArrowColumn):
        return col.array
    if isinstance(col, LiteralColumn):
        return pa.array([col.value] * col.size, type=col.data_type)
    raise TypeError(f"Unsupported ColumnData type: {type(col)}")


# -----------------------------------------------------------------------------
# ProjectionExec
# -----------------------------------------------------------------------------


@dataclass
class ProjectionExec(PhysicalPlan):
    """
    Physical projection operator.

    Evaluates a list of expressions and produces a new DataBatch with the provided schema.
    """

    input: PhysicalPlan
    exprs: Sequence[PhysicalExpr]
    _schema: TableSchema

    def __post_init__(self) -> None:
        if len(self.exprs) != len(self._schema.fields):
            raise ValueError(
                f"ProjectionExec expr count mismatch: "
                f"{len(self.exprs)} expressions for {len(self._schema.fields)} fields"
            )

    def schema(self) -> TableSchema:
        return self._schema

    def children(self) -> list[PhysicalPlan]:
        return [self.input]

    def execute(self) -> Iterator[DataBatch]:
        for batch in self.input.execute():
            out_fields: list[ColumnData] = [expr.evaluate(batch) for expr in self.exprs]

            # DataBatch validates same length automatically
            yield DataBatch(self._schema, out_fields)

    def __str__(self) -> str:
        joined = ", ".join(str(e) for e in self.exprs)
        return f"ProjectionExec: {joined}"


# -----------------------------------------------------------------------------
# Print Physical Plan
# -----------------------------------------------------------------------------


def print_physical_plan(
    plan: PhysicalPlan,
    prefix: str = "",
    is_last: bool = True,
    is_root: bool = True,
    verbose: bool = False,
) -> str:
    """
    EXPLAIN-style physical plan printer.

    Root is printed without connectors.
    """
    label = str(plan)
    if verbose:
        label = f"{label}  {format_schema(plan.schema())}"

    if is_root:
        lines = [label]
    else:
        connector = "└── " if is_last else "├── "
        lines = [f"{prefix}{connector}{label}"]

    kids = plan.children()
    if not kids:
        return "\n".join(lines)

    next_prefix = prefix + ("    " if is_last else "│   ")
    for i, child in enumerate(kids):
        last = i == len(kids) - 1
        lines.append(
            print_physical_plan(
                child,
                prefix=next_prefix,
                is_last=last,
                is_root=False,
                verbose=verbose,
            )
        )

    return "\n".join(lines)


def format_schema(schema: TableSchema) -> str:
    """
    Compact schema formatter for verbose explain.
    Example: [id:int64, name:string]
    """
    if not schema.fields:
        return "[]"
    parts = [f"{f.name}:{f.data_type}" for f in schema.fields]
    return "[" + ", ".join(parts) + "]"
