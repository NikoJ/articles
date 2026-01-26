from dataclasses import dataclass, field
from typing import Optional

from core.datasources import DataSource
from core.tables import SchemaField, TableSchema


class LogicalPlan:
    """
    A logical plan represents a data transformation or action that
    returns a relation (a set of tuples).
    """

    def schema(self) -> TableSchema:
        """
        Returns the schema of the data that will be produced by this logical plan.
        """
        raise NotImplementedError

    def children(self) -> list["LogicalPlan"]:
        """
        Returns the children (inputs) of this logical plan.
        """
        raise NotImplementedError

    def explain(self, verbose: bool = False) -> str:
        """
        Pretty-print the logical plan tree (similar to SQL EXPLAIN).

        - Root is printed without tree connector.
        - Children are printed as a tree with └── / ├── connectors.
        - verbose=True appends the output schema to each node.
        """
        return print_logical_plan(self, verbose=verbose)


class LogicalExpr:
    """
    Logical expression used in logical query plans. Expressions describe computations
    without executing them and are resolved against an input LogicalPlan during
    planning to infer the resulting field name and data type.
    """

    def to_field(self, input: LogicalPlan) -> SchemaField:
        """
        Resolve this expression against the given logical plan and return
        the resulting SchemaField (name and data type).
        """
        raise NotImplementedError


# -----------------------------------------------------------------------------
# Logical Scan
# -----------------------------------------------------------------------------


@dataclass
class Scan(LogicalPlan):
    """
    Scan represents reading data from a DataSource with an optional projection.

    Scan is a leaf node in the logical plan tree.
    """

    source_uri: str
    data_source: DataSource
    projection: Optional[list[str]] = None
    _schema: TableSchema = field(init=False)

    def __post_init__(self) -> None:
        self._schema: TableSchema = self._derive_schema()

    def schema(self) -> TableSchema:
        return self._schema

    def _derive_schema(self) -> TableSchema:
        schema: TableSchema = self.data_source.schema()

        if not self.projection:
            return schema

        # Validate projection early (planning-time)
        available: set[str] = {f.name for f in schema.fields}
        missing: list[str] = [name for name in self.projection if name not in available]
        if missing:
            raise ValueError(
                f"Scan projection contains unknown columns: {missing}. "
                f"Available columns: {sorted(available)}"
            )

        return schema.select(self.projection)

    def children(self) -> list[LogicalPlan]:
        return []

    def __str__(self) -> str:
        if not self.projection:
            return f"Scan: {self.source_uri}; projection=None"
        return f"Scan: {self.source_uri}; projection={self.projection}"


# -----------------------------------------------------------------------------
# Logical Projection
# -----------------------------------------------------------------------------


@dataclass
class Projection(LogicalPlan):
    """
    Projection applies a list of expressions to its input and produces a new schema.

    Example:
        SELECT a, b, (price * qty) AS total FROM t
    """

    input: LogicalPlan
    exprs: list[LogicalExpr]
    _schema: TableSchema = field(init=False)

    def __post_init__(self) -> None:
        self._schema: TableSchema = TableSchema(
            [expr.to_field(self.input) for expr in self.exprs]
        )

    def schema(self) -> TableSchema:
        return self._schema

    def children(self) -> list[LogicalPlan]:
        return [self.input]

    def __str__(self) -> str:
        joined: str = ", ".join(str(e) for e in self.exprs)
        return f"Projection: {joined}"


# -----------------------------------------------------------------------------
# Logical Filter
# -----------------------------------------------------------------------------


@dataclass
class Filter(LogicalPlan):
    """
    Filter selects rows from its input based on a boolean expression.
    Equivalent to SQL WHERE.
    """

    input: LogicalPlan
    expr: LogicalExpr
    _schema: TableSchema = field(init=False)

    def __post_init__(self) -> None:
        # Filter does not change schema, so we can cache it
        self._schema: TableSchema = self.input.schema()

    def schema(self) -> TableSchema:
        return self._schema

    def children(self) -> list[LogicalPlan]:
        return [self.input]

    def __str__(self) -> str:
        return f"Filter: {self.expr}"


# -----------------------------------------------------------------------------
# Print Logical Plan
# -----------------------------------------------------------------------------


def _format_plan_line(plan: LogicalPlan, verbose: bool) -> str:
    if not verbose:
        return str(plan)

    schema = plan.schema()
    fields = ", ".join(f"{f.name}:{f.data_type}" for f in schema.fields)
    return f"{plan}  [{fields}]"


def _explain_lines(
    plan: LogicalPlan,
    prefix: str,
    is_last: bool,
    verbose: bool,
) -> list[str]:
    connector = "└── " if is_last else "├── "
    line = f"{prefix}{connector}{_format_plan_line(plan, verbose=verbose)}"

    children = plan.children()
    if not children:
        return [line]

    next_prefix = prefix + ("    " if is_last else "│   ")
    lines = [line]
    for i, child in enumerate(children):
        last = i == (len(children) - 1)
        lines.extend(
            _explain_lines(child, prefix=next_prefix, is_last=last, verbose=verbose)
        )

    return lines


def print_logical_plan(plan: LogicalPlan, verbose: bool = False) -> str:
    """
    Format a logical plan as a readable tree.
    Example:
        Projection: #id, #name
        └── Filter: #state = 'CO'
            └── Scan: employee.csv; projection=None
    """
    lines: list[str] = [_format_plan_line(plan, verbose=verbose)]

    children = plan.children()
    for i, child in enumerate(children):
        is_last = i == (len(children) - 1)
        lines.extend(_explain_lines(child, prefix="", is_last=is_last, verbose=verbose))

    return "\n".join(lines)