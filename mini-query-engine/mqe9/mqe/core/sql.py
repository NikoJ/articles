from typing import TYPE_CHECKING

import sqlglot
from sqlglot import exp

from core.logical_expr import (
    Add,
    Alias,
    And,
    Column,
    Divide,
    Eq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    Multiply,
    Neq,
    Or,
    Subtract,
    lit,
)
from core.logical_plan import Filter, LogicalExpr, LogicalPlan, Projection

if TYPE_CHECKING:
    from core.context import ExecutionContext

_BINARY_OPS: dict[type[exp.Expression], type] = {
    exp.And: And,
    exp.Or: Or,
    exp.EQ: Eq,
    exp.NEQ: Neq,
    exp.GT: Gt,
    exp.GTE: GtEq,
    exp.LT: Lt,
    exp.LTE: LtEq,
    exp.Add: Add,
    exp.Sub: Subtract,
    exp.Mul: Multiply,
    exp.Div: Divide,
}

_UNSUPPORTED_CLAUSES: tuple[str, ...] = (
    "with_",
    "joins",
    "group",
    "having",
    "order",
    "limit",
    "offset",
    "windows",
    "qualify",
    "distinct",
)

_CLAUSE_DISPLAY_NAMES: dict[str, str] = {
    "with_": "WITH (CTE)",
    "joins": "JOIN",
}


class SqlPlanningError(ValueError):
    """Raised when a SQL query uses a construct the frontend can't translate."""


class SqlPlanner:
    """
    Translates SQL text into a `LogicalPlan`, resolving table names in FROM
    against tables registered on the given `ExecutionContext`.
    """

    def __init__(self, ctx: "ExecutionContext") -> None:
        self._ctx = ctx

    def plan(self, sql: str, verbose: bool = False) -> LogicalPlan:
        tree: exp.Expression = sqlglot.parse_one(sql)
        if not isinstance(tree, exp.Select):
            raise SqlPlanningError(
                f"Only SELECT statements are supported, got: {tree.sql()}"
            )
        if verbose:
            print(repr(tree))
        return self._plan_select(tree)

    def _plan_select(self, select: exp.Select) -> LogicalPlan:
        for clause in _UNSUPPORTED_CLAUSES:
            if select.args.get(clause):
                name = _CLAUSE_DISPLAY_NAMES.get(clause, clause.upper())
                raise SqlPlanningError(f"Unsupported SQL clause: {name}")

        plan: LogicalPlan = self._plan_from(select)

        where: exp.Where | None = select.args.get("where")
        if where is not None:
            plan = Filter(plan, self._expr(where.this))

        return self._plan_projection(select.expressions, plan)

    def _plan_from(self, select: exp.Select) -> LogicalPlan:
        from_clause: exp.From | None = select.args.get("from_")
        if from_clause is None:
            raise SqlPlanningError("SELECT without FROM is not supported")

        table_expr: exp.Expression = from_clause.this
        if not isinstance(table_expr, exp.Table):
            raise SqlPlanningError(f"Unsupported FROM source: {table_expr.sql()}")

        table_name: str = table_expr.name
        registered: LogicalPlan | None = self._ctx.get_table(table_name)
        if registered is None:
            raise SqlPlanningError(
                f"Unknown table '{table_name}'; register it first with "
                f"ctx.register_table('{table_name}', ...)"
            )

        return registered

    def _plan_projection(
        self, expressions: list[exp.Expression], input_plan: LogicalPlan
    ) -> LogicalPlan:
        if len(expressions) == 1 and isinstance(expressions[0], exp.Star):
            return input_plan

        exprs: list[LogicalExpr] = [self._select_expr(e) for e in expressions]
        return Projection(input_plan, exprs)

    def _select_expr(self, node: exp.Expression) -> LogicalExpr:
        if isinstance(node, exp.Alias):
            return Alias(self._expr(node.this), node.alias)
        return self._expr(node)

    def _expr(self, node: exp.Expression) -> LogicalExpr:
        if isinstance(node, exp.Paren):
            return self._expr(node.this)

        if isinstance(node, exp.Column):
            return Column(node.name)

        if isinstance(node, exp.Boolean):
            return lit(bool(node.this))

        if isinstance(node, exp.Literal):
            if node.is_string:
                return lit(node.this)
            return lit(float(node.this) if "." in node.this else int(node.this))

        if isinstance(node, exp.Neg):
            inner: exp.Expression = node.this
            if isinstance(inner, exp.Literal) and not inner.is_string:
                value: float | int = (
                    float(inner.this) if "." in inner.this else int(inner.this)
                )
                return lit(-value)
            return Subtract(lit(0), self._expr(inner))

        ctor = _BINARY_OPS.get(type(node))
        if ctor is not None:
            return ctor(self._expr(node.this), self._expr(node.expression))

        raise SqlPlanningError(
            f"Unsupported SQL expression: {node.sql()!r} ({type(node).__name__})"
        )
