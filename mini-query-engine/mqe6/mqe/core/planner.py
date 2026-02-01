from dataclasses import dataclass

from core.logical_expr import (
    Add,
    Alias,
    And,
    BinaryExpr,
    CastExpr,
    Column,
    ColumnIndex,
    Divide,
    Eq,
    Gt,
    GtEq,
    LiteralDouble,
    LiteralLong,
    LiteralString,
    LogicalExpr,
    Lt,
    LtEq,
    Multiply,
    Neq,
    Or,
    Subtract,
)
from core.logical_plan import Filter, LogicalPlan, Projection, Scan
from core.physical_expr import (
    AddExpression,
    AndExpression,
    ColumnExpression,
    DivideExpression,
    EqExpression,
    GtEqExpression,
    GtExpression,
    LtEqExpression,
    LtExpression,
    MultiplyExpression,
    NeqExpression,
    OrExpression,
    PhysicalExprNode,
    SubtractExpression,
    lit,
)
from core.physical_plan import FilterExec, PhysicalPlan, ProjectionExec, ScanExec
from core.tables import TableSchema


@dataclass
class Planner:
    def create_physical_plan(self, plan: LogicalPlan) -> PhysicalPlan:
        if isinstance(plan, Scan):
            return ScanExec(
                data_source=plan.data_source,
                projection=plan.projection or [],
            )

        if isinstance(plan, Filter):
            input_plan: PhysicalPlan = self.create_physical_plan(plan.input)
            predicate: PhysicalExprNode = self.create_physical_expr(
                plan.expr, input_schema=plan.input.schema()
            )
            return FilterExec(input=input_plan, predicate=predicate)

        if isinstance(plan, Projection):
            input_plan = self.create_physical_plan(plan.input)
            exprs: list[PhysicalExprNode] = [
                self.create_physical_expr(expr, input_schema=plan.input.schema())
                for expr in plan.exprs
            ]

            # use already computed logical schema
            out_schema: TableSchema = plan.schema()

            return ProjectionExec(input=input_plan, exprs=exprs, _schema=out_schema)

        raise TypeError(f"Unsupported logical plan: {type(plan).__name__}")

    def create_physical_expr(self, expr: LogicalExpr, input_schema) -> PhysicalExprNode:
        """
        Build a physical expression bound to the given input schema.
        input_schema: TableSchema
        """

        # Fast path: literals
        if isinstance(expr, LiteralLong):
            return lit(expr.n)

        if isinstance(expr, LiteralDouble):
            return lit(expr.n)

        if isinstance(expr, LiteralString):
            return lit(expr.s)

        # Column ref by index
        if isinstance(expr, ColumnIndex):
            return ColumnExpression(expr.i)

        # Alias does not exist physically, schema already contains the name
        if isinstance(expr, Alias):
            return self.create_physical_expr(expr.expr, input_schema)

        # Column ref by name -> index binding
        if isinstance(expr, Column):
            idx = self._resolve_column_index(expr.name, input_schema)
            return ColumnExpression(idx)

        # Cast
        if isinstance(expr, CastExpr):
            return PhysicalExprNode.cast(
                self.create_physical_expr(expr.expr, input_schema), expr.data_type
            )

        # Binary expressions
        if isinstance(expr, BinaryExpr):
            le: PhysicalExprNode = self.create_physical_expr(expr.le, input_schema)
            re: PhysicalExprNode = self.create_physical_expr(expr.re, input_schema)

            if isinstance(expr, Eq):
                return EqExpression(le, re)
            if isinstance(expr, Neq):
                return NeqExpression(le, re)
            if isinstance(expr, Gt):
                return GtExpression(le, re)
            if isinstance(expr, GtEq):
                return GtEqExpression(le, re)
            if isinstance(expr, Lt):
                return LtExpression(le, re)
            if isinstance(expr, LtEq):
                return LtEqExpression(le, re)
            if isinstance(expr, And):
                return AndExpression(le, re)
            if isinstance(expr, Or):
                return OrExpression(le, re)

            if isinstance(expr, Add):
                return AddExpression(le, re)
            if isinstance(expr, Subtract):
                return SubtractExpression(le, re)
            if isinstance(expr, Multiply):
                return MultiplyExpression(le, re)
            if isinstance(expr, Divide):
                return DivideExpression(le, re)

            raise TypeError(f"Unsupported binary expression: {type(expr).__name__}")

        raise TypeError(f"Unsupported logical expression: {type(expr).__name__}")

    def _resolve_column_index(self, name: str, input_schema: TableSchema) -> int:
        """
        Resolve column name -> index.
        """
        for i, f in enumerate(input_schema.fields):
            if f.name == name:
                return i
        raise ValueError(f"No column named '{name}' in input schema")
