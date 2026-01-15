from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from core.datatypes import BOOLEAN, FLOAT32, FLOAT64, INT64, STRING
from core.logical_plan import LogicalExpr, LogicalPlan
from core.tables import SchemaField

# -----------------------------
# Helpers
# -----------------------------


def _ensure_expr(value: Any) -> LogicalExpr:
    if isinstance(value, LogicalExpr):
        return value
    return lit(value)


def _expr_name(expr: LogicalExpr) -> str:
    return str(expr)


def lit(value: Any) -> LogicalExpr:
    match value:
        case bool() as _val:
            return LiteralBoolean(_val)
        case int() as _val:
            return LiteralLong(_val)
        case float() as _val:
            return LiteralDouble(_val)
        case str() as _val:
            return LiteralString(_val)
        case _:
            raise TypeError(f"Unsupported literal type: {type(value)}")


def col(name: str) -> "Column":
    return Column(name)


def cast(expr: LogicalExpr, data_type: pa.DataType) -> "CastExpr":
    return CastExpr(_ensure_expr(expr), data_type)


def alias(expr: LogicalExpr, name: str) -> "Alias":
    return Alias(_ensure_expr(expr), name)


# -----------------------------
# Base node that provides operator sugar
# (keeps LogicalExpr interface minimal)
# -----------------------------


class LogicalExprNode(LogicalExpr):
    """
    Base class for expression AST nodes that adds operator overloads.

    LogicalExpr stays minimal (interface), LogicalExprNode provides syntactic sugar
    for the DSL.
    """

    # Boolean logic
    def __and__(self, other: Any) -> "And":
        return And(self, _ensure_expr(other))

    def __or__(self, other: Any) -> "Or":
        return Or(self, _ensure_expr(other))

    def __invert__(self) -> "Not":
        # Use ~expr as NOT (common Python DSL trick)
        return Not(self)

    # Comparisons
    def __eq__(self, other: Any) -> "Eq":  # type: ignore[override]
        return Eq(self, _ensure_expr(other))

    def __ne__(self, other: Any) -> "Neq":  # type: ignore[override]
        return Neq(self, _ensure_expr(other))

    def __gt__(self, other: Any) -> "Gt":
        return Gt(self, _ensure_expr(other))

    def __ge__(self, other: Any) -> "GtEq":
        return GtEq(self, _ensure_expr(other))

    def __lt__(self, other: Any) -> "Lt":
        return Lt(self, _ensure_expr(other))

    def __le__(self, other: Any) -> "LtEq":
        return LtEq(self, _ensure_expr(other))

    # Arithmetic
    def __add__(self, other: Any) -> "Add":
        return Add(self, _ensure_expr(other))

    def __sub__(self, other: Any) -> "Subtract":
        return Subtract(self, _ensure_expr(other))

    def __mul__(self, other: Any) -> "Multiply":
        return Multiply(self, _ensure_expr(other))

    def __truediv__(self, other: Any) -> "Divide":
        return Divide(self, _ensure_expr(other))

    def __mod__(self, other: Any) -> "Mod":
        return Mod(self, _ensure_expr(other))

    # Reverse arithmetic (e.g., 10 + col("x"))
    def __radd__(self, other: Any) -> "Add":
        return Add(_ensure_expr(other), self)

    def __rsub__(self, other: Any) -> "Subtract":
        return Subtract(_ensure_expr(other), self)

    def __rmul__(self, other: Any) -> "Multiply":
        return Multiply(_ensure_expr(other), self)

    def __rtruediv__(self, other: Any) -> "Divide":
        return Divide(_ensure_expr(other), self)

    def __rmod__(self, other: Any) -> "Mod":
        return Mod(_ensure_expr(other), self)

    # Aliasing
    def as_(self, name: str) -> "Alias":
        return Alias(self, name)


# -----------------------------
# Leaf expressions
# -----------------------------


@dataclass(frozen=True, eq=False)
class Column(LogicalExprNode):
    """
    Logical expression representing a reference to a column by name.
    """

    name: str

    def to_field(self, input: LogicalPlan) -> SchemaField:
        for field in input.schema().fields:
            if field.name == self.name:
                return field
        raise ValueError(f"No column named '{self.name}' in {input.schema().fields}")

    def __str__(self) -> str:
        return f"#{self.name}"


@dataclass(frozen=True, eq=False)
class ColumnIndex(LogicalExprNode):
    i: int

    def to_field(self, input: LogicalPlan) -> SchemaField:
        try:
            return input.schema().fields[self.i]
        except IndexError as e:
            raise ValueError(f"Column index out of range: {self.i}") from e

    def __str__(self) -> str:
        return f"#{self.i}"


@dataclass(frozen=True, eq=False)
class LiteralString(LogicalExprNode):
    s: str

    def to_field(self, input: LogicalPlan) -> SchemaField:
        return SchemaField(_expr_name(self), STRING)

    def __str__(self) -> str:
        return f"'{self.s}'"


@dataclass(frozen=True, eq=False)
class LiteralLong(LogicalExprNode):
    n: int

    def to_field(self, input: LogicalPlan) -> SchemaField:
        return SchemaField(_expr_name(self), INT64)

    def __str__(self) -> str:
        return str(self.n)


@dataclass(frozen=True, eq=False)
class LiteralFloat(LogicalExprNode):
    n: float

    def to_field(self, input: LogicalPlan) -> SchemaField:
        return SchemaField(_expr_name(self), FLOAT32)

    def __str__(self) -> str:
        return str(self.n)


@dataclass(frozen=True, eq=False)
class LiteralDouble(LogicalExprNode):
    n: float

    def to_field(self, input: LogicalPlan) -> SchemaField:
        return SchemaField(_expr_name(self), FLOAT64)

    def __str__(self) -> str:
        return str(self.n)


@dataclass(frozen=True, eq=False)
class LiteralBoolean(LogicalExprNode):
    b: bool

    def to_field(self, input: LogicalPlan) -> SchemaField:
        return SchemaField(_expr_name(self), BOOLEAN)

    def __str__(self) -> str:
        return "TRUE" if self.b else "FALSE"


# -----------------------------
# Cast, Alias, Functions
# -----------------------------


@dataclass(frozen=True, eq=False)
class CastExpr(LogicalExprNode):
    expr: LogicalExpr
    data_type: pa.DataType

    def to_field(self, input: LogicalPlan) -> SchemaField:
        f = self.expr.to_field(input)
        # keep expression name readable
        return SchemaField(f.name, self.data_type)

    def __str__(self) -> str:
        return f"CAST({self.expr} AS {self.data_type})"


@dataclass(frozen=True, eq=False)
class Alias(LogicalExprNode):
    expr: LogicalExpr
    alias: str

    def to_field(self, input: LogicalPlan) -> SchemaField:
        return SchemaField(self.alias, self.expr.to_field(input).data_type)

    def __str__(self) -> str:
        return f"{self.expr} AS {self.alias}"


@dataclass(frozen=True, eq=False)
class ScalarFunction(LogicalExprNode):
    name: str
    args: tuple[LogicalExpr]
    return_type: pa.DataType

    def to_field(self, input: LogicalPlan) -> SchemaField:
        return SchemaField(_expr_name(self), self.return_type)

    def __str__(self) -> str:
        args = ", ".join(map(str, self.args))
        return f"{self.name}({args})"


# -----------------------------
# Unary / Binary expressions
# -----------------------------


@dataclass(frozen=True, eq=False)
class UnaryExpr(LogicalExprNode):
    name: str
    op: str
    expr: LogicalExpr

    def __str__(self) -> str:
        return f"{self.op}({self.expr})"


class Not(UnaryExpr):
    def __init__(self, expr: LogicalExpr):
        super().__init__("not", "NOT", expr)

    def to_field(self, input: LogicalPlan) -> SchemaField:
        return SchemaField(_expr_name(self), BOOLEAN)


@dataclass(frozen=True, eq=False)
class BinaryExpr(LogicalExprNode):
    name: str
    op: str
    le: LogicalExpr
    re: LogicalExpr

    def __str__(self) -> str:
        return f"({self.le} {self.op} {self.re})"


class BooleanBinaryExpr(BinaryExpr):
    def to_field(self, input: LogicalPlan) -> SchemaField:
        return SchemaField(_expr_name(self), BOOLEAN)


class And(BooleanBinaryExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("and", "AND", le, re)


class Or(BooleanBinaryExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("or", "OR", le, re)


class Eq(BooleanBinaryExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("eq", "=", le, re)


class Neq(BooleanBinaryExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("neq", "!=", le, re)


class Gt(BooleanBinaryExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("gt", ">", le, re)


class GtEq(BooleanBinaryExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("gteq", ">=", le, re)


class Lt(BooleanBinaryExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("lt", "<", le, re)


class LtEq(BooleanBinaryExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("lteq", "<=", le, re)


class MathExpr(BinaryExpr):
    def to_field(self, input: LogicalPlan) -> SchemaField:
        # MQE3 simplification: result type = left type
        return SchemaField(_expr_name(self), self.le.to_field(input).data_type)


class Add(MathExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("add", "+", le, re)


class Subtract(MathExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("sub", "-", le, re)


class Multiply(MathExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("mult", "*", le, re)


class Divide(MathExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("div", "/", le, re)


class Mod(MathExpr):
    def __init__(self, le: LogicalExpr, re: LogicalExpr):
        super().__init__("mod", "%", le, re)
