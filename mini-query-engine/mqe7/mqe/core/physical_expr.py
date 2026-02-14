from dataclasses import dataclass
from typing import Any, Callable, Optional, Union

import pyarrow as pa
import pyarrow.compute as pc

from core.datatypes import ArrowColumn, ColumnData, LiteralColumn
from core.physical_plan import PhysicalExpr
from core.tables import DataBatch

# -----------------------------------------------------------------------------
# Public typing
# -----------------------------------------------------------------------------

PhysicalExprLike = Union["PhysicalExprNode", int, float, str, bool]


# -----------------------------------------------------------------------------
# Base node with operator overloading
# -----------------------------------------------------------------------------


class PhysicalExprNode(PhysicalExpr):
    """
    Base class for physical expressions.

    Defines operator overloads so we can build physical expr trees directly:
      - arithmetic: + - * /
      - comparisons: == != < <= > >=
      - boolean logic: & | ~
    """

    def evaluate(self, input: DataBatch) -> ColumnData:
        raise NotImplementedError

    # --------------------
    # Convenience methods
    # --------------------

    def alias(self, name: str) -> "PhysicalExprNode":
        return AliasExpression(self, name)

    def cast(self, target_type: pa.DataType) -> "PhysicalExprNode":
        return CastExpression(self, target_type)

    # --------------------
    # Arithmetic
    # --------------------

    def __add__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return AddExpression(self, to_phys_expr(other))

    def __radd__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return AddExpression(to_phys_expr(other), self)

    def __sub__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return SubtractExpression(self, to_phys_expr(other))

    def __rsub__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return SubtractExpression(to_phys_expr(other), self)

    def __mul__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return MultiplyExpression(self, to_phys_expr(other))

    def __rmul__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return MultiplyExpression(to_phys_expr(other), self)

    def __truediv__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return DivideExpression(self, to_phys_expr(other))

    def __rtruediv__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return DivideExpression(to_phys_expr(other), self)

    # --------------------
    # Comparisons
    # --------------------

    def __eq__(self, other: object) -> "PhysicalExprNode":  # type: ignore[override]
        return EqExpression(self, to_phys_expr(other))  # type: ignore[arg-type]

    def __ne__(self, other: object) -> "PhysicalExprNode":  # type: ignore[override]
        return NeqExpression(self, to_phys_expr(other))  # type: ignore[arg-type]

    def __lt__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return LtExpression(self, to_phys_expr(other))

    def __le__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return LtEqExpression(self, to_phys_expr(other))

    def __gt__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return GtExpression(self, to_phys_expr(other))

    def __ge__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return GtEqExpression(self, to_phys_expr(other))

    # --------------------
    # Boolean logic
    # --------------------
    # Python keywords (and/or/not) can't be overloaded.
    # We use &, |, ~ like Polars/Spark.

    def __and__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return AndExpression(self, to_phys_expr(other))

    def __rand__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return AndExpression(to_phys_expr(other), self)

    def __or__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return OrExpression(self, to_phys_expr(other))

    def __ror__(self, other: PhysicalExprLike) -> "PhysicalExprNode":
        return OrExpression(to_phys_expr(other), self)

    def __invert__(self) -> "PhysicalExprNode":
        return NotExpression(self)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def to_phys_expr(value: PhysicalExprLike) -> PhysicalExprNode:
    if isinstance(value, PhysicalExprNode):
        return value
    return LiteralExpression(value)


def _infer_type(value: Any) -> pa.DataType:
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, float):
        return pa.float64()
    if isinstance(value, str):
        return pa.string()
    raise TypeError(f"Cannot infer Arrow type for literal: {type(value)}")


def _as_scalar(col: LiteralColumn) -> pa.Scalar:
    return pa.scalar(col.value, type=col.data_type)


def _wrap_arrow_result(out: Any, result_type: Optional[pa.DataType]) -> ArrowColumn:
    if isinstance(out, pa.ChunkedArray):
        out = out.combine_chunks()

    if isinstance(out, pa.Scalar):
        out = pa.array([out.as_py()], type=result_type)

    if not isinstance(out, pa.Array):
        raise TypeError(f"Unsupported Arrow compute result: {type(out)}")

    if result_type is not None and out.type != result_type:
        out = pc.cast(out, result_type)

    return ArrowColumn(out)


def _binary_compute(
    left: ColumnData,
    right: ColumnData,
    fn: Callable[[Any, Any], Any],
    python_fallback: Optional[Callable[[Any, Any], Any]],
    result_type: Optional[pa.DataType] = None,
) -> ColumnData:
    """
    Arrow-first binary compute.

    Cases:
      - Literal vs Literal: compute once -> LiteralColumn
      - Arrow vs Literal: vectorized Arrow kernel (array, scalar)
      - Literal vs Arrow: vectorized Arrow kernel (scalar, array)
      - Arrow vs Arrow: vectorized Arrow kernel (array, array)
    Fallback (slow path) is used only when Arrow kernel can't run.
    """
    if left.get_size() != right.get_size():
        raise ValueError(
            f"Column sizes must match: { left.get_size() } != { right.get_size() }"
        )

    # ---- literal vs literal (compute once)
    if isinstance(left, LiteralColumn) and isinstance(right, LiteralColumn):
        if python_fallback is None:
            raise ValueError(
                "python_fallback is required for literal-vs-literal evaluation"
            )
        value = python_fallback(left.value, right.value)
        dtype = result_type or _infer_type(value)
        return LiteralColumn(dtype, value, left.size)

    # ---- Arrow vs Arrow
    if isinstance(left, ArrowColumn) and isinstance(right, ArrowColumn):
        try:
            return _wrap_arrow_result(fn(left.array, right.array), result_type)
        except Exception:
            if python_fallback is None:
                raise
            values = [
                python_fallback(a, b)
                for a, b in zip(left.array.to_pylist(), right.array.to_pylist())
            ]
            return ArrowColumn(pa.array(values, type=result_type))

    # ---- Arrow vs Literal
    if isinstance(left, ArrowColumn) and isinstance(right, LiteralColumn):
        try:
            return _wrap_arrow_result(fn(left.array, _as_scalar(right)), result_type)
        except Exception:
            if python_fallback is None:
                raise
            values = [python_fallback(a, right.value) for a in left.array.to_pylist()]
            return ArrowColumn(pa.array(values, type=result_type))

    # ---- Literal vs Arrow
    if isinstance(left, LiteralColumn) and isinstance(right, ArrowColumn):
        try:
            return _wrap_arrow_result(fn(_as_scalar(left), right.array), result_type)
        except Exception:
            if python_fallback is None:
                raise
            values = [python_fallback(left.value, b) for b in right.array.to_pylist()]
            return ArrowColumn(pa.array(values, type=result_type))

    raise TypeError(f"Unsupported ColumnData operands: {type(left)} vs {type(right)}")


# -----------------------------------------------------------------------------
# Leaf expressions
# -----------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class ColumnExpression(PhysicalExprNode):
    """
    Reference a column in the input batch by index.
    """

    index: int

    def evaluate(self, input: DataBatch) -> ColumnData:
        return input.field(self.index)

    def __str__(self) -> str:
        return f"#{self.index}"


@dataclass(frozen=True, eq=False)
class LiteralExpression(PhysicalExprNode):
    """
    A literal value broadcasted to the input batch length.
    """

    value: Any
    data_type: Optional[pa.DataType] = None

    def evaluate(self, input: DataBatch) -> ColumnData:
        dtype = self.data_type or _infer_type(self.value)
        return LiteralColumn(dtype, self.value, input.row_count())

    def __str__(self) -> str:
        return f"'{self.value}'" if isinstance(self.value, str) else str(self.value)


def lit(value: Any, data_type: Optional[pa.DataType] = None) -> LiteralExpression:
    return LiteralExpression(value=value, data_type=data_type)


# -----------------------------------------------------------------------------
# Boolean operators
# -----------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class AndExpression(PhysicalExprNode):
    left: PhysicalExprNode
    right: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        return _binary_compute(
            self.left.evaluate(input),
            self.right.evaluate(input),
            fn=pc.and_,
            python_fallback=lambda a, b: bool(a) and bool(b),
            result_type=pa.bool_(),
        )

    def __str__(self) -> str:
        return f"({self.left} AND {self.right})"


@dataclass(frozen=True, eq=False)
class OrExpression(PhysicalExprNode):
    left: PhysicalExprNode
    right: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        return _binary_compute(
            self.left.evaluate(input),
            self.right.evaluate(input),
            fn=pc.or_,
            python_fallback=lambda a, b: bool(a) or bool(b),
            result_type=pa.bool_(),
        )

    def __str__(self) -> str:
        return f"({self.left} OR {self.right})"


@dataclass(frozen=True, eq=False)
class NotExpression(PhysicalExprNode):
    expr: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        col = self.expr.evaluate(input)
        if isinstance(col, ArrowColumn):
            out = pc.invert(col.array)
            return _wrap_arrow_result(out, pa.bool_())
        if isinstance(col, LiteralColumn):
            return LiteralColumn(pa.bool_(), not bool(col.value), col.size)
        raise TypeError(f"Unsupported ColumnData for NOT: {type(col)}")

    def __str__(self) -> str:
        return f"(NOT {self.expr})"


# -----------------------------------------------------------------------------
# Comparisons
# -----------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class EqExpression(PhysicalExprNode):
    left: PhysicalExprNode
    right: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        return _binary_compute(
            self.left.evaluate(input),
            self.right.evaluate(input),
            fn=pc.equal,
            python_fallback=lambda a, b: a == b,
            result_type=pa.bool_(),
        )

    def __str__(self) -> str:
        return f"({self.left} = {self.right})"


@dataclass(frozen=True, eq=False)
class NeqExpression(PhysicalExprNode):
    left: PhysicalExprNode
    right: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        return _binary_compute(
            self.left.evaluate(input),
            self.right.evaluate(input),
            fn=pc.not_equal,
            python_fallback=lambda a, b: a != b,
            result_type=pa.bool_(),
        )

    def __str__(self) -> str:
        return f"({self.left} != {self.right})"


@dataclass(frozen=True, eq=False)
class LtExpression(PhysicalExprNode):
    left: PhysicalExprNode
    right: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        return _binary_compute(
            self.left.evaluate(input),
            self.right.evaluate(input),
            fn=pc.less,
            python_fallback=lambda a, b: a < b,
            result_type=pa.bool_(),
        )

    def __str__(self) -> str:
        return f"({self.left} < {self.right})"


@dataclass(frozen=True, eq=False)
class LtEqExpression(PhysicalExprNode):
    left: PhysicalExprNode
    right: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        return _binary_compute(
            self.left.evaluate(input),
            self.right.evaluate(input),
            fn=pc.less_equal,
            python_fallback=lambda a, b: a <= b,
            result_type=pa.bool_(),
        )

    def __str__(self) -> str:
        return f"({self.left} <= {self.right})"


@dataclass(frozen=True, eq=False)
class GtExpression(PhysicalExprNode):
    left: PhysicalExprNode
    right: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        return _binary_compute(
            self.left.evaluate(input),
            self.right.evaluate(input),
            fn=pc.greater,
            python_fallback=lambda a, b: a > b,
            result_type=pa.bool_(),
        )

    def __str__(self) -> str:
        return f"({self.left} > {self.right})"


@dataclass(frozen=True, eq=False)
class GtEqExpression(PhysicalExprNode):
    left: PhysicalExprNode
    right: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        return _binary_compute(
            self.left.evaluate(input),
            self.right.evaluate(input),
            fn=pc.greater_equal,
            python_fallback=lambda a, b: a >= b,
            result_type=pa.bool_(),
        )

    def __str__(self) -> str:
        return f"({self.left} >= {self.right})"


# -----------------------------------------------------------------------------
# Arithmetic
# -----------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class AddExpression(PhysicalExprNode):
    left: PhysicalExprNode
    right: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        return _binary_compute(
            self.left.evaluate(input),
            self.right.evaluate(input),
            fn=pc.add,
            python_fallback=lambda a, b: a + b,
        )

    def __str__(self) -> str:
        return f"({self.left} + {self.right})"


@dataclass(frozen=True, eq=False)
class SubtractExpression(PhysicalExprNode):
    left: PhysicalExprNode
    right: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        return _binary_compute(
            self.left.evaluate(input),
            self.right.evaluate(input),
            fn=pc.subtract,
            python_fallback=lambda a, b: a - b,
        )

    def __str__(self) -> str:
        return f"({self.left} - {self.right})"


@dataclass(frozen=True, eq=False)
class MultiplyExpression(PhysicalExprNode):
    left: PhysicalExprNode
    right: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        return _binary_compute(
            self.left.evaluate(input),
            self.right.evaluate(input),
            fn=pc.multiply,
            python_fallback=lambda a, b: a * b,
        )

    def __str__(self) -> str:
        return f"({self.left} * {self.right})"


@dataclass(frozen=True, eq=False)
class DivideExpression(PhysicalExprNode):
    left: PhysicalExprNode
    right: PhysicalExprNode

    def evaluate(self, input: DataBatch) -> ColumnData:
        return _binary_compute(
            self.left.evaluate(input),
            self.right.evaluate(input),
            fn=pc.divide,
            python_fallback=lambda a, b: a / b,
        )

    def __str__(self) -> str:
        return f"({self.left} / {self.right})"


# -----------------------------------------------------------------------------
# Cast / Alias
# -----------------------------------------------------------------------------


@dataclass(frozen=True, eq=False)
class CastExpression(PhysicalExprNode):
    """
    Cast a physical expression result to a target Arrow data type.
    """

    expr: PhysicalExprNode
    target_type: pa.DataType

    def evaluate(self, input: DataBatch) -> ColumnData:
        col = self.expr.evaluate(input)

        if isinstance(col, ArrowColumn):
            out = pc.cast(col.array, self.target_type)
            return _wrap_arrow_result(out, self.target_type)

        if isinstance(col, LiteralColumn):
            scalar = pa.scalar(col.value, type=col.data_type)
            casted = pc.cast(scalar, self.target_type)
            value = casted.as_py() if isinstance(casted, pa.Scalar) else casted
            return LiteralColumn(self.target_type, value, col.size)

        raise TypeError(f"Unsupported ColumnData type for cast: {type(col)}")

    def __str__(self) -> str:
        return f"CAST({self.expr} AS {self.target_type})"


@dataclass(frozen=True, eq=False)
class AliasExpression(PhysicalExprNode):
    """
    Alias wrapper for explain/debug.

    Does not change values, only preserves a readable name.
    Final output naming is controlled by ProjectionExec schema.
    """

    expr: PhysicalExprNode
    name: str

    def evaluate(self, input: DataBatch) -> ColumnData:
        return self.expr.evaluate(input)

    def __str__(self) -> str:
        return f"{self.expr} AS {self.name}"
