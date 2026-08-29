from abc import ABC, abstractmethod
from collections.abc import Sequence

from core.logical_expr import (
    Alias,
    BinaryExpr,
    CastExpr,
    Column,
    LogicalExpr,
    ScalarFunction,
    UnaryExpr,
)
from core.logical_plan import Filter, LogicalPlan, Projection, Scan


class OptimizerRule(ABC):
    """
    Represents a single optimization rule for a logical plan.

    A rule takes a `LogicalPlan`, applies a specific rewrite,
    and returns an equivalent plan that is expected to be more efficient.

    `LogicalPlan` nodes are immutable, so rules should create new nodes
    instead of modifying existing ones.

    To add a new optimization, implement a new `OptimizerRule` subclass
    and register it in `Optimizer.DEFAULT_RULES`,
    or pass it directly to `Optimizer(rules=[...])`.
    """

    @abstractmethod
    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        """
        Rewrite `logical plan` into an equivalent plan and return the new tree.
        """
        ...


def _referenced_columns(expr: LogicalExpr) -> set[str]:
    """
    Recursively collect the names of all columns referenced by a logical
    tree. Used to figure out which source columns a Filter
    predicate or a Projection expression actually depends on.
    """
    if isinstance(expr, Column):
        return {expr.name}

    if isinstance(expr, (UnaryExpr, CastExpr, Alias)):
        return _referenced_columns(expr.expr)

    if isinstance(expr, BinaryExpr):
        return _referenced_columns(expr.le) | _referenced_columns(expr.re)

    if isinstance(expr, ScalarFunction):
        columns: set[str] = set()
        for arg in expr.args:
            columns |= _referenced_columns(arg)
        return columns

    # Literals, ColumnIndex, etc. don't reference any named column.
    return set()


class ProjectionPushDown(OptimizerRule):
    """
    Push column requirements down to the Scan, so the DataSource only reads
    the columns actually needed further up the plan.
    """

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        return self._push_down(plan, required=set())

    def _push_down(self, plan: LogicalPlan, required: set[str]) -> LogicalPlan:
        if isinstance(plan, Scan):
            return self._prune_scan(plan, required)

        if isinstance(plan, Filter):
            child_required = required | _referenced_columns(plan.expr)
            return Filter(self._push_down(plan.input, child_required), plan.expr)

        if isinstance(plan, Projection):
            expr_required: set[str] = set()
            for expr in plan.exprs:
                expr_required |= _referenced_columns(expr)
            return Projection(self._push_down(plan.input, expr_required), plan.exprs)

        return plan

    def _prune_scan(self, scan: Scan, required: set[str]) -> Scan:
        if not required:
            return scan

        source_order = [f.name for f in scan.data_source.schema().fields]
        pruned = [name for name in source_order if name in required]

        return Scan(
            source_uri=scan.source_uri,
            data_source=scan.data_source,
            projection=pruned,
        )


class PredicatePushDown(OptimizerRule):
    """
    Pushes Filter nodes closer to the data source when possible.

    Filter can move below Projection nodes only if all referenced columns
    pass through unchanged. Computed expressions and aliases prevent pushdown.

    Filtering earlier can reduce the amount of work done by later operations.
    """

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        if isinstance(plan, Scan):
            return plan

        if isinstance(plan, Projection):
            return Projection(self.optimize(plan.input), plan.exprs)

        if isinstance(plan, Filter):
            # Optimize the subtree first so any Filters nested further down
            # are already pushed as far as they can go.
            new_input = self.optimize(plan.input)
            return self._push_past_projections(Filter(new_input, plan.expr))

        return plan

    def _push_past_projections(self, filter_node: Filter) -> LogicalPlan:
        referenced = _referenced_columns(filter_node.expr)
        predicate = filter_node.expr

        skipped: list[Projection] = []
        current: LogicalPlan = filter_node.input

        while isinstance(current, Projection):
            passthrough = {e.name for e in current.exprs if isinstance(e, Column)}
            if not referenced.issubset(passthrough):
                break
            skipped.append(current)
            current = current.input

        if not skipped:
            return filter_node

        result: LogicalPlan = Filter(current, predicate)
        for projection in reversed(skipped):
            result = Projection(result, projection.exprs)
        return result


class Optimizer:
    """
    Rewrites a logical plan into an equivalent, cheaper-to-execute plan by
    running it through a fixed pipeline of OptimizerRules, one after another.
    """

    DEFAULT_RULES: Sequence[OptimizerRule] = (
        PredicatePushDown(),
        ProjectionPushDown(),
    )

    def __init__(self, rules: Sequence[OptimizerRule] | None = None) -> None:
        self.rules: Sequence[OptimizerRule] = (
            rules if rules is not None else self.DEFAULT_RULES
        )

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        for rule in self.rules:
            plan = rule.optimize(plan)
        return plan
