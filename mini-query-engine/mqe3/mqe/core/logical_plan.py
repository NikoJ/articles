from core.tables import SchemaField, TableSchema


class LogicalPlan:
    """
    TODO MQE4
    """

    def schema(self) -> TableSchema:
        """
        TODO MQE4
        """
        raise NotImplementedError


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
