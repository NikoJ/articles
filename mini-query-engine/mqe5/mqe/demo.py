import pyarrow as pa

from core.datasources import DataSource
from core.logical_expr import col
from core.logical_plan import Filter, Projection, Scan
from core.tables import SchemaField, TableSchema

# --- Minimal DataSource stub (for planning only) ----------------------------

class FakeDataSource(DataSource):
    def __init__(self, schema: TableSchema):
        self._schema = schema

    def schema(self) -> TableSchema:
        return self._schema

    def scan(self, projection: list[str]):
        raise NotImplementedError("This demo builds logical plans only")

# -------------------------------

def main() -> None:
    # 1) Define schema
    schema: TableSchema = TableSchema(
        fields=[
            SchemaField("id", pa.int64()),
            SchemaField("first_name", pa.string()),
            SchemaField("state", pa.string()),
        ]
    )

    # 2) Create a fake source
    ds: DataSource = FakeDataSource(schema)

    # 3) Build logical plan:
    # SELECT id * 2 AS new_id, first_name
    # FROM employee.csv
    # WHERE first_name = 'Niko'
    scan: Scan = Scan(source_uri="employee.csv", data_source=ds)

    plan: Projection = Projection(
        input=Filter(
            input=scan,
            expr=(col("first_name") == "Niko"),
        ),
        exprs=[(col("id") * 2).as_("new_id"), col("first_name")],
    )

    # 4) Print verbose explain (with schema at each node)
    print(plan.explain())

    # 5) Print the plan (without schema at each node)
    print()
    print(plan.explain(verbose=False))


if __name__ == "__main__":
    main()
