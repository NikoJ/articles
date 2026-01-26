import pyarrow as pa

from core.datasources import InMemoryDataSource
from core.datatypes import ArrowColumn
from core.physical_expr import ColumnExpression, EqExpression, lit
from core.physical_plan import FilterExec, ProjectionExec, ScanExec
from core.tables import DataBatch, SchemaField, TableSchema


def main() -> None:
    # 1) Build a single in-memory DataBatch
    schema: TableSchema = TableSchema(
        [
            SchemaField("id", pa.int64()),
            SchemaField("first_name", pa.string()),
            SchemaField("state", pa.string()),
        ]
    )

    data: list[ArrowColumn] = [
        ArrowColumn(pa.array([1, 2, 3])),
        ArrowColumn(pa.array(["Niko", "Alice", "Joy"])),
        ArrowColumn(pa.array(["CO", "CA", "NY"])),
    ]

    batch: DataBatch = DataBatch(schema=schema, fields=data)

    # 2) Create InMemoryDataSource with auto-schema inference
    ds: InMemoryDataSource = InMemoryDataSource(data=[batch])

    # 3) Build Physical Plan
    #
    # SQL-ish:
    # SELECT id * 2 AS new_id, first_name
    # FROM in_memory
    # WHERE first_name = 'Niko'

    scan: ScanExec = ScanExec(data_source=ds, projection=[])  # [] means "read all"

    predicate: EqExpression = EqExpression(
        ColumnExpression(1),  # first_name
        lit("Niko"),
    )
    filter_: FilterExec = FilterExec(input=scan, predicate=predicate)

    out_schema: TableSchema = TableSchema(
        fields=[
            SchemaField("new_id", pa.int64()),
            SchemaField("first_name", pa.string()),
        ]
    )
    proj: ProjectionExec = ProjectionExec(
        input=filter_,
        exprs=[(ColumnExpression(0) * 2).alias("new_id"), ColumnExpression(1)],
        _schema=out_schema,
    )

    # 4) Print EXPLAIN
    print("=== PHYSICAL PLAN ===")
    print(proj.explain(verbose=True))

    # 5) Execute and print output
    print("\n=== OUTPUT ===")
    print(next(proj.execute()))


if __name__ == "__main__":
    main()
