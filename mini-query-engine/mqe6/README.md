# [MQE6: From DataFrame API to Query Execution](https://nikoondata.substack.com/)

This repo contains a small educational prototype of a **mini query engine in Python**,
built on top of **Apache Arrow**.

MQE6 focuses on connecting the DataFrame API, logical plan, and physical execution layers.
We introduce a minimal lazy DataFrame interface and compile queries end-to-end:

```
DataFrame API → Logical Plan → Physical Plan → Execution
```

In this part we implement:
- ExecutionContext — single entry point for building and executing queries
- LazyFrame / DataFrame API (select, filter, collect)
- Logical → Physical compilation via the planner
- Expression binding (column name → index)
- Explain support for both logical and physical plans
- End-to-end execution on Arrow batches

Example:
```python
    lf: mqe.LazyFrame = (
        mqe.from_dict(
            {
                "id": [1, 2, 3],
                "first_name": ["Niko", "Alice", "Joy"],
                "state": ["CO", "CA", "NY"],
            }
        )
        .filter(col("first_name") == "Niko")
        .select("id", (col("id") * 2).alias("new_id"), "first_name")
    )

    lf.explain(verbose=True)

    result: mqe.DataFrame = lf.collect()
```
Result:
```
===== LOGICAL PLAN =====

Projection: #id, (#id * 2) AS new_id, #first_name  [id:int64, new_id:int64, first_name:string]
└── Filter: (#first_name = 'Niko')  [id:int64, first_name:string, state:string]
    └── Scan: in_memory; projection=None  [id:int64, first_name:string, state:string]

===== PHYSICAL PLAN =====

ProjectionExec: #0, (#0 * 2), #1  [id:int64, new_id:int64, first_name:string]
    └── FilterExec: ((#1 = 'Niko'))  [id:int64, first_name:string, state:string]
        └── ScanExec: projection=None, source=InMemoryDataSource)  [id:int64, first_name:string, state:string]

===== EXAMPLE =====

DataFrame Summary
Rows:    1
Columns: 3
Batches: 1
Schema:  id:int64, new_id:int64, first_name:string
===================================================
[Batch 0]
Rows:    1
Columns: 3
Data:
--------------------------
id      new_id  first_name
--------------------------
1       2       Niko
```
---

## 📁 Project Structure

    mqe/
    ├── demo.py
    ├── docker-compose.yml
    ├── Dockerfile
    ├── pyproject.toml
    ├── uv.lock
    ├── ...
    └── core/
        ├── datatypes.py        # ColumnData: ArrowColumn / LiteralColumn
        ├── tables.py           # SchemaField/TableSchema/DataBatch
        ├── logical_plan.py     # Logical plans (Scan/Filter/Projection) + explain()
        ├── logical_expr.py     # Expression DSL (logical layer)
        ├── datasources.py      # Data sources (e.g., InMemoryDataSource)
        ├── physical_plan.py    # Physical operators + explain(): ScanExec/FilterExec/ProjectionExec
        ├── planner.py          # Logical → Physical compilation + binding
        ├── frames.py           # LazyFrame/DataFrame user API
        └── context.py          # ExecutionContext (entry point)
---

## 🚀 Getting Started

You can run the demo either **inside Docker (recommended)** or **locally**.

### Run with Docker + uv (recommended)

> Requires Docker installed on your machine.

```bash
git clone https://github.com/NikoJ/articles.git
cd mqe6/mqe

docker compose up -d

docker compose exec mqe6 uv run demo.py
```
