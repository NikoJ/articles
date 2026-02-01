# [MQE6: From DataFrame API to Query Execution](https://nikoondata.substack.com/)

This repo contains a small educational prototype of a **mini query engine in Python**,
built on top of **Apache Arrow**.

MQE6 focuses on connecting the DataFrame API, logical plan, and physical execution layers.
We introduce a minimal lazy DataFrame interface and compile queries end-to-end:

`DataFrame API → Logical Plan → Physical Plan → Execution`

In this part we implement:
- ExecutionContext — single entry point for building and executing queries
- LazyFrame / DataFrame API (select, filter, collect)
- Logical → Physical compilation via the planner
- Expression binding (column name → index)
- Explain support for both logical and physical plans
- End-to-end execution on Arrow batches
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
