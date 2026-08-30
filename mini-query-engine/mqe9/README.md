# [MQE9: SQL Frontend](https://nikoondata.substack.com/)

This repo contains a small educational prototype of a **mini query engine in Python**,
built on top of **Apache Arrow**.

MQE9 adds a thin SQL frontend on top of the existing DataFrame API:

    SQL string
       └─(sqlglot.parse_one)→ SQL AST (sqlglot expressions)
                   └─(SqlPlanner)→ LogicalPlan

[sqlglot](https://github.com/tobymao/sqlglot) parses the SQL text into itsown AST.
`SqlPlanner` walks that AST and rebuilds it out of the engine's own `Scan`/`Filter`/`Projection` and `LogicalExpr` nodes,
so a SQL query ends up as a regular `LogicalPlan` and runs through the same optimizer,
planner and executor as a query built with `LazyFrame`. There's no catalog or filesystem lookup,
a table has to be registered first with `ctx.register_table("demo", ctx.read.csv(...))` before `FROM demo` resolves.
`ctx.sql(...)` / `mqe.sql(...)` parses a query into a `LazyFrame`.
Only what `LogicalPlan` can express is supported: a single-table `SELECT ... FROM <table> [WHERE ...]`
with column refs, aliases, comparisons, `AND`/`OR` and `+ - * /`.

## 📁 Project Structure

    mqe/
    ├── demo.py
    ├── demo.csv
    ├── demo.parquet
    ├── docker-compose.yaml
    ├── Dockerfile
    ├── pyproject.toml
    ├── uv.lock
    ├── ...
    └── core/
        ├── datatypes.py        # ColumnData: ArrowColumn / LiteralColumn
        ├── tables.py           # SchemaField/TableSchema/DataBatch
        ├── logical_plan.py     # Logical plans (Scan/Filter/Projection) + explain()
        ├── logical_expr.py     # Expression DSL (logical layer)
        ├── datasources.py      # DataSource: InMemory / CSV / Parquet
        ├── readers.py          # DataReader (ctx.read.csv / ctx.read.parquet)
        ├── optimizer.py        # Optimizer + rules: PredicatePushDown / ProjectionPushDown
        ├── sql.py              # SQL frontend: SqlPlanner (sqlglot AST -> LogicalPlan)
        ├── physical_plan.py    # Physical operators + explain(): ScanExec/FilterExec/ProjectionExec
        ├── physical_expr.py    # Bound, executable expressions (physical layer)
        ├── planner.py          # Logical → Physical compilation + binding
        ├── frames.py           # LazyFrame/DataFrame user API
        └── context.py          # ExecutionContext (entry point)

## 🚀 Getting Started

You can run the demo either **inside Docker (recommended)** or **locally**.

### Run with Docker + uv (recommended)

> Requires Docker installed on your machine.

```bash
git clone https://github.com/NikoJ/articles.git
cd mqe9/mqe

docker compose up -d

docker compose exec mqe9 uv run demo.py
```

### Run locally with uv

> Requires [uv](https://docs.astral.sh/uv/) and Python 3.13 installed on your machine.

```bash
git clone https://github.com/NikoJ/articles.git
cd mqe9/mqe

uv sync
uv run demo.py
```
