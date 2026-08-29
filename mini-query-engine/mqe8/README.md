# [MQE8: Optimizer + EXPLAIN](https://nikoondata.substack.com/)

This repo contains a small educational prototype of a **mini query engine in Python**,
built on top of **Apache Arrow**.

MQE8 gives the engine its first real logical-plan optimizations: a small,
rule-based `Optimizer` rewrites a `LogicalPlan` before it's compiled to a physical plan.
`PredicatePushDown` moves `Filter` nodes below `Projection` nodes when every column the
predicate needs passes through unchanged, and `ProjectionPushDown` prunes each `Scan`'s column
projection down to only the columns still referenced further up the tree — so
`CSVDataSource`/`ParquetDataSource` read less data. `LazyFrame.explain()` now
takes an `optimized` flag and prints the optimized logical plan alongside the
original and the physical plan.

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
cd mqe8/mqe

docker compose up -d

docker compose exec mqe8 uv run demo.py
```

### Run locally with uv

> Requires [uv](https://docs.astral.sh/uv/) and Python 3.13 installed on your machine.

```bash
git clone https://github.com/NikoJ/articles.git
cd mqe8/mqe

uv sync
uv run demo.py
```
