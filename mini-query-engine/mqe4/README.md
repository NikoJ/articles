# [MQE4: Logical Query Plans (Scan, Filter, Projection)](https://nikoondata.substack.com/)

This repo contains a small educational prototype of a **mini query engine in Python**,
built on top of **Apache Arrow**.

MQE4 focuses on the logical plan layer: a minimal query plan tree that describes what should happen in a query, without executing any data.

In this part we implement:
- a minimal LogicalPlan contract:
    - `schema() -> TableSchema`
    - `children() -> list[LogicalPlan]`
    - `explain(verbose=False) -> str`
- core logical plan nodes:
    - `Scan` (leaf node, reads schema from a `DataSource`)
    - `Filter` (row selection using a boolean `LogicalExpr`)
    - `Projection` (output expressions using `LogicalExpr.to_field(input_plan))
- projection pruning support via `TableSchema.select([...])`
- a readable plan tree printer (`EXPLAIN`-style output), including `verbose=True` mode

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
        ├── datatypes.py    # Arrow data types used by the engine
        ├── tables.py       # SchemaField/TableSchema/DataBatch (+ select)
        ├── logical_plan.py # MQE4: logical plans (Scan/Filter/Projection) + explain()
        ├── logical_expr.py # Expression DSL from MQE3 (used by Filter/Projection)
        └── datasources.py  # DataSource stub (schema-only for planning)

---

## 🚀 Getting Started

You can run the demo either **inside Docker (recommended)** or **locally**.

### Run with Docker + uv (recommended)

> Requires Docker installed on your machine.

```bash
git clone https://github.com/NikoJ/articles.git
cd mqe4/mqe

docker compose up -d

docker compose exec mqe4 uv run demo.py
```
