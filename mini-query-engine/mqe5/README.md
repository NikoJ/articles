# [MQE5](https://nikoondata.substack.com/)

This repo contains a small educational prototype of a **mini query engine in Python**,
built on top of **Apache Arrow**.

MQE5 focuses on the **physical plan** layer. We implement the core
physical operators and run an end-to-end execution pipeline (the logical→physical
translation will be added in the next part).

In this part we implement:
- **Physical plan tree** with `explain(verbose=True)`
- **Streaming-friendly execution**: operators return `Iterator[DataBatch]`
- **Vectorized compute with Arrow** (`pyarrow.compute` kernels)
- Core operators: `ScanExec` → `FilterExec` → `ProjectionExec`
- A small **in-memory data source** for reproducible demos

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
        └── physical_expr.py    # Executable expressions (Arrow-first evaluation)
---

## 🚀 Getting Started

You can run the demo either **inside Docker (recommended)** or **locally**.

### Run with Docker + uv (recommended)

> Requires Docker installed on your machine.

```bash
git clone https://github.com/NikoJ/articles.git
cd mqe5/mqe

docker compose up -d

docker compose exec mqe5 uv run demo.py
```
