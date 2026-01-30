# [MQE6](https://nikoondata.substack.com/)

This repo contains a small educational prototype of a **mini query engine in Python**,
built on top of **Apache Arrow**.

TODO

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
cd mqe6/mqe

docker compose up -d

docker compose exec mqe6 uv run demo.py
```
