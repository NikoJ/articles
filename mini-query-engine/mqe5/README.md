# [MQE5](https://nikoondata.substack.com/)

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
        ├── datatypes.py        # Arrow data types used by the engine
        ├── tables.py           # SchemaField/TableSchema/DataBatch
        ├── logical_plan.py     # Logical plans (Scan/Filter/Projection) + explain()
        ├── logical_expr.py     # Expression DSL
        ├── datasources.py      # DataSource stub (schema-only for planning)
        ├── physical_plan.py    # TODO
        └── physical_expr.py    # TODO
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
