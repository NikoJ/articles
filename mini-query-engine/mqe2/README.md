# MQE2

This repo contains a small educational prototype of a **mini query engine in Python**,
built on top of **Apache Arrow**.

This specific demo focuses on the **data model layer**:

- table schema (`TableSchema`, `SchemaField`)
- column abstraction (`ColumnData`, `ArrowColumn`, `LiteralColumn`)
- in-memory batches of data (`DataBatch`)
- a small `demo.py` script that builds a schema, creates Arrow-backed columns,
  assembles a `DataBatch` and prints it in a readable form.

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
        ├── datatypes.py
        └── tables.py

---

## 🚀 Getting Started

You can run the demo either **inside Docker (recommended)** or **locally**.

### Run with Docker + uv (recommended)

> Requires Docker installed on your machine.

```bash
git clone https://github.com/NikoJ/articles.git
cd mqe2/mqe

docker compose up -d

docker compose exec mqe2 uv run demo.py
```
