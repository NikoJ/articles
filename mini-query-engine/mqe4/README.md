# [MQE4](https://nikoondata.substack.com/)

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
        ├── datatypes.py        
        ├── tables.py           
        ├── logical_plan.py     
        ├── logical_expr.py    
        └── TODO

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
