# [MQE3: Preparing Expressions for Logical Plans](https://nikoondata.substack.com/p/mqe3-preparing-exprs-for-logical-plans)

This repo contains a small educational prototype of a **mini query engine in Python**,
built on top of **Apache Arrow**.

MQE3 focuses on the **logical expression layer**: a small expression AST + DSL that can be
attached to logical plan operators during query planning.

In this part we implement:

- a minimal `LogicalExpr` contract (`to_field(input_plan) -> SchemaField`)
- `LogicalExprNode` with operator overloading to build expression trees
- core expression nodes:
  - column references (`Column`, `col("x")`)
  - literals (`lit(123)`, `lit("xxx")`)
  - boolean ops (`AND`, `OR`, `NOT`)
  - comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`)
  - arithmetic (`+`, `-`, `*`, `/`, `%`)
  - aliases (`AS`), casts (`CAST`), scalar functions

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
        ├── logical_plan.py     # LogicalPlan + LogicalExpr contracts
        └── logical_expr.py     # MQE3: expression AST + DSL (this part)

---

## 🚀 Getting Started

You can run the demo either **inside Docker (recommended)** or **locally**.

### Run with Docker + uv (recommended)

> Requires Docker installed on your machine.

```bash
git clone https://github.com/NikoJ/articles.git
cd mqe3/mqe

docker compose up -d

docker compose exec mqe3 uv run demo.py
```
