# Mini Query Engine in Python

This repository is the home for my educational project and article series on [**Building a Mini Query Engine in Python**](https://nikoondata.substack.com/p/plan-building-a-mini-query-engine).

## What this project is

- A **learning playground** for understanding how query engines work under the hood.
- A **step-by-step companion** to my Substack series [*Plan: Building a Mini Query Engine in Python*](https://nikoondata.substack.com/p/plan-building-a-mini-query-engine).
- A **minimal engine core**, focused on clarity rather than features.

The engine is built around:

- **Apache Arrow** as the in-memory data model (types, schema, columnar layout).
- A **DataFrame-style API first**. **SQL** is added later as a thin frontend.
- A classic pipeline:
*Query → Logical Plan → Optimizer → Physical Plan → Executor → Result*.

## What this project is *not*

- Not a production-ready database.
- Not distributed or parallel.
- Not a replacement for DuckDB, Polars, Spark or any other system.

## Draft plan for the Mini Query Engine in Python series

- [MQE1: Overall Architecture of a Mini Query Engine](https://nikoondata.substack.com/p/mqe1-overall-architecture-of-mini-query-engine)
- How we store data inside the engine (data model on Apache Arrow)
- Expression DSL (col(), lit(), conditions and operations)
- Logical plan: Scan/Filter/Projection
- Physical plan and planner: how logic turns into execution
- Executor: running the physical plan
- JOIN: combining tables
- GROUP BY and aggregates
- Optimizer + EXPLAIN: simple logical rewrites
- Data sources: CSV and the DataSource layer
- SQL frontend: plugging in a ready-made parser

---

For each article that includes code examples, there will be a corresponding folder named `mqe*` (e.g. `mqe2`, `mqe3`, …) with runnable code you can execute on your local machine.

>Links to the published articles will be added here as they go live.

---

📖 More content: [Niko on Data on Substack](https://nikoondata.substack.com/)
