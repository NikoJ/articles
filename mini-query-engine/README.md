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
- [MQE2: Data Model of a Mini Query Engine in Python](https://nikoondata.substack.com/p/mqe2-data-model-of-mini-query-engine)
- [MQE3: Preparing Expressions for Logical Plans](https://nikoondata.substack.com/p/mqe3-preparing-exprs-for-logical-plans)
- [MQE4: Logical Query Plans (Scan, Filter, Projection)](https://nikoondata.substack.com/p/mqe4-logical-query-plans-scan-filter-projection)
- [MQE5: Physical Plan and Query Execution](https://nikoondata.substack.com/p/mqe5-physical-plan-and-query-execution)
- [MQE6: From DataFrame API to Query Execution](https://nikoondata.substack.com/p/mqe6-from-dataframe-api-to-query-execution)
- MQE7: Data sources: CSV and the DataSource layer
- MQE8: Optimizer + EXPLAIN: simple logical rewrites
- MQE9: SQL frontend: plugging in a ready-made parser

---

For each article that includes code examples, there will be a corresponding folder named `mqe*` (e.g. `mqe2`, `mqe3`, …) with runnable code you can execute on your local machine.

>Links to the published articles will be added here as they go live.

---

📖 More content: [Niko on Data on Substack](https://nikoondata.substack.com/)
