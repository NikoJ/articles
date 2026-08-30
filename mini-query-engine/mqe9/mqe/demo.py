import core as mqe
from core import col


def main() -> None:
    # DataFrame API
    lf: mqe.LazyFrame = (
        mqe.read.csv("/data/demo.csv")
        .filter(col("first_name") == "Niko")
        .select("id", (col("id") * 2).alias("new_id"), "first_name")
    )

    print("\n===== EXPLAIN PLAN (DataFrame API) =====")
    lf.explain(verbose=True)

    print("\n===== RESULT (DataFrame API) =====\n")
    print(lf.collect())

    # SQL
    mqe.register_table("demo", mqe.read.csv("/data/demo.csv"))

    print("\n===== SQL AST (sqlglot) =====")
    sql_lf: mqe.LazyFrame = mqe.sql(
        """
        SELECT id, (id * 2) AS new_id, first_name
        FROM demo
        WHERE first_name = 'Niko'
        """,
        verbose=True,
    )

    print("\n===== EXPLAIN PLAN (SQL) =====")
    sql_lf.explain(verbose=True)

    print("\n===== RESULT (SQL) =====\n")
    print(sql_lf.collect())


if __name__ == "__main__":
    main()
