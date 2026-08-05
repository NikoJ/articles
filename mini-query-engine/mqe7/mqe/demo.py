import core as mqe
from core import col


def main() -> None:
    # Example *csv*
    lf: mqe.LazyFrame = (
        mqe.read.csv("/data/demo.csv")
        .filter(col("first_name") == "Niko")
        .select("id", (col("id") * 2).alias("new_id"), "first_name")
    )

    print("\n===== EXPLAIN PLAN (csv) =====")
    lf.explain(verbose=True)

    print("\n===== EXAMPLE 1 (csv) =====\n")
    result: mqe.DataFrame = lf.collect()
    print(result)

    print("\n===== EXAMPLE 2 (csv) =====\n")
    result2: mqe.DataFrame = result.select("first_name")
    print(result2)

    # Example *parquet*
    lf: mqe.LazyFrame = (
        mqe.read.parquet("/data/demo.parquet")
        .filter(col("first_name") == "Alice")
        .select("id", (col("id") * 2).alias("new_id"), "first_name")
    )

    print("\n===== EXPLAIN PLAN (parquet) =====")
    lf.explain(verbose=True)

    print("\n===== EXAMPLE 1 (parquet) =====\n")
    result: mqe.DataFrame = lf.collect()
    print(result)

    print("\n===== EXAMPLE 2 (parquet) =====\n")
    result2: mqe.DataFrame = result.select("first_name")
    print(result2)


if __name__ == "__main__":
    main()
