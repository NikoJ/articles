from core import DataFrame, ExecutionContext, LazyFrame, col


def main() -> None:
    ct: ExecutionContext = ExecutionContext()

    lf: LazyFrame = ct.from_dict(
        {
            "id": [1, 2, 3],
            "first_name": ["Niko", "Alice", "Joy"],
            "state": ["CO", "CA", "NY"],
        }
    )

    lf = lf.filter(col("first_name") == "Niko").select(
        "id", (col("id") * 2).alias("new_id"), "first_name"
    )
    
    lf.explain(True)

    result: DataFrame = lf.collect()
    print("\n===== EXAMPLE 1 =====\n")
    print(result)

    result2: DataFrame = result.select("first_name")
    print("\n===== EXAMPLE 2 =====\n")
    print(result2)


if __name__ == "__main__":
    main()
