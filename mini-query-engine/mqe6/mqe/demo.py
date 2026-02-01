import core as mqe
from core import col


def main() -> None:
    # Example
    lf: mqe.LazyFrame = (
        mqe.from_dict(
            {
                "id": [1, 2, 3],
                "first_name": ["Niko", "Alice", "Joy"],
                "state": ["CO", "CA", "NY"],
            }
        )
        .filter(col("first_name") == "Niko")
        .select("id", (col("id") * 2).alias("new_id"), "first_name")
    )

    lf.explain(verbose=True)

    result: mqe.DataFrame = lf.collect()
    print("\n===== EXAMPLE 1 =====\n")
    print(result)

    result2: mqe.DataFrame = result.select("first_name")
    print("\n===== EXAMPLE 2 =====\n")
    print(result2)


if __name__ == "__main__":
    main()
