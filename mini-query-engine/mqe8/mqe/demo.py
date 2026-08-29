import core as mqe
from core import col
from core.optimizer import Optimizer, PredicatePushDown, ProjectionPushDown

RULE_SETS: list[tuple[str, list]] = [
    ("ProjectionPushDown ONLY", [ProjectionPushDown()]),
    ("PredicatePushDown ONLY", [PredicatePushDown()]),
    ("BOTH", [PredicatePushDown(), ProjectionPushDown()]),
]


def compare_rules(lf: mqe.LazyFrame) -> None:
    ctx = mqe.get_context()
    original_optimizer = ctx.optimizer

    print("\n----- NO OPTIMIZATION -----")
    lf.explain(verbose=True, optimized=False)

    try:
        for label, rules in RULE_SETS:
            ctx.optimizer = Optimizer(rules=rules)
            print(f"\n----- {label} -----")
            lf.explain(verbose=True, optimized=True)
    finally:
        ctx.optimizer = original_optimizer


def main() -> None:
    lf: mqe.LazyFrame = (
        mqe.read.csv("/data/demo.csv")
        .select("id", "first_name")
        .filter(col("first_name") == "Niko")
    )
    compare_rules(lf)

    print("\n===== RESULT =====\n")
    print(lf.collect())


if __name__ == "__main__":
    main()
