from sqlmesh.core.macros import MacroEvaluator, macro


@macro()
def add_one(evaluator: MacroEvaluator, column: str):
    return evaluator.parse_one(f"{column} + 1")
