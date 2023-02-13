from sqlglot import exp

from sqlmesh.core.dialect import MacroVar
from sqlmesh.core.macros import MacroEvaluator, macro
from sqlmesh.utils.errors import MacroEvalError


@macro()
def add_one(evaluator: MacroEvaluator, column: str) -> exp.Expression:
    return evaluator.parse_one(f"{column} + 1")


@macro()
def incremental_by_ds(evaluator: MacroEvaluator, column: exp.Column) -> exp.Expression:
    expression = evaluator.transform(
        exp.Between(this=column, low=MacroVar(this="start_ds"), high=MacroVar(this="end_ds"))
    )
    if not isinstance(expression, exp.Expression):
        raise MacroEvalError(f"Return type is {type(expression)}, expected exp.Expression")

    return expression
