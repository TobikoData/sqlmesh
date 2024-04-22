from macros.utils import between  # type: ignore
from sqlglot import exp

from sqlmesh import macro


@macro()
def incremental_by_ds(evaluator, column: exp.Column):
    return between(evaluator, column, evaluator.locals["start_date"], evaluator.locals["end_date"])
