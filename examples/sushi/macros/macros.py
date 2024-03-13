from macros.utils import between  # type: ignore

from sqlmesh import macro


@macro()
def incremental_by_ds(evaluator, column):
    return between(evaluator, column, evaluator.locals["start_date"], evaluator.locals["end_date"])
