from sqlmesh import macro


@macro()
def add_one(evaluator, column):
    return f"{column} + 1"


@macro()
def between(evaluator, value, start, end):
    return value.between(start, end)


@macro()
def incremental_by_ds(evaluator, column):
    return between(evaluator, column, evaluator.locals["start_ds"], evaluator.locals["end_ds"])
