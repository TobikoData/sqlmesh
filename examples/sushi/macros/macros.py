from sqlmesh import macro


@macro()
def add_one(evaluator, column):
    return f"{column} + 1"


@macro()
def incremental_by_ds(evaluator, column):
    return column.between(evaluator.locals["start_ds"], evaluator.locals["end_ds"])
