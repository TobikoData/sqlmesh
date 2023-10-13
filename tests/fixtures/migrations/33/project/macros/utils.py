from sqlmesh import macro


@macro()
def add_one(evaluator, column):
    return f"{column} + 1"


@macro()
def between(evaluator, value, start, end):
    return value.between(start, end)
