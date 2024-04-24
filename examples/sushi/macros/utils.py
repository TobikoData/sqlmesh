from sqlglot import exp

from sqlmesh import macro


@macro()
def add_one(evaluator, column: int):
    # typed column will be cast to an int and return an integer back
    assert isinstance(column, int)
    return column + 1


@macro()
def is_positive(evaluator, column):
    # untyped column will be a sqlglot column and return a sqlglot exp "column > 0"
    assert isinstance(column, exp.Column)
    return column > 0


@macro()
def between(evaluator, value, start, end):
    return value.between(start, end)
