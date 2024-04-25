from sqlglot import exp

from sqlmesh import SQL, macro


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
def sql_literal(
    evaluator,
    column: SQL,
    str_lit: SQL,
    string: str,
    column_str: str,
    column_quoted: str,
    column_with_db: str,
):
    assert isinstance(column, str)
    assert isinstance(str_lit, str)
    assert str_lit == "'x'"
    assert isinstance(string, str)
    assert string == "y"
    assert isinstance(column_str, str)
    assert column_str == "a"
    assert isinstance(column_quoted, str)
    assert column_quoted == "b"
    assert isinstance(column_with_db, exp.Column)
    assert column_with_db.sql() == "c.d"

    return column


@macro()
def between(evaluator, value, start, end):
    return value.between(start, end)
