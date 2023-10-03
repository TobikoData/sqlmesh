import sys
import typing as t
from unittest import mock

import pytest
from sqlglot import exp, parse_one

from sqlmesh.core.dialect import StagedFilePath
from sqlmesh.core.macros import MacroEvaluator, macro
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.metaprogramming import Executable, ExecutableKind


@macro()
def filter_country(
    evaluator: MacroEvaluator, expression: exp.Condition, country: exp.Literal
) -> exp.Condition:
    return t.cast(exp.Condition, exp.and_(expression, exp.column("country").eq(country)))


@macro("UPPER")
def upper_case(evaluator: MacroEvaluator, expression: exp.Condition) -> str:
    return f"UPPER({expression} + 1)"


@macro("noop")
def noop(evaluator: MacroEvaluator):
    return None


@pytest.fixture
def macro_evaluator() -> MacroEvaluator:
    return MacroEvaluator(
        "hive",
        {"test": Executable(name="test", payload=f"def test(_):\n    return 'test'")},
    )


def test_case():
    assert macro.get_registry()["upper"]


def test_external_macro() -> None:
    def foo(evaluator: MacroEvaluator) -> str:
        return "foo"

    # Mimic a SQLMesh macro definition by setting the func's metadata appropriately
    setattr(foo, "__wrapped__", foo)
    setattr(foo, "__sqlmesh_macro__", True)

    sys.modules["pkg"] = mock.Mock()
    with mock.patch("pkg.foo", foo):
        evaluator = MacroEvaluator(
            python_env={
                "foo": Executable(
                    payload="from pkg import foo",
                    kind=ExecutableKind.IMPORT,
                    name=None,
                    path=None,
                    alias=None,
                )
            }
        )

        assert "@FOO" in evaluator.macros
        assert evaluator.macros["@FOO"](evaluator) == "foo"


def test_macro_var(macro_evaluator):
    expression = parse_one("@x")
    for k, v in [
        (1, "1"),
        ("1", "'1'"),
        (None, "NULL"),
        (True, "TRUE"),
        (False, "FALSE"),
    ]:
        macro_evaluator.locals = {"x": k}
        assert macro_evaluator.transform(expression).sql() == v

    # Check Snowflake-specific StagedFilePath / MacroVar behavior
    e = parse_one("select @x from @path, @y", dialect="snowflake")
    macro_evaluator.locals = {"x": parse_one("a"), "y": parse_one("t2")}

    assert e.find(StagedFilePath) is not None
    assert macro_evaluator.transform(e).sql(dialect="snowflake") == "SELECT a FROM @path, t2"

    # Referencing a var that doesn't exist in the evaluator's scope should raise
    macro_evaluator.locals = {}
    for dialect in ("", "snowflake"):
        with pytest.raises(SQLMeshError) as ex:
            macro_evaluator.transform(parse_one("SELECT @y", dialect=dialect))

        assert "Macro variable 'y' is undefined." in str(ex.value)

    # Parsing a "parameter" like Snowflake's $1 should not produce a MacroVar expression
    e = parse_one("select $1 from @path (file_format => bla.foo)", read="snowflake")
    assert e.find(exp.Parameter) is e.selects[0]
    assert e.sql(dialect="snowflake") == "SELECT $1 FROM @path (FILE_FORMAT => bla.foo)"


def test_macro_str_replace(macro_evaluator):
    expression = parse_one("""@'@val1, @val2'""")
    macro_evaluator.locals = {"val1": "one", "val2": "two"}
    assert macro_evaluator.transform(expression).sql() == "'one, two'"


def test_macro_custom(macro_evaluator, assert_exp_eq):
    assert_exp_eq(macro_evaluator.transform(parse_one("SELECT @TEST()")), "SELECT test")


def test_ast_correctness(macro_evaluator):
    macro_evaluator.locals = {"x": "y"}
    assert macro_evaluator.transform(parse_one("SELECT * FROM @SQL('@x')")).find(
        exp.Table
    ) == exp.table_("y")


@pytest.mark.parametrize(
    "sql, expected, args",
    [
        (
            "select @each([(a, b), (c, d)], (x, y) -> x as y)",
            "SELECT a AS b, c AS d",
            {},
        ),
        (
            "select @each(['a', 'b'], x -> x = y as is_@{x})",
            "SELECT 'a' = y as is_a, 'b' = y as is_b",
            {},
        ),
        (
            """@UPPER(@EVAL(1 + 1)) + 2""",
            "UPPER(2 + 1) + 2",
            {},
        ),
        (
            """@UPPER(x) + 2""",
            "UPPER(x + 1) + 2",
            {},
        ),
        (
            """select @EACH([a, b], x -> x)""",
            "SELECT a, b",
            {},
        ),
        (
            """@FILTER_COUNTRY(continent = 'NA', 'USA')""",
            "continent = 'NA' AND country = 'USA'",
            {},
        ),
        ("""@SQL(@REDUCE([100, 200, 300, 400], (x,y) -> x + y))""", "1000", {}),
        (
            """@SQL(@REDUCE(@FILTER([100, 200, 300, 400], x -> x > 250), (x,y) -> x + y))""",
            "700",
            {},
        ),
        (
            """select @EACH([a, b, c], x -> x and @SQL('@y'))""",
            "SELECT a AND z, b AND z, c AND z",
            {"y": "z"},
        ),
        (
            """select @REDUCE([a, b, c], (x, y) -> x and y and @SQL('@z'))""",
            "SELECT a AND b AND z2 AND c AND z2",
            {"z": "z2"},
        ),
        (
            """select @REDUCE(@EACH([a, b, c], x -> column like x), (x, y) -> x or y)""",
            "SELECT column LIKE a OR column LIKE b OR column LIKE c",
            {},
        ),
        (
            """select @EACH([a, b, c], x -> column like x AS @SQL('@{x}_y', 'Identifier')), @x""",
            "SELECT column LIKE a AS a_y, column LIKE b AS b_y, column LIKE c AS c_y, '3'",
            {"x": "3"},
        ),
        (
            """@WITH(@do_with) all_cities as (select * from city) select all_cities""",
            "WITH all_cities AS (SELECT * FROM city) SELECT all_cities",
            {"do_with": True},
        ),
        (
            """@WITH(@do_with) all_cities as (select * from city) select all_cities""",
            "SELECT all_cities",
            {"do_with": False},
        ),
        (
            """select * from city left outer @JOIN(@do_join) country on city.country = country.name""",
            "SELECT * FROM city LEFT OUTER JOIN country ON city.country = country.name",
            {"do_join": True},
        ),
        (
            """select * from city left outer @JOIN(@do_join) country on city.country = country.name""",
            "SELECT * FROM city",
            {"do_join": False},
        ),
        (
            """select * from city @JOIN(@do_join) country on city.country = country.name""",
            "SELECT * FROM city JOIN country ON city.country = country.name",
            {"do_join": True},
        ),
        (
            """select * from city @WHERE(@do_where) population > 100 and country = 'Mexico'""",
            "SELECT * FROM city WHERE population > 100 AND country = 'Mexico'",
            {"do_where": True},
        ),
        (
            """select * from city @WHERE(@do_where) population > 100 and country = 'Mexico'""",
            "SELECT * FROM city",
            {"do_where": False},
        ),
        (
            """select * from city @WHERE(@x > 5) population > 100 and country = 'Mexico'""",
            "SELECT * FROM city WHERE population > 100 AND country = 'Mexico'",
            {"x": 6},
        ),
        (
            """select * from city @WHERE(@x > 5) population > 100 and country = 'Mexico'""",
            "SELECT * FROM city",
            {"x": 5},
        ),
        (
            """select * from city @GROUP_BY(@do_group) country, population""",
            "SELECT * FROM city GROUP BY country, population",
            {"do_group": True},
        ),
        (
            """select * from city @GROUP_BY(@do_group) country, population""",
            "SELECT * FROM city",
            {"do_group": False},
        ),
        (
            """select * from city group by country @HAVING(@do_having) population > 100 and country = 'Mexico'""",
            "SELECT * FROM city GROUP BY country HAVING population > 100 AND country = 'Mexico'",
            {"do_having": True},
        ),
        (
            """select * from city group by country @HAVING(@do_having) population > 100 and country = 'Mexico'""",
            "SELECT * FROM city GROUP BY country",
            {"do_having": False},
        ),
        (
            """select * from city @ORDER_BY(@do_order) population, name DESC""",
            "SELECT * FROM city ORDER BY population, name DESC",
            {"do_order": True},
        ),
        (
            """select * from city @ORDER_BY(@do_order) population, name DESC""",
            "SELECT * FROM city",
            {"do_order": False},
        ),
        (
            """select @if(TRUE, 1, 0)""",
            "SELECT 1",
            {},
        ),
        (
            """select @if(FALSE, 1, 0)""",
            "SELECT 0",
            {},
        ),
        (
            """select @if(1 > 0, 1, 0)""",
            "SELECT 1",
            {},
        ),
        (
            """select @if('a' = 'b', c), d""",
            "SELECT d",
            {},
        ),
    ],
)
def test_macro_functions(macro_evaluator, assert_exp_eq, sql, expected, args):
    macro_evaluator.locals = args or {}
    assert_exp_eq(macro_evaluator.transform(parse_one(sql)), expected)


def test_macro_returns_none(macro_evaluator):
    assert macro_evaluator.transform(parse_one("@NOOP()")) is None
