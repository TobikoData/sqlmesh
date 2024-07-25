import typing as t

import pytest
from sqlglot import MappingSchema, ParseError, exp, parse_one

from sqlmesh.core import constants as c, dialect as d
from sqlmesh.core.dialect import StagedFilePath
from sqlmesh.core.macros import SQL, MacroEvalError, MacroEvaluator, macro
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.metaprogramming import Executable


@pytest.fixture
def macro_evaluator() -> MacroEvaluator:
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

    @macro()
    def bitshift_square(evaluator: MacroEvaluator, x: int, y: int) -> int:
        return (x >> y) ** 2

    @macro()
    def prefix_db(evaluator: MacroEvaluator, table: exp.Table, prefix: str) -> exp.Table:
        table.set("db", prefix + table.db)
        return table

    @macro()
    def repeated(evaluator: MacroEvaluator, expr: str, times: int = 2, multi: bool = False):
        if multi is True:
            return (expr,) * times
        return expr * times

    @macro()
    def join_str(evaluator: MacroEvaluator, arg: str, *args: str, **kwargs: str):
        return exp.Literal.string(
            "-".join(
                [
                    arg,
                    *args,
                    *(f"{k}:{v}" for k, v in kwargs.items()),
                ]
            )
        )

    @macro()
    def split(evaluator: MacroEvaluator, string: str, sep: str = ","):
        return string.split(sep)

    @macro()
    def cte_tag_name(evaluator: MacroEvaluator, with_: exp.Select):
        for cte in with_.find_all(exp.CTE):
            name = cte.alias_or_name
            for query in cte.find_all(exp.Select):
                query.select(exp.Literal.string(name).as_("source"), copy=False)
        return with_

    @macro()
    def suffix_idents(evaluator: MacroEvaluator, items: t.List[str], suffix: str):
        return [item + suffix for item in items]

    @macro()
    def suffix_idents_2(evaluator: MacroEvaluator, items: t.Tuple[str, ...], suffix: str):
        return [item + suffix for item in items]

    @macro()
    def stamped(evaluator, query: exp.Select) -> exp.Subquery:
        return query.select(exp.Literal.string("2024-01-01").as_("stamp")).subquery()

    @macro()
    def test_arg_resolution(evaluator, pos_only, /, a1, *, a2=1, **rest):
        return 1

    @macro()
    def test_default_arg_coercion(
        evaluator: MacroEvaluator,
        a1: int = 1,
        a2: int = exp.Literal.number(2),  # type: ignore
    ):
        return sum([a1, a2])

    return MacroEvaluator(
        "hive",
        {"test": Executable(name="test", payload="def test(_):\n    return 'test'")},
    )


def test_star(assert_exp_eq) -> None:
    sql = """SELECT @STAR(foo) FROM foo"""
    expected_sql = """SELECT CAST([foo].[a] AS DATETIMEOFFSET) AS [a], CAST([foo].[b] AS INTEGER) AS [b] FROM foo"""
    schema = MappingSchema(
        {
            "foo": {
                "a": exp.DataType.build("datetimeoffset", dialect="tsql"),
                "b": "int",
            },
        },
        dialect="tsql",
    )
    evaluator = MacroEvaluator(schema=schema, dialect="tsql")
    assert_exp_eq(evaluator.transform(parse_one(sql, read="tsql")), expected_sql, dialect="tsql")

    sql = """SELECT @STAR(foo, exclude := [SomeColumn]) FROM foo"""
    expected_sql = "SELECT CAST(`foo`.`a` AS STRING) AS `a` FROM foo"
    schema = MappingSchema(
        {
            "foo": {
                "a": exp.DataType.build("string"),
                "somecolumn": "int",
            },
        },
        dialect="databricks",
    )
    evaluator = MacroEvaluator(schema=schema, dialect="databricks")
    assert_exp_eq(
        evaluator.transform(parse_one(sql, read="databricks")),
        expected_sql,
        dialect="databricks",
    )


def test_start_no_column_types(assert_exp_eq) -> None:
    sql = """SELECT @STAR(foo) FROM foo"""
    expected_sql = """SELECT [foo].[a] AS [a] FROM foo"""
    schema = MappingSchema({"foo": {"a": exp.DataType.build("UNKNOWN")}}, dialect="tsql")
    evaluator = MacroEvaluator(schema=schema, dialect="tsql")
    assert_exp_eq(evaluator.transform(parse_one(sql, read="tsql")), expected_sql, dialect="tsql")


def test_case(macro_evaluator: MacroEvaluator) -> None:
    assert macro.get_registry()["upper"]


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
    assert e.find(StagedFilePath)
    # test no space
    e = parse_one("select $1 from @path(file_format => bla.foo)", read="snowflake")
    assert e.find(StagedFilePath)
    assert e.sql(dialect="snowflake") == "SELECT $1 FROM @path (FILE_FORMAT => bla.foo)"

    macro_evaluator.locals = {"x": 1}
    macro_evaluator.dialect = "snowflake"
    e = parse_one("COPY INTO @'s3://example/foo_@{x}.csv' FROM a.b.c", read="snowflake")
    assert (
        macro_evaluator.transform(e).sql(dialect="snowflake")
        == "COPY INTO 's3://example/foo_1.csv' FROM a.b.c"
    )


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
            """select @each(['a', 'b'], x -> @x + @{x}_z + @y + @{y}_@{x})""",
            "SELECT 'a' + a_z + 'c' + c_a, 'b' + b_z + 'c' + c_b",
            {"y": "c"},
        ),
        (
            '"is_@{x}"',
            '"is_b"',
            {"x": "b"},
        ),
        (
            "@{x}_is",
            "b_is",
            {"x": "b"},
        ),
        (
            "@{x}_@{y}",
            "b_c",
            {"x": "b", "y": "c"},
        ),
        (
            "@{x}_@y",
            "b_c",
            {"x": "b", "y": "c"},
        ),
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
            """select * from city @LIMIT(@do_limit) 10""",
            "SELECT * FROM city LIMIT 10",
            {"do_limit": True},
        ),
        (
            """select * from city @LIMIT(@do_limit) 10""",
            "SELECT * FROM city",
            {"do_limit": False},
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
        (
            """@BITSHIFT_SQUARE(50, '3')""",
            "36",
            {},
        ),
        (
            """@PREFIX_DB(my.schema.table, 'dev_')""",
            "my.dev_schema.table",
            {},
        ),
        (
            """select @REPEATED(test, 3)""",
            "SELECT testtesttest",
            {},
        ),
        (
            """select @REPEATED(test, 3, true)""",
            "SELECT test, test, test",
            {},
        ),
        (
            """select @SPLIT('a,b,c')""",
            "SELECT a, b, c",
            {},
        ),
        (
            """@CTE_TAG_NAME(WITH step1 AS (SELECT 1) SELECT * FROM step1)""",
            "WITH step1 AS (SELECT 1, 'step1' AS source) SELECT * FROM step1",
            {},
        ),
        (
            """@CTE_TAG_NAME('WITH step1 AS (SELECT 1) SELECT * FROM step1')""",
            "WITH step1 AS (SELECT 1, 'step1' AS source) SELECT * FROM step1",
            {},
        ),
        (
            """SELECT @SUFFIX_IDENTS(['a', 'b', 'c'], 'z')""",
            "SELECT az, bz, cz",
            {},
        ),
        (
            """SELECT @SUFFIX_IDENTS_2(['a', 'b', 'c'], 'z')""",
            "SELECT az, bz, cz",
            {},
        ),
        (
            """select * FROM @STAMPED('select a, b, c')""",
            "SELECT * FROM (SELECT a, b, c, '2024-01-01' AS stamp)",
            {},
        ),
        (
            """@OR(NULL, TRUE)""",
            "TRUE",
            {},
        ),
        (
            """@OR(NULL, NULL)""",
            "TRUE",
            {},
        ),
        (
            """@OR(NULL)""",
            "TRUE",
            {},
        ),
        (
            """@OR()""",
            "TRUE",
            {},
        ),
        (
            """@AND(TRUE, NULL)""",
            "TRUE",
            {},
        ),
        (
            """select * from x ORDER BY @EACH(@x, c -> c)""",
            "SELECT * FROM x ORDER BY a, b",
            {"x": [exp.column("a"), exp.column("b")]},
        ),
        (
            """select @SQL('@x')""",
            "SELECT VAR_MAP('a', 1)",
            {
                "x": {"a": 1},
            },
        ),
        (
            """select @repeated(x, multi := True)""",
            "SELECT x, x",
            {},
        ),
        (
            """select @JOIN_STR(a1, b1, c2, d := d1, e := e2)""",
            "SELECT 'a1-b1-c2-d:d1-e:e2'",
            {},
        ),
        (
            """select @TEST_DEFAULT_ARG_COERCION()""",
            "SELECT 3",
            {},
        ),
    ],
)
def test_macro_functions(macro_evaluator: MacroEvaluator, assert_exp_eq, sql, expected, args):
    macro_evaluator.locals = args or {}
    assert_exp_eq(macro_evaluator.transform(parse_one(sql)), expected)


def test_macro_returns_none(macro_evaluator: MacroEvaluator):
    assert macro_evaluator.transform(parse_one("@NOOP()")) is None


def test_transform_no_template(mocker, macro_evaluator):
    spy = mocker.spy(macro_evaluator, "template")
    macro_evaluator.transform(parse_one("select a, b"))
    spy.assert_not_called()


def test_macro_coercion(macro_evaluator: MacroEvaluator, assert_exp_eq):
    coerce = macro_evaluator._coerce
    assert coerce(exp.Literal.number(1), int) == 1
    assert coerce(exp.Literal.number(1.1), float) == 1.1
    assert coerce(exp.Literal.string("Hi mom"), str) == "Hi mom"
    assert coerce(exp.true(), bool) is True

    # Coercing a string literal to a column should return a column with the same name
    assert_exp_eq(coerce(exp.Literal.string("order"), exp.Column), exp.column("order"))
    # Not possible to coerce this string literal Cast to an exp.Column node -- so it should just return the input
    assert_exp_eq(
        coerce(exp.Literal.string("order::date"), exp.Column), exp.Literal.string("order::date")
    )
    # This however, is correctly coercible since it's a cast
    assert_exp_eq(
        coerce(exp.Literal.string("order::date"), exp.Cast), exp.cast(exp.column("order"), "DATE")
    )

    # Here we resolve ambiguity via the user type hint
    assert_exp_eq(coerce(exp.Literal.string("order"), exp.Identifier), exp.to_identifier("order"))
    assert_exp_eq(coerce(exp.Literal.string("order"), exp.Table), exp.table_("order"))

    # Resolve a union type hint by choosing the first one that works
    assert_exp_eq(
        coerce(exp.Literal.string("order::date"), t.Union[exp.Column, exp.Cast]),
        exp.cast(exp.column("order"), "DATE"),
    )

    # Simply ask for a string, and always get a string
    assert coerce(exp.column("order"), str) == "order"

    # this is not legal since select is too complex
    assert isinstance(coerce(parse_one("SELECT 1"), str), exp.Select)

    # From a string literal to a Select should parse the string literal, and the inverse operation works as well
    assert_exp_eq(
        coerce(exp.Literal.string("SELECT 1 FROM a"), exp.Select), parse_one("SELECT 1 FROM a")
    )
    assert coerce(parse_one("SELECT 1 FROM a"), SQL) == "SELECT 1 FROM a"

    # Get a list of exp directly instead of an exp.Array
    assert coerce(parse_one("[1, 2, 3]"), list) == [
        exp.Literal.number(1),
        exp.Literal.number(2),
        exp.Literal.number(3),
    ]

    # Generics work as well, recursively resolving inner types
    assert coerce(parse_one("[1, 2, 3]"), t.List[int]) == [1, 2, 3]
    assert coerce(parse_one("[1, 2, 3]"), t.Tuple[int, int, float]) == (1, 2, 3.0)
    assert coerce(parse_one("[1, 2, 3]"), t.Tuple[int, ...]) == (1, 2, 3)
    assert coerce(parse_one("[1, 2, 3]"), t.Tuple[int, str, float]) == (1, "2", 3.0)
    assert coerce(
        parse_one("[1, 2, [3]]"), t.Tuple[int, str, t.Union[float, t.Tuple[float, ...]]]
    ) == (1, "2", (3.0,))

    # Using exp.Expression will always return the input expression
    assert coerce(parse_one("order", into=exp.Column), exp.Expression) == exp.column("order")
    assert coerce(exp.Literal.string("OK"), exp.Expression) == exp.Literal.string("OK")

    # Strict flag allows raising errors and is used when recursively coercing expressions
    # otherwise, in general, we want to be lenient and just warn the user when something is not possible
    with pytest.raises(ParseError):
        coerce(exp.Literal.string("order"), exp.Select, strict=True)

    with pytest.raises(SQLMeshError):
        _ = coerce(
            exp.Literal.string("order::date"),
            t.Union[exp.Column, exp.Identifier],
            strict=True,
        )

    with pytest.raises(SQLMeshError):
        _ = coerce(
            exp.column("a", "b"),
            str,
            strict=True,
        )


def test_positional_follows_kwargs(macro_evaluator):
    with pytest.raises(MacroEvalError, match="Positional argument cannot follow"):
        macro_evaluator.evaluate(parse_one("@repeated(x, multi := True, 3)"))


def test_macro_parameter_resolution(macro_evaluator):
    with pytest.raises(MacroEvalError) as e:
        macro_evaluator.evaluate(parse_one("@test_arg_resolution()"))
    assert str(e.value.__cause__) == "missing a required argument: 'pos_only'"

    with pytest.raises(MacroEvalError) as e:
        macro_evaluator.evaluate(parse_one("@test_arg_resolution(a1 := 1)"))
    assert str(e.value.__cause__) == "missing a required argument: 'pos_only'"

    with pytest.raises(MacroEvalError) as e:
        macro_evaluator.evaluate(parse_one("@test_arg_resolution(1)"))
    assert str(e.value.__cause__) == "missing a required argument: 'a1'"

    with pytest.raises(MacroEvalError) as e:
        macro_evaluator.evaluate(parse_one("@test_arg_resolution(1, a2 := 2)"))
    assert str(e.value.__cause__) == "missing a required argument: 'a1'"

    with pytest.raises(MacroEvalError) as e:
        macro_evaluator.evaluate(parse_one("@test_arg_resolution(pos_only := 1)"))

    # The CI was failing for Python 3.12 with the latter message, but other versions fail
    # with the former one. This ensures we capture both.
    assert str(e.value.__cause__) in (
        "'pos_only' parameter is positional only, but was passed as a keyword",
        "missing a required argument: 'a1'",
    )

    with pytest.raises(MacroEvalError) as e:
        macro_evaluator.evaluate(parse_one("@test_arg_resolution(1, 2, 3)"))
    assert str(e.value.__cause__) == "too many positional arguments"


def test_macro_metadata_flag():
    @macro()
    def noop(evaluator) -> None:
        return None

    @macro(metadata_only=True)
    def noop_metadata_only(evaluator) -> None:
        return None

    assert not hasattr(noop, c.SQLMESH_METADATA)
    assert not macro.get_registry()["noop"].metadata_only

    assert getattr(noop_metadata_only, c.SQLMESH_METADATA) is True
    assert macro.get_registry()["noop_metadata_only"].metadata_only is True


def test_macro_first_value_ignore_respect_nulls(assert_exp_eq) -> None:
    schema = MappingSchema({}, dialect="duckdb")
    evaluator = MacroEvaluator(schema=schema, dialect="duckdb")

    evaluator.evaluate(t.cast(d.MacroDef, d.parse_one("@DEF(test, x -> x)")))

    expected_sql = "SELECT FIRST_VALUE(x IGNORE NULLS) OVER (ORDER BY y NULLS FIRST) AS column_test"
    actual_expr = d.parse_one(
        "SELECT FIRST_VALUE(@test(x) IGNORE NULLS) OVER (ORDER BY y) AS column_test"
    )
    assert_exp_eq(evaluator.transform(actual_expr), expected_sql, dialect="duckdb")

    expected_sql = (
        "SELECT FIRST_VALUE(x RESPECT NULLS) OVER (ORDER BY y NULLS FIRST) AS column_test"
    )
    actual_expr = d.parse_one(
        "SELECT FIRST_VALUE(@test(x) RESPECT NULLS) OVER (ORDER BY y) AS column_test"
    )
    assert_exp_eq(evaluator.transform(actual_expr), expected_sql, dialect="duckdb")
