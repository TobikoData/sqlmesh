import pytest
from sqlglot import Dialect, ParseError, exp, parse_one
from sqlglot.dialects.dialect import NormalizationStrategy

from sqlmesh.core.dialect import (
    JinjaQuery,
    JinjaStatement,
    Model,
    format_model_expressions,
    normalize_model_name,
    parse,
    select_from_values_for_batch_range,
    text_diff,
)
from sqlmesh.core.model import SqlModel, load_sql_based_model


def test_format_model_expressions():
    x = format_model_expressions(
        parse(
            """
    MODEL(
    name a.b,
    kind full,
    references (
     a,
     (b, c) as d,
     )
    )
    ;

    @DEF(x
    , 1);

    SELECT
    *,
    CAST(a AS int),
    CAST(b AS int) AS b,
    CAST(c + 1 AS int) AS c,
    d::int,
    e::int as e,
    (f + 1)::int as f,
    sum(g + 1)::int as g,
    CAST(h AS int), -- h
    CAST(i AS int) AS i, -- i
    CAST(j + 1 AS int) AS j, -- j
    k::int, -- k
    l::int as l, -- l
    (m + 1)::int as m, -- m
    sum(n + 1)::int as n, -- n
    o,
    p + 1,
    CAST(x as int)::int,
    """
        )
    )
    assert (
        x
        == """MODEL (
  name a.b,
  kind FULL,
  references (a, (b, c) AS d)
);

@DEF(x, 1);

SELECT
  *,
  a::INT,
  b::INT AS b,
  CAST(c + 1 AS INT) AS c,
  d::INT,
  e::INT AS e,
  (
    f + 1
  )::INT AS f,
  SUM(g + 1)::INT AS g,
  h::INT, /* h */
  i::INT AS i, /* i */
  CAST(j + 1 AS INT) AS j, /* j */
  k::INT, /* k */
  l::INT AS l, /* l */
  (
    m + 1
  )::INT AS m, /* m */
  SUM(n + 1)::INT AS n, /* n */
  o,
  p + 1,
  x::INT::INT"""
    )

    x = format_model_expressions(
        parse(
            """
            MODEL(name a.b, kind FULL);
            JINJA_QUERY_BEGIN; /* comment */ SELECT * FROM x WHERE y = {{ 1 }}; /* comment */ JINJA_END;"""
        )
    )
    assert (
        x
        == """MODEL (
  name a.b,
  kind FULL
);

JINJA_QUERY_BEGIN;
/* comment */ SELECT * FROM x WHERE y = {{ 1 }}; /* comment */
JINJA_END;"""
    )


def test_macro_format():
    assert parse_one("@EACH(ARRAY(1,2), x -> x)").sql() == "@EACH(ARRAY(1, 2), x -> x)"


def test_format_body_macros():
    assert (
        format_model_expressions(
            parse(
                """
    Model ( name foo );
    @WITH(TRUE) x AS (SELECT 1)
    SELECT col::int
    FROM foo @ORDER_BY(@include_order_by)
    @EACH( @columns,
    item -> @'@iteaoeuatnoehutoenahuoanteuhonateuhaoenthuaoentuhaeotnhaoem'),      @'@foo'
    """
            )
        )
        == """MODEL (
  name foo
);

@WITH(TRUE) x AS (
  SELECT
    1
)
SELECT
  col::INT
FROM foo
@ORDER_BY(@include_order_by)
  @EACH(@columns, item -> @'@iteaoeuatnoehutoenahuoanteuhonateuhaoenthuaoentuhaeotnhaoem'),
  @'@foo'"""
    )


def test_text_diff():
    assert """@@ -1,3 +1,3 @@

 SELECT
-  *
-FROM x
+  1
+FROM y""" in text_diff(
        parse("SELECT * FROM x"), parse("SELECT 1 FROM y")
    )


def test_parse():
    expressions = parse(
        """
        MODEL (
            kind full,
            dialect "hive",
        );

        CACHE TABLE x as SELECT 1 as Y;

        JINJA_QUERY_BEGIN;

        SELECT * FROM x WHERE y = {{ 1 }} ;

        JINJA_END;

        JINJA_STATEMENT_BEGIN;

        {{ side_effect() }};

        JINJA_END;
    """
    )

    assert len(expressions) == 4
    assert isinstance(expressions[0], Model)
    assert isinstance(expressions[1], exp.Cache)
    assert isinstance(expressions[2], JinjaQuery)
    assert isinstance(expressions[3], JinjaStatement)

    assert parse_one("{'a': 1}", read="duckdb").sql(dialect="duckdb") == "{'a': 1}"

    assert parse_one("metric") == exp.column("metric")
    assert parse_one("model(1, 2, 3)") == exp.func("model", 1, 2, 3)

    expressions = parse(
        """
        MODEL (
            kind full,
            dialect duckdb,
            grain metric,
        );

        SELECT 1 AS metric
        """
    )
    assert len(expressions) == 2
    assert isinstance(expressions[0], Model)
    assert isinstance(expressions[1], exp.Select)
    assert expressions[0].expressions[2].args["value"] == exp.to_identifier("metric")


def test_parse_jinja_with_semicolons():
    expressions = parse(
        """
        CREATE TABLE a as SELECT 1;
        CREATE TABLE b as SELECT 1;

        JINJA_STATEMENT_BEGIN;

        {% call set_sql_header(config) %}
            CREATE OR REPLACE TEMP MACRO add(a, b) AS a + b;
        {%- endcall %}

        JINJA_END;

        DROP TABLE a;
        DROP TABLE b;
    """
    )

    assert len(expressions) == 5
    assert isinstance(expressions[0], exp.Create)
    assert isinstance(expressions[1], exp.Create)
    assert isinstance(expressions[2], JinjaStatement)
    assert isinstance(expressions[3], exp.Drop)
    assert isinstance(expressions[4], exp.Drop)


def test_seed():
    expressions = parse(
        """
        MODEL (
            kind SEED (
                path '..\..\..\data\data.csv',
            ),
        );
    """
    )
    assert len(expressions) == 1
    assert "../../../data/data.csv" in expressions[0].sql()


def test_select_from_values_for_batch_range_json():
    values = [(1, "2022-01-01", '{"foo":"bar"}'), (2, "2022-01-01", '{"foo":"qaz"}')]
    columns_to_types = {
        "id": exp.DataType.build("int"),
        "ds": exp.DataType.build("text"),
        "json_col": exp.DataType.build("json"),
    }

    assert select_from_values_for_batch_range(values, columns_to_types, 0, len(values)).sql() == (
        """SELECT CAST(id AS INT) AS id, CAST(ds AS TEXT) AS ds, CAST(json_col AS JSON) AS json_col """
        """FROM """
        """(VALUES (1, '2022-01-01', PARSE_JSON('{"foo":"bar"}')), (2, '2022-01-01', PARSE_JSON('{"foo":"qaz"}'))) """
        """AS t(id, ds, json_col)"""
    )

    assert select_from_values_for_batch_range([], columns_to_types, 0, 0).sql() == (
        "SELECT CAST(id AS INT) AS id, CAST(ds AS TEXT) AS ds, CAST(json_col AS JSON) AS json_col "
        "FROM (VALUES (CAST(NULL AS INT), CAST(NULL AS TEXT), CAST(NULL AS JSON))) AS t(id, ds, json_col) WHERE FALSE"
    )


@pytest.fixture(params=["mysql", "duckdb", "postgres", "snowflake"])
def normalization_dialect(request):
    if request.param == "duckdb":
        assert Dialect["duckdb"].NORMALIZATION_STRATEGY == NormalizationStrategy.CASE_INSENSITIVE
    elif request.param == "mysql":
        assert Dialect["mysql"].NORMALIZATION_STRATEGY == NormalizationStrategy.CASE_SENSITIVE
    elif request.param == "snowflake":
        assert Dialect["snowflake"].NORMALIZATION_STRATEGY == NormalizationStrategy.UPPERCASE
    elif request.param == "postgres":
        assert Dialect["postgres"].NORMALIZATION_STRATEGY == NormalizationStrategy.LOWERCASE
    return request.param


normalization_tests_fields = (
    "table, default_catalog, case_sensitive, case_insensitive, lowercase, uppercase"
)
normalization_tests = [
    ("table", None, '"table"', '"table"', '"table"', '"TABLE"'),
    ("db.table", None, '"db"."table"', '"db"."table"', '"db"."table"', '"DB"."TABLE"'),
    (
        "catalog.db.table",
        None,
        '"catalog"."db"."table"',
        '"catalog"."db"."table"',
        '"catalog"."db"."table"',
        '"CATALOG"."DB"."TABLE"',
    ),
    ("table", "catalog", '"table"', '"table"', '"table"', '"TABLE"'),
    (
        "db.table",
        "catalog",
        '"catalog"."db"."table"',
        '"catalog"."db"."table"',
        '"catalog"."db"."table"',
        '"CATALOG"."DB"."TABLE"',
    ),
    (
        "DB.TABLE",
        "CATALOG",
        '"CATALOG"."DB"."TABLE"',
        '"catalog"."db"."table"',
        '"catalog"."db"."table"',
        '"CATALOG"."DB"."TABLE"',
    ),
    ("tAble", None, '"tAble"', '"table"', '"table"', '"TABLE"'),
    (
        "Db.tAble",
        "CaTalog",
        '"CaTalog"."Db"."tAble"',
        '"catalog"."db"."table"',
        '"catalog"."db"."table"',
        '"CATALOG"."DB"."TABLE"',
    ),
    ('"tAble"', None, '"tAble"', '"table"', '"tAble"', '"tAble"'),
    (
        'Db."tAble"',
        '"CaTalog"',
        '"CaTalog"."Db"."tAble"',
        '"catalog"."db"."table"',
        '"CaTalog"."db"."tAble"',
        '"CaTalog"."DB"."tAble"',
    ),
]


@pytest.mark.parametrize(normalization_tests_fields, normalization_tests)
def test_normalize_model_name(
    table,
    default_catalog,
    case_sensitive,
    case_insensitive,
    lowercase,
    uppercase,
    normalization_dialect,
):
    if Dialect[normalization_dialect].NORMALIZATION_STRATEGY == NormalizationStrategy.UPPERCASE:
        expected = uppercase
    elif (
        Dialect[normalization_dialect].NORMALIZATION_STRATEGY
        == NormalizationStrategy.CASE_SENSITIVE
    ):
        expected = case_sensitive
    elif (
        Dialect[normalization_dialect].NORMALIZATION_STRATEGY
        == NormalizationStrategy.CASE_INSENSITIVE
    ):
        expected = case_insensitive
    else:
        expected = lowercase
    assert normalize_model_name(table, default_catalog, normalization_dialect) == expected


@pytest.mark.parametrize(normalization_tests_fields, normalization_tests)
def test_multiple_normalization(
    table,
    default_catalog,
    case_sensitive,
    case_insensitive,
    lowercase,
    uppercase,
    normalization_dialect,
):
    if Dialect[normalization_dialect].NORMALIZATION_STRATEGY == NormalizationStrategy.UPPERCASE:
        expected = uppercase
    elif (
        Dialect[normalization_dialect].NORMALIZATION_STRATEGY
        == NormalizationStrategy.CASE_SENSITIVE
    ):
        expected = case_sensitive
    elif (
        Dialect[normalization_dialect].NORMALIZATION_STRATEGY
        == NormalizationStrategy.CASE_INSENSITIVE
    ):
        expected = case_insensitive
    else:
        expected = lowercase
    kwargs = {"default_catalog": default_catalog, "dialect": normalization_dialect}
    assert (
        normalize_model_name(
            normalize_model_name(normalize_model_name(table, **kwargs), **kwargs), **kwargs
        )
        == expected
    )


@pytest.mark.parametrize(normalization_tests_fields, normalization_tests)
def test_model_normalization_multiple_serde(
    table,
    default_catalog,
    case_sensitive,
    case_insensitive,
    lowercase,
    uppercase,
    normalization_dialect,
):
    if Dialect[normalization_dialect].NORMALIZATION_STRATEGY == NormalizationStrategy.UPPERCASE:
        expected = uppercase
    elif (
        Dialect[normalization_dialect].NORMALIZATION_STRATEGY
        == NormalizationStrategy.CASE_SENSITIVE
    ):
        expected = case_sensitive
    elif (
        Dialect[normalization_dialect].NORMALIZATION_STRATEGY
        == NormalizationStrategy.CASE_INSENSITIVE
    ):
        expected = case_insensitive
    else:
        expected = lowercase
    expressions = parse(
        f"""
        MODEL (
            name {table},
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column ds
            ),
            dialect {normalization_dialect},
        );

        SELECT col::text, ds::text
    """
    )
    model = load_sql_based_model(
        expressions, time_column_format="%Y", default_catalog=default_catalog
    )
    assert model.fqn == expected
    # double serialization to ensure even multiple passes don't change results
    model_serialized = SqlModel.parse_raw(SqlModel.parse_raw(model.json()).json())
    assert model_serialized.fqn == expected


def test_model_normalization_quote_flexibility():
    assert (
        normalize_model_name("`catalog`.`db`.`table`", default_catalog=None, dialect="spark")
        == '"catalog"."db"."table"'
    )
    # This takes advantage of the fact that although double quotes ('"') aren't valid quotes in spark, sqlglot still allows it
    assert (
        normalize_model_name('"catalog"."db"."table"', default_catalog=None, dialect="spark")
        == '"catalog"."db"."table"'
    )
    # It doesn't work the other way which is what we currently expect
    with pytest.raises(ParseError):
        normalize_model_name("`catalog`.`db`.`table`", default_catalog=None, dialect=None)
