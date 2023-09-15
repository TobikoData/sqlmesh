from sqlglot import exp, parse_one

from sqlmesh.core.dialect import (
    JinjaQuery,
    JinjaStatement,
    Model,
    format_model_expressions,
    parse,
    select_from_values_for_batch_range,
    text_diff,
)


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
  a::INT AS a,
  b::INT AS b,
  CAST(c + 1 AS INT) AS c,
  d::INT AS d,
  e::INT AS e,
  (
    f + 1
  )::INT AS f,
  SUM(g + 1)::INT AS g,
  h::INT AS h, /* h */
  i::INT AS i, /* i */
  CAST(j + 1 AS INT) AS j, /* j */
  k::INT AS k, /* k */
  l::INT AS l, /* l */
  (
    m + 1
  )::INT AS m, /* m */
  SUM(n + 1)::INT AS n, /* n */
  o AS o,
  p + 1"""
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
  col::INT AS col
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
        parse_one("SELECT * FROM x"), parse_one("SELECT 1 FROM y")
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


def select_from_values_for_batch_range_json():
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
