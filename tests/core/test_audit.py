import pytest
from sqlglot import exp, parse, parse_one

from sqlmesh.core.audit import Audit, builtin
from sqlmesh.core.model import IncrementalByTimeRangeKind, Model, create_sql_model
from sqlmesh.utils.errors import AuditConfigError


@pytest.fixture
def model() -> Model:
    return create_sql_model(
        "db.test_model",
        parse_one("SELECT a, b, ds"),
        kind=IncrementalByTimeRangeKind(time_column="ds"),
    )


def test_load(assert_exp_eq):
    expressions = parse(
        """
        Audit (
            name my_audit,
            dialect spark,
            blocking false,
        );

        SELECT
            *
        FROM
            db.table
        WHERE
            col IS NULL
    """
    )

    audit = Audit.load(expressions, path="/path/to/audit", dialect="duckdb")
    assert audit.dialect == "spark"
    assert audit.blocking is False
    assert audit.skip is False
    assert_exp_eq(
        audit.query,
        """
    SELECT
        *
    FROM
        db.table
    WHERE
        col IS NULL
    """,
    )


def test_load_multiple(assert_exp_eq):
    expressions = parse(
        """
        Audit (
            name first_audit,
            dialect spark,
        );

        SELECT *
        FROM db.table
        WHERE col1 IS NULL;

        Audit (
            name second_audit,
            dialect duckdb,
            blocking false,
        );

        SELECT *
        FROM db.table
        WHERE col2 IS NULL;
    """
    )

    first_audit, second_audit = Audit.load_multiple(expressions, path="/path/to/audit")
    assert first_audit.dialect == "spark"
    assert first_audit.blocking is True
    assert first_audit.skip is False
    assert_exp_eq(
        first_audit.query,
        """
    SELECT *
    FROM db.table
    WHERE col1 IS NULL
    """,
    )

    assert second_audit.dialect == "duckdb"
    assert second_audit.blocking is False
    assert second_audit.skip is False
    assert_exp_eq(
        second_audit.query,
        """
    SELECT *
    FROM db.table
    WHERE col2 IS NULL
    """,
    )


def test_no_audit_statement():
    expressions = parse(
        """
        SELECT 1
    """
    )
    with pytest.raises(AuditConfigError) as ex:
        Audit.load(expressions, path="/path/to/audit", dialect="duckdb")
    assert "Incomplete audit definition" in str(ex.value)


def test_unordered_audit_statements():
    expressions = parse(
        """
        SELECT 1;

        AUDIT (
            name my_audit,
        );
    """
    )

    with pytest.raises(AuditConfigError) as ex:
        Audit.load(expressions, path="/path/to/audit", dialect="duckdb")
    assert "AUDIT statement is required as the first statement" in str(ex.value)


def test_no_query():
    expressions = parse(
        """
        AUDIT (
            name my_audit,
        );

        @DEF(x, 1)
    """
    )

    with pytest.raises(AuditConfigError) as ex:
        Audit.load(expressions, path="/path/to/audit", dialect="duckdb")
    assert "Missing SELECT query" in str(ex.value)


def test_macro(model: Model):
    expected_query = """SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" BETWEEN '1970-01-01' AND '1970-01-01') AS "_q_0" WHERE "a" IS NULL"""

    audit = Audit(
        name="test_audit",
        query="SELECT * FROM @this_model WHERE a IS NULL",
    )

    audit_jinja = Audit(
        name="test_audit",
        query="JINJA_QUERY_BEGIN; SELECT * FROM {{ this_model }} WHERE a IS NULL; JINJA_END;",
    )

    assert audit.render_query(model).sql() == expected_query
    assert audit_jinja.render_query(model).sql() == expected_query


def test_load_with_defaults(model: Model, assert_exp_eq):
    expressions = parse(
        """
        Audit (
            name my_audit,
            defaults (
                field1 = some_column,
                field2 = 3,
                field3 = other_column,
                field4 = 'some string'
            )
        );

        SELECT
            *
        FROM
            db.table
        WHERE 1=1
            AND @IF(@field4 = 'overridden', @field4 IN ('some string', 'other string'), 1=1)
            AND @field1 = @field2 
            AND @field3 != @field4
    """
    )
    audit = Audit.load(expressions, path="/path/to/audit", dialect="duckdb")
    assert audit.defaults == {
        "field1": exp.to_column("some_column"),
        "field2": exp.Literal.number(3),
        "field3": exp.to_column("other_column"),
        "field4": exp.Literal.string("some string"),
    }
    assert_exp_eq(
        audit.render_query(model, field4=exp.Literal.string("overridden")),
        'SELECT * FROM "db"."table" AS "table" WHERE "other_column" <> \'overridden\' AND "some_column" = 3 AND \'overridden\' IN (\'some string\', \'other string\')',
    )


def test_not_null_audit(model: Model):
    rendered_query_a = builtin.not_null_audit.render_query(
        model,
        columns=[exp.to_column("a")],
    )
    assert (
        rendered_query_a.sql()
        == """SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" BETWEEN '1970-01-01' AND '1970-01-01') AS "_q_0" WHERE "a" IS NULL"""
    )

    rendered_query_a_and_b = builtin.not_null_audit.render_query(
        model,
        columns=[exp.to_column("a"), exp.to_column("b")],
    )
    assert (
        rendered_query_a_and_b.sql()
        == """SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" BETWEEN '1970-01-01' AND '1970-01-01') AS "_q_0" WHERE "a" IS NULL OR "b" IS NULL"""
    )


def test_unique_values_audit(model: Model):
    rendered_query_a = builtin.unique_values_audit.render_query(model, columns=[exp.to_column("a")])
    assert (
        rendered_query_a.sql()
        == """SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY "a" ORDER BY 1) AS "a_rank" FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" BETWEEN '1970-01-01' AND '1970-01-01') AS "_q_0") AS "_q_1" WHERE "a_rank" > 1"""
    )

    rendered_query_a_and_b = builtin.unique_values_audit.render_query(
        model, columns=[exp.to_column("a"), exp.to_column("b")]
    )
    assert (
        rendered_query_a_and_b.sql()
        == """SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY "a" ORDER BY 1) AS "a_rank", ROW_NUMBER() OVER (PARTITION BY "b" ORDER BY 1) AS "b_rank" FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" BETWEEN '1970-01-01' AND '1970-01-01') AS "_q_0") AS "_q_1" WHERE "a_rank" > 1 OR "b_rank" > 1"""
    )


def test_accepted_values_audit(model: Model):
    rendered_query = builtin.accepted_values_audit.render_query(
        model,
        column=exp.to_column("a"),
        is_in=["value_a", "value_b"],
    )
    assert (
        rendered_query.sql()
        == """SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" BETWEEN '1970-01-01' AND '1970-01-01') AS "_q_0" WHERE NOT "a" IN ('value_a', 'value_b')"""
    )


def test_number_of_rows_audit(model: Model):
    rendered_query = builtin.number_of_rows_audit.render_query(
        model,
        threshold=0,
    )
    assert (
        rendered_query.sql()
        == """SELECT 1 AS "1" FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" BETWEEN '1970-01-01' AND '1970-01-01') AS "_q_0" HAVING COUNT(*) <= 0 LIMIT 0 + 1"""
    )


def test_forall_audit(model: Model):
    rendered_query_a = builtin.forall_audit.render_query(
        model,
        criteria=[parse_one("a >= b")],
    )
    assert (
        rendered_query_a.sql()
        == """SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" BETWEEN '1970-01-01' AND '1970-01-01') AS "_q_0" WHERE NOT ("a" >= "b")"""
    )

    rendered_query_a = builtin.forall_audit.render_query(
        model,
        criteria=[parse_one("a >= b"), parse_one("c + d - e < 1.0")],
    )
    assert (
        rendered_query_a.sql()
        == """SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" BETWEEN '1970-01-01' AND '1970-01-01') AS "_q_0" WHERE NOT ("a" >= "b") OR NOT ("c" + "d" - "e" < 1.0)"""
    )


def test_accepted_range_audit(model: Model):
    rendered_query = builtin.accepted_range_audit.render_query(
        model, column=exp.to_column("a"), min_v=0
    )
    assert (
        rendered_query.sql()
        == 'SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= \'1970-01-01\' AND "ds" >= \'1970-01-01\') AS "_q_0" WHERE "a" <= 0'
    )
    rendered_query = builtin.accepted_range_audit.render_query(
        model, column=exp.to_column("a"), max_v=100, inclusive=exp.false()
    )
    assert (
        rendered_query.sql()
        == 'SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= \'1970-01-01\' AND "ds" >= \'1970-01-01\') AS "_q_0" WHERE "a" > 100'
    )
    rendered_query = builtin.accepted_range_audit.render_query(
        model, column=exp.to_column("a"), min_v=100, max_v=100
    )
    assert (
        rendered_query.sql()
        == 'SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= \'1970-01-01\' AND "ds" >= \'1970-01-01\') AS "_q_0" WHERE "a" <= 100 OR "a" >= 100'
    )


def test_at_least_one_audit(model: Model):
    rendered_query = builtin.at_least_one_audit.render_query(
        model,
        column=exp.to_column("a"),
    )
    assert (
        rendered_query.sql()
        == 'WITH "src" AS (SELECT COUNT(*) AS "cnt_nulls" FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= \'1970-01-01\' AND "ds" >= \'1970-01-01\') AS "_q_0" WHERE "a" IS NULL), "tgt" AS (SELECT COUNT(*) AS "cnt_tot" FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= \'1970-01-01\' AND "ds" >= \'1970-01-01\') AS "_q_1") SELECT * FROM "src" INNER JOIN "tgt" ON "src"."cnt_nulls" = "tgt"."cnt_tot"'
    )


def test_relationship_audit(model: Model):
    rendered_query = builtin.relationship_audit.render_query(
        model,
        source_column=exp.to_column("a"),
        target_column=exp.to_column("a"),
        to=exp.to_table("db.test_model"),
    )
    assert (
        rendered_query.sql()
        == 'SELECT "child"."source_column" AS "source_column" FROM (SELECT "a" AS "source_column" FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= \'1970-01-01\' AND "ds" >= \'1970-01-01\') AS "_q_0" WHERE NOT "a" IS NULL) AS "child" LEFT JOIN (SELECT "a" AS "target_column" FROM "db"."test_model" AS "test_model") AS "parent" ON "child"."source_column" = "parent"."target_column" WHERE "parent"."target_column" IS NULL'
    )


def test_mutually_exclusive_ranges_audit(model: Model):
    rendered_query = builtin.mutually_exclusive_ranges_audit.render_query(
        model,
        lower_bound_column=exp.to_column("a"),
        upper_bound_column=exp.to_column("a"),
    )
    assert (
        rendered_query.sql()
        == 'WITH "window_functions" AS (SELECT "a" AS "lower_bound", "a" AS "upper_bound", LEAD("a") OVER (ORDER BY "a", "a") AS "next_lower_bound", ROW_NUMBER() OVER (ORDER BY "a" DESC, "a" DESC) = 1 AS "is_last_record" FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= \'1970-01-01\' AND "ds" >= \'1970-01-01\') AS "_q_0"), "calc" AS (SELECT *, COALESCE("lower_bound" <= "upper_bound", FALSE) AS "lower_bound_comp_upper_bound", COALESCE("upper_bound" <= "next_lower_bound", "is_last_record", FALSE) AS "upper_bound_comp_next_lower_bound" FROM "window_functions"), "validation_errors" AS (SELECT * FROM "calc" WHERE NOT "lower_bound_comp_upper_bound" OR NOT "upper_bound_comp_next_lower_bound") SELECT * FROM "validation_errors"'
    )
