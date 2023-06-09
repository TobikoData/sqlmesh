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
    expected_query = """SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= '1970-01-01' AND "ds" >= '1970-01-01') AS "_q_0" WHERE "a" IS NULL"""

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


def test_not_null_audit(model: Model):
    rendered_query_a = builtin.not_null_audit.render_query(
        model,
        columns=[exp.to_column("a")],
    )
    assert (
        rendered_query_a.sql()
        == """SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= '1970-01-01' AND "ds" >= '1970-01-01') AS "_q_0" WHERE "a" IS NULL"""
    )

    rendered_query_a_and_b = builtin.not_null_audit.render_query(
        model,
        columns=[exp.to_column("a"), exp.to_column("b")],
    )
    assert (
        rendered_query_a_and_b.sql()
        == """SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= '1970-01-01' AND "ds" >= '1970-01-01') AS "_q_0" WHERE "a" IS NULL OR "b" IS NULL"""
    )


def test_unique_values_audit(model: Model):
    rendered_query_a = builtin.unique_values_audit.render_query(model, columns=[exp.to_column("a")])
    assert (
        rendered_query_a.sql()
        == """SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY "a" ORDER BY 1) AS "a_rank" FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= '1970-01-01' AND "ds" >= '1970-01-01') AS "_q_0") AS "_q_1" WHERE "a_rank" > 1"""
    )

    rendered_query_a_and_b = builtin.unique_values_audit.render_query(
        model, columns=[exp.to_column("a"), exp.to_column("b")]
    )
    assert (
        rendered_query_a_and_b.sql()
        == """SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY "a" ORDER BY 1) AS "a_rank", ROW_NUMBER() OVER (PARTITION BY "b" ORDER BY 1) AS "b_rank" FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= '1970-01-01' AND "ds" >= '1970-01-01') AS "_q_0") AS "_q_1" WHERE "a_rank" > 1 OR "b_rank" > 1"""
    )


def test_accepted_values_audit(model: Model):
    rendered_query = builtin.accepted_values_audit.render_query(
        model,
        column=exp.to_column("a"),
        is_in=["value_a", "value_b"],
    )
    assert (
        rendered_query.sql()
        == """SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= '1970-01-01' AND "ds" >= '1970-01-01') AS "_q_0" WHERE NOT "a" IN ('value_a', 'value_b')"""
    )


def test_number_of_rows_audit(model: Model):
    rendered_query = builtin.number_of_rows_audit.render_query(
        model,
        threshold=0,
    )
    assert (
        rendered_query.sql()
        == """SELECT 1 AS "1" FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= '1970-01-01' AND "ds" >= '1970-01-01') AS "_q_0" HAVING COUNT(*) <= 0 LIMIT 1"""
    )


def test_forall_audit(model: Model):
    rendered_query_a = builtin.forall_audit.render_query(
        model,
        criteria=[parse_one("a >= b")],
    )
    assert (
        rendered_query_a.sql()
        == '''SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= '1970-01-01' AND "ds" >= '1970-01-01') AS "_q_0" WHERE NOT "a" >= "b"'''
    )

    rendered_query_a = builtin.forall_audit.render_query(
        model,
        criteria=[parse_one("a >= b"), parse_one("c + d - e < 1.0")],
    )
    assert (
        rendered_query_a.sql()
        == """SELECT * FROM (SELECT * FROM "db"."test_model" AS "test_model" WHERE "ds" <= '1970-01-01' AND "ds" >= '1970-01-01') AS "_q_0" WHERE NOT "a" >= "b" OR NOT "c" + "d" - "e" < 1.0"""
    )
