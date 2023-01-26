import pytest
from sqlglot import parse, parse_one

from sqlmesh.core.audit import Audit, column
from sqlmesh.core.model import Model, create_sql_model
from sqlmesh.utils.errors import AuditConfigError


@pytest.fixture
def model() -> Model:
    return create_sql_model("db.test_model", parse_one("SELECT a, b, ds"), [])


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
    expected_query = "SELECT * FROM db.test_model WHERE a IS NULL AND ds <= '1970-01-01' AND ds >= '1970-01-01'"

    audit = Audit(
        name="test_audit",
        query="SELECT * FROM @this_model WHERE a IS NULL",
    )

    audit_jinja = Audit(
        name="test_audit",
        query="SELECT * FROM {{ this_model }} WHERE a IS NULL",
    )

    assert audit.render_query(model).sql() == expected_query
    assert audit_jinja.render_query(model).sql() == expected_query


def test_not_null_audit(model: Model):
    rendered_query_a = column.not_null_audit.render_query(
        model,
        columns=["a"],
    )
    assert (
        rendered_query_a.sql()
        == "SELECT * FROM db.test_model WHERE a IS NULL AND ds <= '1970-01-01' AND ds >= '1970-01-01'"
    )

    rendered_query_a_and_b = column.not_null_audit.render_query(
        model,
        columns=["a", "b"],
    )
    assert (
        rendered_query_a_and_b.sql()
        == "SELECT * FROM db.test_model WHERE (a IS NULL OR b IS NULL) AND ds <= '1970-01-01' AND ds >= '1970-01-01'"
    )
