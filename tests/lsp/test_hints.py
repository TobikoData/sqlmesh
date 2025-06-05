"""Tests for type hinting SQLMesh models"""

import pytest

from sqlglot import exp, parse_one

from sqlmesh.core.context import Context
from sqlmesh.lsp.context import LSPContext, ModelTarget
from sqlmesh.lsp.hints import get_hints, _get_type_hints_for_model_from_query
from sqlmesh.lsp.uri import URI


@pytest.mark.fast
def test_hints() -> None:
    context = Context(paths=["examples/sushi"])
    lsp_context = LSPContext(context)

    # Find model URIs
    active_customers_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.active_customers" in info.names
    )
    customer_revenue_lifetime_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customer_revenue_lifetime" in info.names
    )
    customer_revenue_by_day_path = next(
        path
        for path, info in lsp_context.map.items()
        if isinstance(info, ModelTarget) and "sushi.customer_revenue_by_day" in info.names
    )

    active_customers_uri = URI.from_path(active_customers_path)
    ac_hints = get_hints(lsp_context, active_customers_uri, start_line=0, end_line=9999)
    assert len(ac_hints) == 2
    assert ac_hints[0].label == "::INT"
    assert ac_hints[1].label == "::TEXT"

    customer_revenue_lifetime_uri = URI.from_path(customer_revenue_lifetime_path)
    crl_hints = get_hints(
        lsp_context=lsp_context,
        document_uri=customer_revenue_lifetime_uri,
        start_line=0,
        end_line=9999,
    )
    assert len(crl_hints) == 3
    assert crl_hints[0].label == "::INT"
    assert crl_hints[1].label == "::DOUBLE"
    assert crl_hints[2].label == "::DATE"

    customer_revenue_by_day_uri = URI.from_path(customer_revenue_by_day_path)
    crbd_hints = get_hints(
        lsp_context=lsp_context,
        document_uri=customer_revenue_by_day_uri,
        start_line=0,
        end_line=9999,
    )
    assert len(crbd_hints) == 1
    assert crbd_hints[0].label == "::INT"


@pytest.mark.fast
def test_union_hints() -> None:
    query_str = """SELECT a FROM table_a UNION SELECT b FROM table_b UNION SELECT c FROM table_c"""
    query = parse_one(query_str, dialect="postgres")

    result = _get_type_hints_for_model_from_query(
        query=query,
        dialect="postgres",
        columns_to_types={
            "a": exp.DataType.build("TEXT"),
            "b": exp.DataType.build("INT"),
            "c": exp.DataType.build("DATE"),
        },
        start_line=0,
        end_line=1,
    )

    assert len(result) == 3
    assert result[0].label == "::DATE"
    assert result[1].label == "::TEXT"
    assert result[2].label == "::INT"


@pytest.mark.fast
def test_complex_hints() -> None:
    query = parse_one("SELECT a, b FROM c", dialect="postgres")

    result = _get_type_hints_for_model_from_query(
        query=query,
        dialect="postgres",
        columns_to_types={
            "a": exp.DataType.build("VARCHAR(100)"),
            "b": exp.DataType.build("STRUCT<INT, STRUCT<TEXT, ARRAY<INT>>>"),
        },
        start_line=0,
        end_line=1,
    )

    assert len(result) == 2
    assert result[0].label == "::VARCHAR(100)"
    assert result[1].label == "::STRUCT<INT, STRUCT<TEXT, INT[]>>"


@pytest.mark.fast
def test_simple_cast_hints() -> None:
    """Don't add type hints if the expression is already a cast"""
    query = parse_one("SELECT a::INT, CAST(b AS DATE), c FROM d", dialect="postgres")

    result = _get_type_hints_for_model_from_query(
        query=query,
        dialect="postgres",
        columns_to_types={
            "a": exp.DataType.build("INT"),
            "b": exp.DataType.build("DATE"),
            "c": exp.DataType.build("TEXT"),
        },
        start_line=0,
        end_line=1,
    )

    assert len(result) == 1
    assert result[0].label == "::TEXT"


@pytest.mark.fast
def test_alias_cast_hints() -> None:
    """Don't add type hints if the expression is already a cast"""
    query = parse_one(
        "SELECT raw_a::INT AS a, CAST(raw_b AS DATE) AS b, c FROM d", dialect="postgres"
    )

    result = _get_type_hints_for_model_from_query(
        query=query,
        dialect="postgres",
        columns_to_types={
            "a": exp.DataType.build("INT"),
            "b": exp.DataType.build("DATE"),
            "c": exp.DataType.build("TEXT"),
        },
        start_line=0,
        end_line=1,
    )

    assert len(result) == 1
    assert result[0].label == "::TEXT"


@pytest.mark.fast
def test_simple_cte_hints() -> None:
    """Don't add type hints if the expression is already a cast"""
    query = parse_one("WITH t AS (SELECT a FROM b) SELECT a AS c FROM t", dialect="postgres")

    result = _get_type_hints_for_model_from_query(
        query=query,
        dialect="postgres",
        columns_to_types={
            "c": exp.DataType.build("INT"),
        },
        start_line=0,
        end_line=1,
    )

    assert len(result) == 1
    assert result[0].label == "::INT"


@pytest.mark.fast
def test_cte_with_union_hints() -> None:
    """Don't add type hints if the expression is already a cast"""
    query = parse_one(
        """WITH x AS (SELECT a FROM t),
                y AS (SELECT b FROM t),
                z AS (SELECT c FROM t)
         SELECT a AS d FROM x
          UNION
         SELECT b AS e FROM y
          UNION
         SELECT c AS f FROM z""",
        dialect="postgres",
    )

    result = _get_type_hints_for_model_from_query(
        query=query,
        dialect="postgres",
        columns_to_types={
            "a": exp.DataType.build("TEXT"),
            "b": exp.DataType.build("DATE"),
            "c": exp.DataType.build("INT"),
            "d": exp.DataType.build("TEXT"),
            "e": exp.DataType.build("DATE"),
            "f": exp.DataType.build("INT"),
        },
        start_line=0,
        end_line=9999,
    )

    assert len(result) == 3
    assert result[0].label == "::INT"
    assert result[1].label == "::TEXT"
    assert result[2].label == "::DATE"
