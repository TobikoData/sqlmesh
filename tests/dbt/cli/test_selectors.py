import typing as t
import pytest
from sqlmesh_dbt import selectors
from sqlmesh.core.context import Context
from pathlib import Path


@pytest.mark.parametrize(
    "dbt_select,expected",
    [
        ([], None),
        (["main.model_a"], "main.model_a"),
        (["main.model_a main.model_b"], "main.model_a | main.model_b"),
        (["main.model_a", "main.model_b"], "main.model_a | main.model_b"),
        (["(main.model_a & ^main.model_b)"], "(main.model_a & ^main.model_b)"),
        (
            ["(+main.model_a & ^main.model_b)", "main.model_c"],
            "(+main.model_a & ^main.model_b) | main.model_c",
        ),
    ],
)
def test_selection(dbt_select: t.List[str], expected: t.Optional[str]):
    assert selectors.to_sqlmesh(dbt_select=dbt_select, dbt_exclude=[]) == expected


@pytest.mark.parametrize(
    "dbt_exclude,expected",
    [
        ([], None),
        (["main.model_a"], "^(main.model_a)"),
        (["(main.model_a & main.model_b)"], "^(main.model_a & main.model_b)"),
        (["main.model_a,main.model_b"], "^(main.model_a & main.model_b)"),
        (["main.model_a +main.model_b"], "^(main.model_a | +main.model_b)"),
        (
            ["(+main.model_a & ^main.model_b)", "main.model_c"],
            "^((+main.model_a & ^main.model_b) | main.model_c)",
        ),
    ],
)
def test_exclusion(dbt_exclude: t.List[str], expected: t.Optional[str]):
    assert selectors.to_sqlmesh(dbt_select=[], dbt_exclude=dbt_exclude) == expected


@pytest.mark.parametrize(
    "dbt_select,dbt_exclude,expected",
    [
        ([], [], None),
        (["+main.model_a"], ["raw.src_data"], "+main.model_a & ^(raw.src_data)"),
        (
            ["+main.model_a", "main.*b+"],
            ["raw.src_data"],
            "(+main.model_a | main.*b+) & ^(raw.src_data)",
        ),
        (
            ["+main.model_a", "main.*b+"],
            ["raw.src_data", "tag:disabled"],
            "(+main.model_a | main.*b+) & ^(raw.src_data | tag:disabled)",
        ),
    ],
)
def test_selection_and_exclusion(
    dbt_select: t.List[str], dbt_exclude: t.List[str], expected: t.Optional[str]
):
    assert selectors.to_sqlmesh(dbt_select=dbt_select, dbt_exclude=dbt_exclude) == expected


@pytest.mark.parametrize(
    "expression,expected",
    [
        ("", ([], [])),
        ("model_a", (["model_a"], [])),
        ("model_a model_b", (["model_a", "model_b"], [])),
        ("model_a,model_b", ([], ["model_a", "model_b"])),
        ("model_a model_b,model_c", (["model_a"], ["model_b", "model_c"])),
        ("model_a,model_b model_c", (["model_c"], ["model_a", "model_b"])),
    ],
)
def test_split_unions_and_intersections(
    expression: str, expected: t.Tuple[t.List[str], t.List[str]]
):
    assert selectors._split_unions_and_intersections(expression) == expected


@pytest.mark.parametrize(
    "dbt_select,expected",
    [
        (["aging"], set()),
        (
            ["staging"],
            {
                '"jaffle_shop"."main"."stg_customers"',
                '"jaffle_shop"."main"."stg_orders"',
                '"jaffle_shop"."main"."stg_payments"',
            },
        ),
        (["staging.stg_customers"], {'"jaffle_shop"."main"."stg_customers"'}),
        (["stg_customers.staging"], set()),
        (
            ["+customers"],
            {
                '"jaffle_shop"."main"."customers"',
                '"jaffle_shop"."main"."stg_customers"',
                '"jaffle_shop"."main"."stg_orders"',
                '"jaffle_shop"."main"."stg_payments"',
                '"jaffle_shop"."main"."raw_customers"',
                '"jaffle_shop"."main"."raw_orders"',
                '"jaffle_shop"."main"."raw_payments"',
            },
        ),
        (["customers+"], {'"jaffle_shop"."main"."customers"'}),
        (
            ["customers+", "stg_orders"],
            {'"jaffle_shop"."main"."customers"', '"jaffle_shop"."main"."stg_orders"'},
        ),
        (["tag:agg"], {'"jaffle_shop"."main"."agg_orders"'}),
        (
            ["staging.stg_customers", "tag:agg"],
            {
                '"jaffle_shop"."main"."stg_customers"',
                '"jaffle_shop"."main"."agg_orders"',
            },
        ),
        (
            ["+tag:agg"],
            {
                '"jaffle_shop"."main"."agg_orders"',
                '"jaffle_shop"."main"."orders"',
                '"jaffle_shop"."main"."stg_orders"',
                '"jaffle_shop"."main"."stg_payments"',
                '"jaffle_shop"."main"."raw_orders"',
                '"jaffle_shop"."main"."raw_payments"',
            },
        ),
        (
            ["tag:agg+"],
            {
                '"jaffle_shop"."main"."agg_orders"',
            },
        ),
    ],
)
def test_select_by_dbt_names(
    jaffle_shop_duckdb: Path,
    jaffle_shop_duckdb_context: Context,
    dbt_select: t.List[str],
    expected: t.Set[str],
):
    (jaffle_shop_duckdb / "models" / "agg_orders.sql").write_text("""
     {{ config(tags=["agg"]) }}
      select order_date, count(*) as num_orders from {{ ref('orders') }}                                                             
    """)

    ctx = jaffle_shop_duckdb_context
    ctx.load()
    assert '"jaffle_shop"."main"."agg_orders"' in ctx.models

    selector = ctx._new_selector()
    assert selector._dbt_mode

    sqlmesh_selector = selectors.to_sqlmesh(dbt_select=dbt_select, dbt_exclude=[])
    assert sqlmesh_selector

    assert selector.expand_model_selections([sqlmesh_selector]) == expected


@pytest.mark.parametrize(
    "dbt_exclude,expected",
    [
        (["jaffle_shop"], set()),
        (
            ["staging"],
            {
                '"jaffle_shop"."main"."agg_orders"',
                '"jaffle_shop"."main"."customers"',
                '"jaffle_shop"."main"."orders"',
                '"jaffle_shop"."main"."raw_customers"',
                '"jaffle_shop"."main"."raw_orders"',
                '"jaffle_shop"."main"."raw_payments"',
            },
        ),
        (["+customers"], {'"jaffle_shop"."main"."orders"', '"jaffle_shop"."main"."agg_orders"'}),
        (
            ["+tag:agg"],
            {
                '"jaffle_shop"."main"."customers"',
                '"jaffle_shop"."main"."stg_customers"',
                '"jaffle_shop"."main"."raw_customers"',
            },
        ),
    ],
)
def test_exclude_by_dbt_names(
    jaffle_shop_duckdb: Path,
    jaffle_shop_duckdb_context: Context,
    dbt_exclude: t.List[str],
    expected: t.Set[str],
):
    (jaffle_shop_duckdb / "models" / "agg_orders.sql").write_text("""
     {{ config(tags=["agg"]) }}
      select order_date, count(*) as num_orders from {{ ref('orders') }}                                                             
    """)

    ctx = jaffle_shop_duckdb_context
    ctx.load()
    assert '"jaffle_shop"."main"."agg_orders"' in ctx.models

    selector = ctx._new_selector()
    assert selector._dbt_mode

    sqlmesh_selector = selectors.to_sqlmesh(dbt_select=[], dbt_exclude=dbt_exclude)
    assert sqlmesh_selector

    assert selector.expand_model_selections([sqlmesh_selector]) == expected


@pytest.mark.parametrize(
    "dbt_select,dbt_exclude,expected",
    [
        (["jaffle_shop"], ["jaffle_shop"], set()),
        (
            ["staging"],
            ["stg_customers"],
            {
                '"jaffle_shop"."main"."stg_orders"',
                '"jaffle_shop"."main"."stg_payments"',
            },
        ),
        (
            ["staging.stg_customers", "tag:agg"],
            ["tag:agg"],
            {
                '"jaffle_shop"."main"."stg_customers"',
            },
        ),
    ],
)
def test_selection_and_exclusion_by_dbt_names(
    jaffle_shop_duckdb: Path,
    jaffle_shop_duckdb_context: Context,
    dbt_select: t.List[str],
    dbt_exclude: t.List[str],
    expected: t.Set[str],
):
    (jaffle_shop_duckdb / "models" / "agg_orders.sql").write_text("""
     {{ config(tags=["agg"]) }}
      select order_date, count(*) as num_orders from {{ ref('orders') }}                                                             
    """)

    ctx = jaffle_shop_duckdb_context
    ctx.load()
    assert '"jaffle_shop"."main"."agg_orders"' in ctx.models

    selector = ctx._new_selector()
    assert selector._dbt_mode

    sqlmesh_selector = selectors.to_sqlmesh(dbt_select=dbt_select, dbt_exclude=dbt_exclude)
    assert sqlmesh_selector

    assert selector.expand_model_selections([sqlmesh_selector]) == expected
