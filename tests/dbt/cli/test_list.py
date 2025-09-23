import typing as t
import pytest
from pathlib import Path
from click.testing import Result

pytestmark = pytest.mark.slow


def test_list(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    result = invoke_cli(["list"])

    assert result.exit_code == 0
    assert not result.exception

    assert "─ jaffle_shop.orders" in result.output
    assert "─ jaffle_shop.customers" in result.output
    assert "─ jaffle_shop.staging.stg_payments" in result.output
    assert "─ jaffle_shop.raw_orders" in result.output


def test_list_select(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    result = invoke_cli(["list", "--select", "main.raw_customers+"])

    assert result.exit_code == 0
    assert not result.exception

    assert "─ jaffle_shop.customers" in result.output
    assert "─ jaffle_shop.staging.stg_customers" in result.output
    assert "─ jaffle_shop.raw_customers" in result.output

    assert "─ jaffle_shop.staging.stg_payments" not in result.output
    assert "─ jaffle_shop.raw_orders" not in result.output


def test_list_select_exclude(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    # single exclude
    result = invoke_cli(["list", "--select", "main.raw_customers+", "--exclude", "main.orders"])

    assert result.exit_code == 0
    assert not result.exception

    assert "─ jaffle_shop.customers" in result.output
    assert "─ jaffle_shop.staging.stg_customers" in result.output
    assert "─ jaffle_shop.raw_customers" in result.output

    assert "─ jaffle_shop.orders" not in result.output
    assert "─ jaffle_shop.staging.stg_payments" not in result.output
    assert "─ jaffle_shop.raw_orders" not in result.output

    # multiple exclude
    for args in (
        ["--select", "main.stg_orders+", "--exclude", "main.customers", "--exclude", "main.orders"],
        ["--select", "main.stg_orders+", "--exclude", "main.customers main.orders"],
    ):
        result = invoke_cli(["list", *args])
        assert result.exit_code == 0
        assert not result.exception

        assert "─ jaffle_shop.staging.stg_orders" in result.output

        assert "─ jaffle_shop.customers" not in result.output
        assert "─ jaffle_shop.orders" not in result.output


def test_list_with_vars(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    (
        jaffle_shop_duckdb / "models" / "vars_model.sql"
    ).write_text("""                                                          
    select * from {{ ref('custom' + var('foo')) }}
    """)

    result = invoke_cli(["list", "--vars", "foo: ers"])

    assert result.exit_code == 0
    assert not result.exception

    assert (
        """├── jaffle_shop.vars_model
│   └── depends_on: jaffle_shop.customers"""
        in result.output
    )
