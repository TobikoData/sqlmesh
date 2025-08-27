import typing as t
import pytest
from pathlib import Path
from click.testing import Result

pytestmark = pytest.mark.slow


def test_list(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    result = invoke_cli(["list"])

    assert result.exit_code == 0
    assert not result.exception

    assert "main.orders" in result.output
    assert "main.customers" in result.output
    assert "main.stg_payments" in result.output
    assert "main.raw_orders" in result.output


def test_list_select(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    result = invoke_cli(["list", "--select", "main.raw_customers+"])

    assert result.exit_code == 0
    assert not result.exception

    assert "main.orders" in result.output
    assert "main.customers" in result.output
    assert "main.stg_customers" in result.output
    assert "main.raw_customers" in result.output

    assert "main.stg_payments" not in result.output
    assert "main.raw_orders" not in result.output


def test_list_select_exclude(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    # single exclude
    result = invoke_cli(["list", "--select", "main.raw_customers+", "--exclude", "main.orders"])

    assert result.exit_code == 0
    assert not result.exception

    assert "main.customers" in result.output
    assert "main.stg_customers" in result.output
    assert "main.raw_customers" in result.output

    assert "main.orders" not in result.output
    assert "main.stg_payments" not in result.output
    assert "main.raw_orders" not in result.output

    # multiple exclude
    for args in (
        ["--select", "main.stg_orders+", "--exclude", "main.customers", "--exclude", "main.orders"],
        ["--select", "main.stg_orders+", "--exclude", "main.customers main.orders"],
    ):
        result = invoke_cli(["list", *args])
        assert result.exit_code == 0
        assert not result.exception

        assert "main.stg_orders" in result.output

        assert "main.customers" not in result.output
        assert "main.orders" not in result.output


def test_list_with_vars(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    (jaffle_shop_duckdb / "models" / "aliased_model.sql").write_text("""
    {{ config(alias='model_' + var('foo')) }}                                                              
    select 1
    """)

    result = invoke_cli(["list", "--vars", "foo: bar"])

    assert result.exit_code == 0
    assert not result.exception

    assert "model_bar" in result.output
