import typing as t
import pytest
from pathlib import Path
from click.testing import Result
import time_machine
from tests.cli.test_cli import FREEZE_TIME

pytestmark = pytest.mark.slow


def test_run(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    result = invoke_cli(["run"])

    assert result.exit_code == 0
    assert not result.exception

    assert "Model batches executed" in result.output


def test_run_with_selectors(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    with time_machine.travel(FREEZE_TIME):
        # do an initial run to create the objects
        # otherwise the selected subset may depend on something that hasnt been created
        result = invoke_cli(["run"])
        assert result.exit_code == 0
        assert "main.orders" in result.output

    result = invoke_cli(["run", "--select", "main.raw_customers+", "--exclude", "main.orders"])

    assert result.exit_code == 0
    assert not result.exception

    assert "main.stg_customers" in result.output
    assert "main.stg_orders" in result.output
    assert "main.stg_payments" in result.output
    assert "main.customers" in result.output

    assert "main.orders" not in result.output

    assert "Model batches executed" in result.output
