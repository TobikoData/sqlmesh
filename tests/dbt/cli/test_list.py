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
