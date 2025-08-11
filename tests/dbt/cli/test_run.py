import typing as t
import pytest
from pathlib import Path
from click.testing import Result

pytestmark = pytest.mark.slow


def test_run(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    result = invoke_cli(["run"])

    assert result.exit_code == 0
    assert not result.exception

    assert "Model batches executed" in result.output
