import typing as t
from pathlib import Path
import pytest
from click.testing import Result

pytestmark = pytest.mark.slow


def test_profile_and_target(jaffle_shop_duckdb: Path, invoke_cli: t.Callable[..., Result]):
    # profile doesnt exist - error
    result = invoke_cli(["--profile", "nonexist"])
    assert result.exit_code == 1
    assert "Profile 'nonexist' not found in profiles" in result.output

    # profile exists - successful load with default target
    result = invoke_cli(["--profile", "jaffle_shop"])
    assert result.exit_code == 0
    assert "No command specified" in result.output

    # profile exists but target doesnt - error
    result = invoke_cli(["--profile", "jaffle_shop", "--target", "nonexist"])
    assert result.exit_code == 1
    assert "Target 'nonexist' not specified in profiles" in result.output
    assert "valid target names for this profile are" in result.output
    assert "- dev" in result.output

    # profile exists and so does target - successful load with specified target
    result = invoke_cli(["--profile", "jaffle_shop", "--target", "dev"])
    assert result.exit_code == 0
    assert "No command specified" in result.output
