import typing as t
from pathlib import Path
import os
import functools
from click.testing import CliRunner, Result
import pytest


@pytest.fixture
def jaffle_shop_duckdb(copy_to_temp_path: t.Callable[..., t.List[Path]]) -> t.Iterable[Path]:
    fixture_path = Path(__file__).parent / "fixtures" / "jaffle_shop_duckdb"
    assert fixture_path.exists()

    current_path = os.getcwd()
    output_path = copy_to_temp_path(paths=fixture_path)[0]

    # so that we can invoke commands from the perspective of a user that is alrady in the correct directory
    os.chdir(output_path)

    yield output_path

    os.chdir(current_path)


@pytest.fixture
def invoke_cli() -> t.Callable[..., Result]:
    from sqlmesh_dbt.cli import dbt

    return functools.partial(CliRunner().invoke, dbt)
