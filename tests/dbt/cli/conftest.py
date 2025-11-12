import typing as t
import functools
from click.testing import CliRunner, Result
import pytest


@pytest.fixture
def invoke_cli() -> t.Callable[..., Result]:
    from sqlmesh_dbt.cli import dbt

    return functools.partial(CliRunner().invoke, dbt)
