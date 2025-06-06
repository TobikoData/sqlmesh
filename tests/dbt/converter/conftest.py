from pathlib import Path
import typing as t
import pytest
from sqlmesh.core.context import Context


@pytest.fixture
def sushi_dbt_context(copy_to_temp_path: t.Callable) -> Context:
    return Context(paths=copy_to_temp_path("examples/sushi_dbt"))


@pytest.fixture
def empty_dbt_context(copy_to_temp_path: t.Callable) -> Context:
    fixture_path = Path(__file__).parent / "fixtures" / "empty_dbt_project"
    assert fixture_path.exists()

    actual_path = copy_to_temp_path(fixture_path)[0]

    ctx = Context(paths=actual_path)

    return ctx
