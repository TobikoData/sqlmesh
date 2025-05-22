import typing as t
import pytest
from sqlmesh.core.context import Context


@pytest.fixture
def sushi_dbt_context(copy_to_temp_path: t.Callable) -> Context:
    return Context(paths=copy_to_temp_path("examples/sushi_dbt"))
