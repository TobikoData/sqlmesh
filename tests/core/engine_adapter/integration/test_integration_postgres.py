import typing as t
import pytest
from sqlmesh.core.engine_adapter import PostgresEngineAdapter
from tests.core.engine_adapter.integration import TestContext

pytestmark = [pytest.mark.docker, pytest.mark.engine, pytest.mark.postgres]


@pytest.fixture
def mark_gateway() -> t.Tuple[str, str]:
    return "postgres", "inttest_postgres"


@pytest.fixture
def test_type() -> str:
    return "query"


def test_engine_adapter(ctx: TestContext):
    assert isinstance(ctx.engine_adapter, PostgresEngineAdapter)
    assert ctx.engine_adapter.fetchone("select 1") == (1,)
