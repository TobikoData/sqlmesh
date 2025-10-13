from __future__ import annotations

import typing as t

import pytest
from pytest import FixtureRequest

from sqlmesh.core.engine_adapter import DorisEngineAdapter
from tests.core.engine_adapter.integration import TestContext
from tests.core.engine_adapter.integration import (
    generate_pytest_params,
    ENGINES_BY_NAME,
    IntegrationTestEngine,
)


@pytest.fixture(params=list(generate_pytest_params(ENGINES_BY_NAME["doris"])))
def ctx(
    request: FixtureRequest,
    create_test_context: t.Callable[[IntegrationTestEngine, str, str], t.Iterable[TestContext]],
) -> t.Iterable[TestContext]:
    yield from create_test_context(*request.param)


@pytest.fixture
def engine_adapter(ctx: TestContext) -> DorisEngineAdapter:
    assert isinstance(ctx.engine_adapter, DorisEngineAdapter)
    return ctx.engine_adapter


def test_engine_adapter(ctx: TestContext):
    assert isinstance(ctx.engine_adapter, DorisEngineAdapter)
    assert ctx.engine_adapter.fetchone("select 1") == (1,)
