import typing as t
import pytest
from pytest import FixtureRequest
from sqlmesh.core.engine_adapter import FabricEngineAdapter
from tests.core.engine_adapter.integration import TestContext

from tests.core.engine_adapter.integration import (
    TestContext,
    generate_pytest_params,
    ENGINES_BY_NAME,
    IntegrationTestEngine,
)


@pytest.fixture(
    params=list(generate_pytest_params(ENGINES_BY_NAME["fabric"], show_variant_in_test_id=False))
)
def ctx(
    request: FixtureRequest,
    create_test_context: t.Callable[[IntegrationTestEngine, str, str], t.Iterable[TestContext]],
) -> t.Iterable[TestContext]:
    yield from create_test_context(*request.param)


@pytest.fixture
def engine_adapter(ctx: TestContext) -> FabricEngineAdapter:
    assert isinstance(ctx.engine_adapter, FabricEngineAdapter)
    return ctx.engine_adapter


def test_create_drop_catalog(ctx: TestContext, engine_adapter: FabricEngineAdapter):
    catalog_name = ctx.add_test_suffix("test_catalog")

    try:
        ctx.create_catalog(catalog_name)
        # if already exists, should be no-op, not error
        ctx.create_catalog(catalog_name)
        ctx.drop_catalog(catalog_name)
    finally:
        # if doesnt exist, should be no-op, not error
        ctx.drop_catalog(catalog_name)
