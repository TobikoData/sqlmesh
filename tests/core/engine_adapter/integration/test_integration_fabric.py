import typing as t
import threading
import queue
import pytest
from pytest import FixtureRequest
from sqlmesh.core.engine_adapter import FabricEngineAdapter
from sqlmesh.utils.connection_pool import ThreadLocalConnectionPool
from tests.core.engine_adapter.integration import TestContext
from concurrent.futures import ThreadPoolExecutor

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


def test_drop_catalog_clears_threadlocals_that_reference_it(
    ctx: TestContext, engine_adapter: FabricEngineAdapter
):
    catalog_name = ctx.add_test_suffix("test_drop_catalog")
    default_catalog = engine_adapter.get_current_catalog()

    assert isinstance(engine_adapter._connection_pool, ThreadLocalConnectionPool)

    # sets the connection attribute for this thread
    engine_adapter.create_catalog(catalog_name)
    assert engine_adapter._target_catalog is None
    engine_adapter.set_current_catalog(catalog_name)
    assert engine_adapter.get_current_catalog() == catalog_name
    assert engine_adapter._target_catalog == catalog_name

    lock = threading.RLock()

    def _set_and_return_catalog_in_another_thread(
        q: queue.Queue, engine_adapter: FabricEngineAdapter
    ) -> t.Optional[str]:
        q.put("thread_started")

        assert engine_adapter.get_current_catalog() == default_catalog
        assert engine_adapter._target_catalog is None

        engine_adapter.set_current_catalog(catalog_name)
        assert engine_adapter.get_current_catalog() == catalog_name
        assert engine_adapter._target_catalog == catalog_name

        q.put("catalog_set_in_thread")

        # block this thread while we drop the catalog in the main test thread
        lock.acquire()

        # the current catalog should have been cleared from the threadlocal connection attributes
        # when this catalog was dropped by the outer thread, causing it to fall back to the default catalog
        try:
            assert engine_adapter._target_catalog is None
            return engine_adapter.get_current_catalog()
        finally:
            lock.release()

    q: queue.Queue = queue.Queue()

    with ThreadPoolExecutor() as executor:
        lock.acquire()  # we have the lock, thread will be blocked until we release it

        future = executor.submit(_set_and_return_catalog_in_another_thread, q, engine_adapter)

        assert q.get() == "thread_started"
        assert not future.done()

        try:
            assert q.get(timeout=20) == "catalog_set_in_thread"
        except:
            if exec := future.exception():
                raise exec
            raise

        ctx.drop_catalog(catalog_name)
        assert not future.done()

        lock.release()  # yield the lock to the thread

        # block until thread complete
        result = future.result()

        # both threads should be automatically using the default catalog now
        assert result == default_catalog
        assert engine_adapter.get_current_catalog() == default_catalog
