from __future__ import annotations

import typing as t
import pytest
import pathlib
import os
import logging
from pytest import FixtureRequest

from sqlmesh import Config, EngineAdapter
from sqlmesh.core.config.connection import AthenaConnectionConfig
from sqlmesh.core.engine_adapter import AthenaEngineAdapter
from sqlmesh.core.config import load_config_from_paths

from tests.core.engine_adapter.integration import TestContext, TEST_SCHEMA

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def config() -> Config:
    return load_config_from_paths(
        Config,
        project_paths=[
            pathlib.Path("examples/wursthall/config.yaml"),
            pathlib.Path(os.path.join(os.path.dirname(__file__), "config.yaml")),
        ],
        personal_paths=[pathlib.Path("~/.sqlmesh/config.yaml").expanduser()],
    )


@pytest.fixture
def engine_adapter(
    mark_gateway: t.Tuple[str, str],
    config,
    testrun_uid: str,
    request: pytest.FixtureRequest,
) -> EngineAdapter:
    mark, gateway = mark_gateway

    if gateway not in config.gateways:
        # TODO: Once everything is fully setup we want to error if a gateway is not configured that we expect
        pytest.skip(f"Gateway {gateway} not configured")

    connection_config = config.gateways[gateway].connection

    engine_adapter = connection_config.create_engine_adapter()

    if "athena" in mark:
        assert isinstance(connection_config, AthenaConnectionConfig)
        assert isinstance(engine_adapter, AthenaEngineAdapter)

        # S3 files need to go into a unique location for each test run
        # This is because DROP TABLE on a Hive table just drops the table from the metastore
        # The files still exist in S3, so if you CREATE TABLE to the same location, the old data shows back up
        # Note that the `testrun_uid` fixture comes from the xdist plugin
        if connection_config.s3_warehouse_location:
            engine_adapter.s3_warehouse_location = os.path.join(
                connection_config.s3_warehouse_location,
                f"testrun_{testrun_uid}",
                request.node.originalname,
            )

    # Trino: If we batch up the requests then when running locally we get a table not found error after creating the
    # table and then immediately after trying to insert rows into it. There seems to be a delay between when the
    # metastore is made aware of the table and when it responds that it exists. I'm hoping this is not an issue
    # in practice on production machines.
    if not mark.startswith("trino"):
        engine_adapter.DEFAULT_BATCH_SIZE = 1

    # Clear our any local db files that may have been left over from previous runs
    if mark == "duckdb":
        for raw_path in (connection_config.catalogs or {}).values():
            pathlib.Path(raw_path).unlink(missing_ok=True)

    return engine_adapter


@pytest.fixture
def ctx(
    request: FixtureRequest,
    engine_adapter: EngineAdapter,
    test_type: str,
    mark_gateway: t.Tuple[str, str],
) -> t.Iterable[TestContext]:
    mark, gateway = mark_gateway
    is_remote = request.node.get_closest_marker("remote") is not None

    ctx = TestContext(test_type, engine_adapter, mark, gateway, is_remote=is_remote)
    ctx.init()

    with ctx.engine_adapter.session({}):
        yield ctx

    try:
        ctx.cleanup()
    except Exception:
        # We need to catch this exception because if there is an error during teardown, pytest-retry aborts immediately
        # instead of retrying
        logger.exception("Context cleanup failed")


@pytest.fixture
def schema(ctx: TestContext) -> str:
    schema_name = ctx.schema(TEST_SCHEMA)
    ctx.engine_adapter.create_schema(
        schema_name
    )  # note: gets cleaned up when the TestContext fixture gets cleaned up
    return schema_name
