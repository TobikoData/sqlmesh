from __future__ import annotations

import typing as t
import pytest
import pathlib
import os

from sqlmesh import Config, EngineAdapter
from sqlmesh.core.config.connection import AthenaConnectionConfig
from sqlmesh.core.config import load_config_from_paths

from tests.core.engine_adapter.integration import TestContext


@pytest.fixture(scope="session")
def run_count(request) -> t.Iterable[int]:
    count: int = request.config.cache.get("run_count", 0)
    count += 1
    yield count
    request.config.cache.set("run_count", count)


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
    mark_gateway: t.Tuple[str, str], config, testrun_uid, run_count
) -> EngineAdapter:
    mark, gateway = mark_gateway

    if gateway not in config.gateways:
        # TODO: Once everything is fully setup we want to error if a gateway is not configured that we expect
        pytest.skip(f"Gateway {gateway} not configured")

    connection_config = config.gateways[gateway].connection

    if mark == "athena":
        connection_config = t.cast(AthenaConnectionConfig, connection_config)
        # S3 files need to go into a unique location for each test run
        # This is because DROP TABLE on a Hive table just drops the table from the metastore
        # The files still exist in S3, so if you CREATE TABLE to the same location, the old data shows back up
        # This is a problem for any tests like `test_init_project` that use a consistent schema like `sqlmesh_example` between runs
        # Note that the `testrun_uid` fixture comes from the xdist plugin
        testrun_path = f"testrun_{testrun_uid}"
        # TODO: remove the compexity of run_count and just use boto3 to clear out the S3 test location prior to running tests
        if current_location := connection_config.s3_warehouse_location:
            if testrun_path not in current_location:
                # only add it if its not already there (since this setup code gets called multiple times in a full test run)
                connection_config.s3_warehouse_location = os.path.join(
                    current_location, testrun_path, str(run_count)
                )

    engine_adapter = connection_config.create_engine_adapter()
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
def ctx(request, engine_adapter, test_type, mark_gateway):
    _, gateway = mark_gateway
    is_remote = request.node.get_closest_marker("remote") is not None
    return TestContext(test_type, engine_adapter, gateway, is_remote=is_remote)


@pytest.fixture(autouse=True)
def cleanup(ctx: TestContext):
    yield  # run test

    if ctx:
        ctx.cleanup()
