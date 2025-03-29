import pytest
from pathlib import Path
from sqlmesh.core.state_sync import StateSync, EngineAdapterStateSync
from sqlmesh.core.state_sync.dump_load import dump_state, load_state
from sqlmesh.core.console import NoopConsole
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.core import constants as c
from sqlmesh.cli.example_project import init_example_project
from sqlmesh.core.context import Context
from sqlmesh.core.environment import Environment
from sqlmesh.core.config import Config, GatewayConfig, DuckDBConnectionConfig, ModelDefaultsConfig

import json


@pytest.fixture
def example_project_config(tmp_path: Path) -> Config:
    return Config(
        gateways={
            "main": GatewayConfig(
                connection=DuckDBConnectionConfig(database=str(tmp_path / "warehouse.db")),
                state_connection=DuckDBConnectionConfig(database=str(tmp_path / "state.db")),
            )
        },
        default_gateway="main",
        model_defaults=ModelDefaultsConfig(
            dialect="duckdb",
        ),
    )


@pytest.fixture
def state_sync(tmp_path: Path, example_project_config: Config) -> StateSync:
    return EngineAdapterStateSync(
        engine_adapter=example_project_config.get_state_connection("main").create_engine_adapter(),  # type: ignore
        schema=c.SQLMESH,
        context_path=tmp_path,
    )


def test_dump_unsupported_version(tmp_path: Path, state_sync: StateSync) -> None:
    with pytest.raises(SQLMeshError, match=r"No serializer/deserializer implementation available"):
        dump_state(state_sync, tmp_path / "state_dump.json", state_format_version=-1)


def test_dump_empty_state(tmp_path: Path, state_sync: StateSync) -> None:
    output_file = tmp_path / "state_dump.json"

    # Cannot dump an un-migrated state database
    with pytest.raises(SQLMeshError, match=r"Please run a migration"):
        dump_state(state_sync, output_file)

    state_sync.migrate(default_catalog=None)

    dump_state(state_sync, output_file)

    state = json.loads(output_file.read_text(encoding="utf8"))

    assert "timestamp" in state
    assert "versions" in state
    versions = state["versions"]
    assert "schema_version" in versions
    assert "sqlglot_version" in versions
    assert "sqlmesh_version" in versions
    assert "state_dump_version" in versions

    assert "snapshots" in state
    assert isinstance(state["snapshots"], list)
    assert len(state["snapshots"]) == 0

    assert "environments" in state
    assert isinstance(state["environments"], dict)
    assert len(state["environments"]) == 0

    assert "auto_restatements" in state
    assert isinstance(state["auto_restatements"], list)
    assert len(state["auto_restatements"]) == 0


def test_dump_project(
    tmp_path: Path, example_project_config: Config, state_sync: StateSync
) -> None:
    init_example_project(path=tmp_path, dialect="duckdb")
    context = Context(paths=tmp_path, config=example_project_config, state_sync=state_sync)

    plan = context.plan(auto_apply=True)

    assert len(plan.modified_snapshots) > 0

    output_file = tmp_path / "state_dump.json"
    dump_state(state_sync, output_file, NoopConsole())

    state = json.loads(output_file.read_text(encoding="utf8"))
    assert len(state["snapshots"]) > 0

    snapshot_names = [s["name"] for s in state["snapshots"]]
    assert len(snapshot_names) == 3
    assert '"warehouse"."sqlmesh_example"."full_model"' in snapshot_names
    assert '"warehouse"."sqlmesh_example"."incremental_model"' in snapshot_names
    assert '"warehouse"."sqlmesh_example"."seed_model"' in snapshot_names

    assert "prod" in state["environments"]


def test_dump_project_with_modified_dev_environments(
    tmp_path: Path, example_project_config: Config, state_sync: StateSync
) -> None:
    init_example_project(path=tmp_path, dialect="duckdb")
    context = Context(paths=tmp_path, config=example_project_config, state_sync=state_sync)

    plan = context.plan(auto_apply=True)

    assert len(plan.modified_snapshots) > 0

    # modify full_model
    (tmp_path / c.MODELS / "full_model.sql").write_text("""
    MODEL (
        name sqlmesh_example.full_model,
        kind FULL,
        cron '@daily'
    );

    SELECT
        item_id,
        COUNT(DISTINCT id) AS num_orders,
        '1' as modified
    FROM sqlmesh_example.incremental_model
    GROUP BY item_id;
    """)

    # add a new model
    (tmp_path / c.MODELS / "new_model.sql").write_text("""
    MODEL (
        name sqlmesh_example.new_model,
        kind INCREMENTAL_BY_UNIQUE_KEY (
            unique_key id,
            auto_restatement_cron '@daily'
        ),
        cron '@daily'
    );

    SELECT 1 as id;
    """)

    context.load()
    plan = context.plan(environment="dev", auto_apply=True, skip_tests=True)
    assert len(plan.modified_snapshots) == 2

    # dump state
    output_file = tmp_path / "state_dump.json"
    dump_state(state_sync, output_file)
    state = json.loads(output_file.read_text(encoding="utf8"))

    snapshots = state["snapshots"]
    assert len(snapshots) == 5
    assert "prod" in state["environments"]
    assert "dev" in state["environments"]

    prod = state["environments"]["prod"]
    assert len(prod["snapshots"]) == 3
    prod_snapshot_ids = [s.snapshot_id for s in Environment.model_validate(prod).snapshots]

    dev = state["environments"]["dev"]
    assert len(dev["snapshots"]) == 4
    dev_snapshot_ids = [s.snapshot_id for s in Environment.model_validate(dev).snapshots]

    full_model = next(s for s in dev_snapshot_ids if "full_model" in s.name)
    incremental_model = next(s for s in dev_snapshot_ids if "incremental_model" in s.name)
    seed_model = next(s for s in dev_snapshot_ids if "seed_model" in s.name)
    new_model = next(s for s in dev_snapshot_ids if "new_model" in s.name)

    assert incremental_model in prod_snapshot_ids
    assert seed_model in prod_snapshot_ids
    assert full_model not in prod_snapshot_ids
    assert new_model not in prod_snapshot_ids

    # auto restatements only work in prod
    assert len(state["auto_restatements"]) == 0

    # apply prod, trigger cadence run and re-dump show auto restatement dumped
    context.plan(auto_apply=True, skip_tests=True)
    context.run()

    dump_state(state_sync, output_file)
    state = json.loads(output_file.read_text())
    assert len(state["auto_restatements"]) == 1


def test_load_invalid_file(tmp_path: Path, state_sync: StateSync) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text("invalid json file")

    with pytest.raises(Exception, match=r"Invalid JSON character"):
        load_state(state_sync, state_file)

    state_file.write_text("[]")
    with pytest.raises(SQLMeshError, match=r"Expected JSON object"):
        load_state(state_sync, state_file)

    state_file.write_text("{}")
    with pytest.raises(SQLMeshError, match=r"Expecting a 'versions' key"):
        load_state(state_sync, state_file)

    state_file.write_text('{ "versions": [] }')
    with pytest.raises(SQLMeshError, match=r"Expecting the 'versions' key to contain an object"):
        load_state(state_sync, state_file)

    state_file.write_text('{ "versions": {} }')
    with pytest.raises(SQLMeshError, match=r"Unable to determine state dump version"):
        load_state(state_sync, state_file)

    state_file.write_text('{ "versions": { "state_dump_version": "blah" } }')
    with pytest.raises(SQLMeshError, match=r"Unable to parse state dump version"):
        load_state(state_sync, state_file)


def test_load_from_older_version_dump_fails(tmp_path: Path, state_sync: StateSync) -> None:
    state_sync.migrate(default_catalog=None)
    current_version = state_sync.get_versions()

    major, minor = current_version.minor_sqlmesh_version
    older_version = current_version.copy(update=dict(sqlmesh_version=f"{major}.{minor - 1}.0"))

    assert older_version.minor_sqlmesh_version < current_version.minor_sqlmesh_version

    state_file = tmp_path / "state.json"
    state_versions = older_version.model_dump(mode="json")
    state_versions["state_dump_version"] = 1
    state_file.write_text(
        json.dumps({"versions": state_versions, "timestamp": "2024-01-01 00:00:00"})
    )

    with pytest.raises(SQLMeshError, match=r"SQLMesh version mismatch"):
        load_state(state_sync, state_file)


def test_load_from_newer_version_dump_fails(tmp_path: Path, state_sync: StateSync) -> None:
    state_sync.migrate(default_catalog=None)
    current_version = state_sync.get_versions()

    major, minor = current_version.minor_sqlmesh_version
    newer_version = current_version.copy(update=dict(sqlmesh_version=f"{major}.{minor + 1}.0"))

    assert newer_version.minor_sqlmesh_version > current_version.minor_sqlmesh_version

    state_file = tmp_path / "state.json"
    state_versions = newer_version.model_dump(mode="json")
    state_versions["state_dump_version"] = 1
    state_file.write_text(
        json.dumps({"versions": state_versions, "timestamp": "2024-01-01 00:00:00"})
    )

    with pytest.raises(SQLMeshError, match=r"SQLMesh version mismatch"):
        load_state(state_sync, state_file)


def test_roundtrip(tmp_path: Path, example_project_config: Config, state_sync: StateSync) -> None:
    state_file = tmp_path / "state_dump.json"

    init_example_project(path=tmp_path, dialect="duckdb")
    context = Context(paths=tmp_path, config=example_project_config, state_sync=state_sync)

    # populate initial state
    plan = context.plan(auto_apply=True)
    assert plan.has_changes

    # plan again to prove no changes
    plan = context.plan(auto_apply=True)
    assert not plan.has_changes

    dump_state(state_sync, state_file)
    assert len(state_file.read_text()) > 0

    # destroy state
    assert isinstance(state_sync, EngineAdapterStateSync)
    state_sync.engine_adapter.drop_schema("sqlmesh", cascade=True)

    # state was destroyed, plan should have changes
    state_sync.migrate(default_catalog=None)
    plan = context.plan()
    assert plan.has_changes

    # load in state dump
    load_state(state_sync, state_file)

    # plan should have no changes now our state is back
    plan = context.plan()
    assert not plan.has_changes

    # add a new model
    (tmp_path / c.MODELS / "new_model.sql").write_text("""
    MODEL (
        name sqlmesh_example.new_model,
        kind FULL,
        cron '@daily'
    );

    SELECT 1 as id;
    """)

    context.load()
    plan = context.plan(environment="dev", auto_apply=True)
    assert plan.has_changes

    plan = context.plan(environment="dev")
    assert not plan.has_changes

    # dump new state that contains the 'dev' environment
    dump_state(state_sync, state_file)

    # show state destroyed
    state_sync.engine_adapter.drop_schema("sqlmesh", cascade=True)
    with pytest.raises(SQLMeshError, match=r"Please run a migration"):
        state_sync.get_versions(validate=True)

    state_sync.migrate(default_catalog=None)
    load_state(state_sync, state_file)

    # should be no changes in dev
    assert not context.plan(environment="dev").has_changes

    # prod should show a change for adding 'new_model'
    prod_plan = context.plan(environment="prod")
    assert prod_plan.new_snapshots == []
    assert len(prod_plan.modified_snapshots) == 1
    assert "new_model" in list(prod_plan.modified_snapshots.values())[0].name
