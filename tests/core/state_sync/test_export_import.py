import pytest
from pathlib import Path
from sqlmesh.core.state_sync import StateSync, EngineAdapterStateSync, CachingStateSync
from sqlmesh.core.state_sync.export_import import export_state, import_state
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.core import constants as c
from sqlmesh.cli.project_init import init_example_project
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
        cache_dir=tmp_path / c.CACHE,
    )


def test_export_empty_state(tmp_path: Path, state_sync: StateSync) -> None:
    output_file = tmp_path / "state_dump.json"

    # Cannot dump an un-migrated state database
    with pytest.raises(SQLMeshError, match=r"Please run a migration"):
        export_state(state_sync, output_file)

    state_sync.migrate(default_catalog=None)

    export_state(state_sync, output_file)

    state = json.loads(output_file.read_text(encoding="utf8"))

    assert "metadata" in state
    metadata = state["metadata"]
    assert "timestamp" in metadata
    assert "file_version" in metadata
    assert "importable" in metadata

    assert "versions" in state
    versions = state["versions"]
    assert "schema_version" in versions
    assert "sqlglot_version" in versions
    assert "sqlmesh_version" in versions

    assert "snapshots" in state
    assert isinstance(state["snapshots"], list)
    assert len(state["snapshots"]) == 0

    assert "environments" in state
    assert isinstance(state["environments"], dict)
    assert len(state["environments"]) == 0


def test_export_entire_project(
    tmp_path: Path, example_project_config: Config, state_sync: StateSync
) -> None:
    init_example_project(path=tmp_path, engine_type="duckdb")
    context = Context(paths=tmp_path, config=example_project_config, state_sync=state_sync)

    # prod
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

    # dev
    context.load()
    context.plan(environment="dev", auto_apply=True, skip_tests=True)

    output_file = tmp_path / "state_dump.json"
    export_state(state_sync, output_file)

    state = json.loads(output_file.read_text(encoding="utf8"))
    assert "metadata" in state
    # full project dumps can always be imported back
    assert state["metadata"]["importable"]

    assert "versions" in state

    assert len(state["snapshots"]) > 0
    snapshot_names = [s["name"] for s in state["snapshots"]]
    assert len(snapshot_names) == 5
    assert '"warehouse"."sqlmesh_example"."full_model"' in snapshot_names  # will be in here twice
    assert '"warehouse"."sqlmesh_example"."incremental_model"' in snapshot_names
    assert '"warehouse"."sqlmesh_example"."seed_model"' in snapshot_names
    assert '"warehouse"."sqlmesh_example"."new_model"' in snapshot_names

    assert "prod" in state["environments"]
    assert "dev" in state["environments"]

    prod = state["environments"]["prod"]["environment"]
    assert len(prod["snapshots"]) == 3
    prod_snapshot_ids = [s.snapshot_id for s in Environment.model_validate(prod).snapshots]

    dev = state["environments"]["dev"]["environment"]
    assert len(dev["snapshots"]) == 4
    dev_snapshot_ids = [s.snapshot_id for s in Environment.model_validate(dev).snapshots]

    full_model_id = next(s for s in dev_snapshot_ids if "full_model" in s.name)
    incremental_model_id = next(s for s in dev_snapshot_ids if "incremental_model" in s.name)
    seed_model_id = next(s for s in dev_snapshot_ids if "seed_model" in s.name)
    new_model_id = next(s for s in dev_snapshot_ids if "new_model" in s.name)

    assert incremental_model_id in prod_snapshot_ids
    assert seed_model_id in prod_snapshot_ids
    assert full_model_id not in prod_snapshot_ids
    assert new_model_id not in prod_snapshot_ids


def test_export_specific_environment(
    tmp_path: Path, example_project_config: Config, state_sync: StateSync
) -> None:
    output_file = tmp_path / "state_dump.json"
    init_example_project(path=tmp_path, engine_type="duckdb")
    context = Context(paths=tmp_path, config=example_project_config, state_sync=state_sync)

    # create prod
    context.plan(auto_apply=True)

    with pytest.raises(SQLMeshError, match=r"No such environment"):
        export_state(state_sync, output_file, environment_names=["FOO"])

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

    # plan dev
    context.load()
    plan = context.plan(environment="dev", auto_apply=True, skip_tests=True)
    assert len(plan.modified_snapshots) == 1

    # export dev - all models should be included
    export_state(state_sync, output_file, environment_names=["dev"])

    dev_state = json.loads(output_file.read_text(encoding="utf8"))

    assert "metadata" in dev_state
    assert "versions" in dev_state

    assert len(dev_state["snapshots"]) == 3
    snapshot_names = [s["name"] for s in dev_state["snapshots"]]
    assert any("full_model" in name for name in snapshot_names)
    assert any("incremental_model" in name for name in snapshot_names)
    assert any("seed_model" in name for name in snapshot_names)
    dev_full_model = next(s for s in dev_state["snapshots"] if "full_model" in s["name"])

    assert len(dev_state["environments"]) == 1
    assert "dev" in dev_state["environments"]

    # this state dump is still importable even though its just a subset
    assert dev_state["metadata"]["importable"]

    # export prod - prod full_model should be a different version to dev
    export_state(state_sync, output_file, environment_names=["prod"])

    prod_state = json.loads(output_file.read_text(encoding="utf8"))
    snapshot_names = [s["name"] for s in prod_state["snapshots"]]
    assert any("full_model" in name for name in snapshot_names)
    assert any("incremental_model" in name for name in snapshot_names)
    assert any("seed_model" in name for name in snapshot_names)
    prod_full_model = next(s for s in prod_state["snapshots"] if "full_model" in s["name"])

    assert len(prod_state["environments"]) == 1
    assert "prod" in prod_state["environments"]
    assert prod_state["metadata"]["importable"]

    assert dev_full_model["fingerprint"]["data_hash"] != prod_full_model["fingerprint"]["data_hash"]


def test_export_local_state(
    tmp_path: Path, example_project_config: Config, state_sync: StateSync
) -> None:
    output_file = tmp_path / "state_dump.json"
    init_example_project(path=tmp_path, engine_type="duckdb")
    context = Context(paths=tmp_path, config=example_project_config, state_sync=state_sync)

    # create prod
    context.plan(auto_apply=True)

    # modify full_model - create local change
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

    assert len(context.snapshots) == 3

    context.load()

    assert len(context.snapshots) == 4

    export_state(state_sync, output_file, context.snapshots)
    state = json.loads(output_file.read_text(encoding="utf8"))
    assert "metadata" in state
    assert "versions" in state

    # this state dump cannot be imported because its just local state
    assert not state["metadata"]["importable"]

    # no environments because local state is just snapshots
    assert len(state["environments"]) == 0

    snapshots = state["snapshots"]
    assert len(snapshots) == 4
    full_model = next(s for s in snapshots if "full_model" in s["name"])
    new_model = next(s for s in snapshots if "new_model" in s["name"])

    assert "'1' as modified" in full_model["node"]["query"]
    assert "SELECT 1 as id" in new_model["node"]["query"]


def test_import_invalid_file(tmp_path: Path, state_sync: StateSync) -> None:
    state_file = tmp_path / "state.json"
    state_file.write_text("invalid json file")

    with pytest.raises(Exception, match=r"Invalid JSON character"):
        import_state(state_sync, state_file)

    state_file.write_text("[]")
    with pytest.raises(SQLMeshError, match=r"Expected JSON object"):
        import_state(state_sync, state_file)

    state_file.write_text("{}")
    with pytest.raises(SQLMeshError, match=r"Expecting a 'metadata' key"):
        import_state(state_sync, state_file)

    state_file.write_text('{ "metadata": [] }')
    with pytest.raises(SQLMeshError, match=r"Expecting the 'metadata' key to contain an object"):
        import_state(state_sync, state_file)

    state_file.write_text('{ "metadata": {} }')
    with pytest.raises(SQLMeshError, match=r"Unable to determine state file format version"):
        import_state(state_sync, state_file)

    state_file.write_text('{ "metadata": { "file_version": "blah" } }')
    with pytest.raises(SQLMeshError, match=r"Unable to parse state file format version"):
        import_state(state_sync, state_file)

    state_file.write_text('{ "metadata": { "file_version": 1, "importable": false } }')
    with pytest.raises(SQLMeshError, match=r"not importable"):
        import_state(state_sync, state_file)


def test_import_from_older_version_export_fails(tmp_path: Path, state_sync: StateSync) -> None:
    state_sync.migrate(default_catalog=None)
    current_version = state_sync.get_versions()

    major, minor = current_version.minor_sqlmesh_version
    older_version = current_version.copy(update=dict(sqlmesh_version=f"{major}.{minor - 1}.0"))

    assert older_version.minor_sqlmesh_version < current_version.minor_sqlmesh_version

    state_file = tmp_path / "state.json"
    state_versions = older_version.model_dump(mode="json")
    state_file.write_text(
        json.dumps(
            {
                "metadata": {
                    "timestamp": "2024-01-01 00:00:00",
                    "file_version": 1,
                    "importable": True,
                },
                "versions": state_versions,
            }
        )
    )

    with pytest.raises(SQLMeshError, match=r"SQLMesh version mismatch"):
        import_state(state_sync, state_file)


def test_import_from_newer_version_export_fails(tmp_path: Path, state_sync: StateSync) -> None:
    state_sync.migrate(default_catalog=None)
    current_version = state_sync.get_versions()

    major, minor = current_version.minor_sqlmesh_version
    newer_version = current_version.copy(update=dict(sqlmesh_version=f"{major}.{minor + 1}.0"))

    assert newer_version.minor_sqlmesh_version > current_version.minor_sqlmesh_version

    state_file = tmp_path / "state.json"
    state_versions = newer_version.model_dump(mode="json")
    state_file.write_text(
        json.dumps(
            {
                "versions": state_versions,
                "metadata": {
                    "timestamp": "2024-01-01 00:00:00",
                    "file_version": 1,
                    "importable": True,
                },
            }
        )
    )

    with pytest.raises(SQLMeshError, match=r"SQLMesh version mismatch"):
        import_state(state_sync, state_file)


def test_import_local_state_fails(
    tmp_path: Path, example_project_config: Config, state_sync: StateSync
) -> None:
    output_file = tmp_path / "state_dump.json"
    init_example_project(path=tmp_path, engine_type="duckdb")
    context = Context(paths=tmp_path, config=example_project_config, state_sync=state_sync)

    export_state(state_sync, output_file, context.snapshots)
    state = json.loads(output_file.read_text(encoding="utf8"))
    assert len(state["snapshots"]) == 3

    with pytest.raises(SQLMeshError, match=r"not importable"):
        import_state(state_sync, output_file)


def test_import_partial(
    tmp_path: Path, example_project_config: Config, state_sync: StateSync
) -> None:
    output_file = tmp_path / "state_dump.json"
    init_example_project(path=tmp_path, engine_type="duckdb")
    context = Context(paths=tmp_path, config=example_project_config, state_sync=state_sync)

    # create prod
    context.plan(auto_apply=True)

    # add a new model
    (tmp_path / c.MODELS / "new_model.sql").write_text("""
    MODEL (
        name sqlmesh_example.new_model,
        kind FULL,
        cron '@daily'
    );

    SELECT 1 as id;
    """)

    # create dev
    context.load()
    context.plan(environment="dev", auto_apply=True, skip_tests=True)

    # export just dev
    export_state(state_sync, output_file, environment_names=["dev"])

    state = json.loads(output_file.read_text(encoding="utf8"))
    # mess with the file to rename "dev" to "dev2"
    dev = state["environments"].pop("dev")
    dev["environment"]["name"] = "dev2"
    state["environments"]["dev2"] = dev

    assert list(state["environments"].keys()) == ["dev2"]
    output_file.write_text(json.dumps(state), encoding="utf8")

    # import "dev2"
    import_state(state_sync, output_file, clear=False)

    # StateSync should have "prod", "dev" and "dev2".
    assert sorted(list(env.name for env in state_sync.get_environments_summary())) == [
        "dev",
        "dev2",
        "prod",
    ]

    assert not context.plan(environment="dev", skip_tests=True).has_changes
    assert not context.plan(environment="dev2", skip_tests=True).has_changes
    assert context.plan(
        environment="prod", skip_tests=True
    ).has_changes  # prod has changes the 'new_model' model hasnt been applied


def test_roundtrip(tmp_path: Path, example_project_config: Config, state_sync: StateSync) -> None:
    state_file = tmp_path / "state_dump.json"

    init_example_project(path=tmp_path, engine_type="duckdb")
    context = Context(paths=tmp_path, config=example_project_config, state_sync=state_sync)

    # populate initial state
    plan = context.plan(auto_apply=True)
    assert plan.has_changes

    # plan again to prove no changes
    plan = context.plan(auto_apply=True)
    assert not plan.has_changes

    export_state(state_sync, state_file)
    assert len(state_file.read_text()) > 0

    # destroy state
    assert isinstance(state_sync, EngineAdapterStateSync)
    state_sync.engine_adapter.drop_schema("sqlmesh", cascade=True)

    # state was destroyed, plan should have changes
    state_sync.migrate(default_catalog=None)
    plan = context.plan()
    assert plan.has_changes

    # load in state dump
    import_state(state_sync, state_file)

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
    export_state(state_sync, state_file)

    # show state destroyed
    state_sync.engine_adapter.drop_schema("sqlmesh", cascade=True)
    with pytest.raises(SQLMeshError, match=r"Please run a migration"):
        state_sync.get_versions(validate=True)

    state_sync.migrate(default_catalog=None)
    import_state(state_sync, state_file)

    # should be no changes in dev
    assert not context.plan(environment="dev").has_changes

    # prod should show a change for adding 'new_model'
    prod_plan = context.plan(environment="prod")
    assert prod_plan.new_snapshots == []
    assert len(prod_plan.modified_snapshots) == 1
    assert "new_model" in list(prod_plan.modified_snapshots.values())[0].name


def test_roundtrip_includes_auto_restatements(
    tmp_path: Path, example_project_config: Config, state_sync: StateSync
) -> None:
    init_example_project(path=tmp_path, engine_type="duckdb")

    # add a model with auto restatements
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

    context = Context(paths=tmp_path, config=example_project_config, state_sync=state_sync)
    context.plan(auto_apply=True)

    # dump state
    output_file = tmp_path / "state_dump.json"
    export_state(state_sync, output_file)
    state = json.loads(output_file.read_text(encoding="utf8"))

    snapshots = state["snapshots"]
    assert len(snapshots) == 4

    # auto restatements only work after a cadence run
    new_model_snapshot = next(s for s in snapshots if "new_model" in s["name"])
    assert "next_auto_restatement_ts" not in new_model_snapshot

    # trigger cadence run and re-dump show auto restatement dumped
    context.run()

    export_state(state_sync, output_file)
    state = json.loads(output_file.read_text())

    new_model_snapshot = next(s for s in state["snapshots"] if "new_model" in s["name"])
    assert new_model_snapshot["next_auto_restatement_ts"] > 0

    # import the state again and run a plan to show there is no changes / the auto restatement was imported
    import_state(state_sync, output_file)

    plan = context.plan(skip_tests=True)
    assert not plan.has_changes


def test_roundtrip_includes_environment_statements(tmp_path: Path) -> None:
    config = Config(
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
        before_all=["select 1 as before_all"],
        after_all=["select 2 as after_all"],
    )

    context = Context(paths=tmp_path, config=config)
    context.plan(auto_apply=True)

    state_file = tmp_path / "state_dump.json"
    context.export_state(state_file)

    environments = json.loads(state_file.read_text(encoding="utf8"))["environments"]

    assert environments["prod"]["statements"][0]["before_all"][0] == "select 1 as before_all"
    assert environments["prod"]["statements"][0]["after_all"][0] == "select 2 as after_all"

    assert not context.plan().has_changes

    state_sync = context.state_sync
    assert isinstance(state_sync, CachingStateSync)
    assert isinstance(state_sync.state_sync, EngineAdapterStateSync)

    # show state destroyed
    state_sync.state_sync.engine_adapter.drop_schema("sqlmesh", cascade=True)  # type: ignore
    with pytest.raises(SQLMeshError, match=r"Please run a migration"):
        state_sync.get_versions(validate=True)

    state_sync.migrate(default_catalog=None)
    import_state(state_sync, state_file)

    assert not context.plan().has_changes

    environment_statements = state_sync.get_environment_statements("prod")
    assert len(environment_statements) == 1
    assert environment_statements[0].before_all[0] == "select 1 as before_all"
    assert environment_statements[0].after_all[0] == "select 2 as after_all"
