import typing as t
import pytest
from pytest import FixtureRequest
from pathlib import Path
from sqlmesh.core.engine_adapter import PostgresEngineAdapter
from sqlmesh.core.config import Config, DuckDBConnectionConfig
from tests.core.engine_adapter.integration import TestContext
import time_machine
from datetime import timedelta
from sqlmesh.utils.date import to_ds
from sqlglot import exp
from sqlmesh.core.context import Context
from sqlmesh.core.state_sync import CachingStateSync, EngineAdapterStateSync
from sqlmesh.core.snapshot.definition import SnapshotId

from tests.core.engine_adapter.integration import (
    TestContext,
    generate_pytest_params,
    ENGINES_BY_NAME,
    IntegrationTestEngine,
    TEST_SCHEMA,
)


@pytest.fixture(params=list(generate_pytest_params(ENGINES_BY_NAME["postgres"])))
def ctx(
    request: FixtureRequest,
    create_test_context: t.Callable[[IntegrationTestEngine, str, str], t.Iterable[TestContext]],
) -> t.Iterable[TestContext]:
    yield from create_test_context(*request.param)


@pytest.fixture
def engine_adapter(ctx: TestContext) -> PostgresEngineAdapter:
    assert isinstance(ctx.engine_adapter, PostgresEngineAdapter)
    return ctx.engine_adapter


def test_engine_adapter(ctx: TestContext):
    assert isinstance(ctx.engine_adapter, PostgresEngineAdapter)
    assert ctx.engine_adapter.fetchone("select 1") == (1,)


def test_server_version_psycopg(ctx: TestContext):
    assert isinstance(ctx.engine_adapter, PostgresEngineAdapter)
    assert ctx.engine_adapter.server_version != (0, 0)


def test_janitor_drop_cascade(ctx: TestContext, tmp_path: Path) -> None:
    """
    Scenario:
        Ensure that cleaning up expired table snapshots also cleans up any unexpired view snapshots that depend on them
        - We create a A (table) <- B (view)
        - In dev, we modify A - triggers new version of A and a dev preview of B that both expire in 7 days
        - We advance time by 3 days
        - In dev, we modify B - triggers a new version of B that depends on A but expires 3 days after A
        - In dev, we create B(view) <- C(view) and B(view) <- D(table)
        - We advance time by 5 days so that A has reached its expiry but B, C and D have not
        - We expire dev so that none of these snapshots are promoted and are thus targets for cleanup
        - We run the janitor

    Expected outcome:
        - All the dev versions of A and B should be dropped
        - C should be dropped as well because it's a view that depends on B which was dropped
        - D should not be dropped because while it depends on B which was dropped, it's a table so is still valid after B is dropped
        - We should NOT get a 'ERROR: cannot drop table x because other objects depend on it'

    Note that the references in state to the views that were cascade-dropped by postgres will still exist, this is considered ok
    as applying a plan will recreate the physical objects
    """

    def _all_snapshot_ids(context: Context) -> t.List[SnapshotId]:
        assert isinstance(context.state_sync, CachingStateSync)
        assert isinstance(context.state_sync.state_sync, EngineAdapterStateSync)

        return [
            SnapshotId(name=name, identifier=identifier)
            for name, identifier in context.state_sync.state_sync.engine_adapter.fetchall(
                "select name, identifier from sqlmesh._snapshots"
            )
        ]

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    schema = exp.to_table(ctx.schema(TEST_SCHEMA)).this

    (models_dir / "model_a.sql").write_text(f"""
    MODEL (
        name {schema}.model_a,
        kind FULL
    );
    SELECT 1 as a, 2 as b;
    """)

    (models_dir / "model_b.sql").write_text(f"""
    MODEL (
        name {schema}.model_b,
        kind VIEW
    );
    SELECT a from {schema}.model_a;
    """)

    def _mutate_config(gateway: str, config: Config):
        config.gateways[gateway].state_connection = DuckDBConnectionConfig(
            database=str(tmp_path / "state.db")
        )

    with time_machine.travel("2020-01-01 00:00:00"):
        sqlmesh = ctx.create_context(
            path=tmp_path, config_mutator=_mutate_config, ephemeral_state_connection=False
        )
        sqlmesh.plan(auto_apply=True)

    model_a_snapshot = next(s for n, s in sqlmesh.snapshots.items() if "model_a" in n)
    # expiry is last updated + ttl
    assert timedelta(milliseconds=model_a_snapshot.ttl_ms) == timedelta(weeks=1)
    assert to_ds(model_a_snapshot.updated_ts) == "2020-01-01"
    assert to_ds(model_a_snapshot.expiration_ts) == "2020-01-08"

    model_b_snapshot = next(s for n, s in sqlmesh.snapshots.items() if "model_b" in n)
    assert timedelta(milliseconds=model_b_snapshot.ttl_ms) == timedelta(weeks=1)
    assert to_ds(model_b_snapshot.updated_ts) == "2020-01-01"
    assert to_ds(model_b_snapshot.expiration_ts) == "2020-01-08"

    model_a_prod_snapshot = model_a_snapshot
    model_b_prod_snapshot = model_b_snapshot

    # move forward 1 days
    # new dev environment - touch models to create new snapshots
    # model a / b expiry in prod should remain unmodified
    # model a / b expiry in dev should be as at today
    with time_machine.travel("2020-01-02 00:00:00"):
        (models_dir / "model_a.sql").write_text(f"""
        MODEL (
            name {schema}.model_a,
            kind FULL
        );
        SELECT 1 as a, 2 as b, 3 as c;
        """)

        sqlmesh = ctx.create_context(
            path=tmp_path, config_mutator=_mutate_config, ephemeral_state_connection=False
        )
        sqlmesh.plan(environment="dev", auto_apply=True)

        # should now have 4 snapshots in state - 2x model a and 2x model b
        # the new model b is a dev preview because its upstream model changed
        all_snapshot_ids = _all_snapshot_ids(sqlmesh)
        assert len(all_snapshot_ids) == 4
        assert len([s for s in all_snapshot_ids if "model_a" in s.name]) == 2
        assert len([s for s in all_snapshot_ids if "model_b" in s.name]) == 2

        # context just has the two latest
        assert len(sqlmesh.snapshots) == 2

        # these expire 1 day later than what's in prod
        model_a_snapshot = next(s for n, s in sqlmesh.snapshots.items() if "model_a" in n)
        assert timedelta(milliseconds=model_a_snapshot.ttl_ms) == timedelta(weeks=1)
        assert to_ds(model_a_snapshot.updated_ts) == "2020-01-02"
        assert to_ds(model_a_snapshot.expiration_ts) == "2020-01-09"

        model_b_snapshot = next(s for n, s in sqlmesh.snapshots.items() if "model_b" in n)
        assert timedelta(milliseconds=model_b_snapshot.ttl_ms) == timedelta(weeks=1)
        assert to_ds(model_b_snapshot.updated_ts) == "2020-01-02"
        assert to_ds(model_b_snapshot.expiration_ts) == "2020-01-09"

    # move forward 3 days
    # touch model b in dev but leave model a
    # this bumps the model b expiry but model a remains unchanged, so will expire before model b even though model b depends on it
    with time_machine.travel("2020-01-05 00:00:00"):
        (models_dir / "model_b.sql").write_text(f"""
        MODEL (
            name {schema}.model_b,
            kind VIEW
        );
        SELECT a, 'b' as b from {schema}.model_a;
        """)

        (models_dir / "model_c.sql").write_text(f"""
        MODEL (
            name {schema}.model_c,
            kind VIEW
        );
        SELECT a, 'c' as c from {schema}.model_b;
        """)

        (models_dir / "model_d.sql").write_text(f"""
        MODEL (
            name {schema}.model_d,
            kind FULL
        );
        SELECT a, 'd' as d from {schema}.model_b;
        """)

        sqlmesh = ctx.create_context(
            path=tmp_path, config_mutator=_mutate_config, ephemeral_state_connection=False
        )
        # need run=True to prevent a "start date is greater than end date" error
        # since dev cant exceed what is in prod, and prod has no cadence runs,
        # without run=True this plan gets start=2020-01-04 (now) end=2020-01-01 (last prod interval) which fails
        sqlmesh.plan(environment="dev", auto_apply=True, run=True)

        # should now have 7 snapshots in state - 2x model a, 3x model b, 1x model c and 1x model d
        all_snapshot_ids = _all_snapshot_ids(sqlmesh)
        assert len(all_snapshot_ids) == 7
        assert len([s for s in all_snapshot_ids if "model_a" in s.name]) == 2
        assert len([s for s in all_snapshot_ids if "model_b" in s.name]) == 3
        assert len([s for s in all_snapshot_ids if "model_c" in s.name]) == 1
        assert len([s for s in all_snapshot_ids if "model_d" in s.name]) == 1

        # context just has the 4 latest
        assert len(sqlmesh.snapshots) == 4

        # model a expiry should not have changed
        model_a_snapshot = next(s for n, s in sqlmesh.snapshots.items() if "model_a" in n)
        assert timedelta(milliseconds=model_a_snapshot.ttl_ms) == timedelta(weeks=1)
        assert to_ds(model_a_snapshot.updated_ts) == "2020-01-02"
        assert to_ds(model_a_snapshot.expiration_ts) == "2020-01-09"

        # model b should now expire well after model a
        model_b_snapshot = next(s for n, s in sqlmesh.snapshots.items() if "model_b" in n)
        assert timedelta(milliseconds=model_b_snapshot.ttl_ms) == timedelta(weeks=1)
        assert to_ds(model_b_snapshot.updated_ts) == "2020-01-05"
        assert to_ds(model_b_snapshot.expiration_ts) == "2020-01-12"

        # model c should expire at the same time as model b
        model_c_snapshot = next(s for n, s in sqlmesh.snapshots.items() if "model_c" in n)
        assert to_ds(model_c_snapshot.updated_ts) == to_ds(model_b_snapshot.updated_ts)
        assert to_ds(model_c_snapshot.expiration_ts) == to_ds(model_b_snapshot.expiration_ts)

        # model d should expire at the same time as model b
        model_d_snapshot = next(s for n, s in sqlmesh.snapshots.items() if "model_d" in n)
        assert to_ds(model_d_snapshot.updated_ts) == to_ds(model_b_snapshot.updated_ts)
        assert to_ds(model_d_snapshot.expiration_ts) == to_ds(model_b_snapshot.expiration_ts)

    # move forward to date where after model a has expired but before model b has expired
    # invalidate dev to trigger cleanups
    # run janitor
    # - table model a is expired so will be cleaned up and this will cascade to view model b
    # - view model b is not expired, but because it got cascaded to, this will cascade again to view model c
    # - table model d is a not a view, so even though its parent view model b got dropped, it doesnt need to be dropped
    with time_machine.travel("2020-01-10 00:00:00"):
        sqlmesh = ctx.create_context(
            path=tmp_path, config_mutator=_mutate_config, ephemeral_state_connection=False
        )

        before_snapshot_ids = _all_snapshot_ids(sqlmesh)

        before_objects = ctx.get_metadata_results(f"sqlmesh__{schema}")
        assert set(before_objects.tables) == set(
            [
                exp.to_table(s.table_name()).text("this")
                for s in (model_a_prod_snapshot, model_a_snapshot, model_d_snapshot)
            ]
        )
        assert set(before_objects.views).issuperset(
            [
                exp.to_table(s.table_name()).text("this")
                for s in (model_b_prod_snapshot, model_b_snapshot, model_c_snapshot)
            ]
        )

        sqlmesh.invalidate_environment("dev")
        sqlmesh.run_janitor(ignore_ttl=False)

        after_snapshot_ids = _all_snapshot_ids(sqlmesh)

        assert len(before_snapshot_ids) != len(after_snapshot_ids)

        # Everything should be left in state except the model_a snapshot, which expired
        assert set(after_snapshot_ids) == set(before_snapshot_ids) - set(
            [model_a_snapshot.snapshot_id]
        )

        # In the db, there should be:
        # - the two original snapshots that were in prod, table model_a and view model_b
        # - model d, even though its not promoted in any environment, because it's a table snapshot that hasnt expired yet
        # the view snapshots that depended on model_a should be gone due to the cascading delete
        after_objects = ctx.get_metadata_results(f"sqlmesh__{schema}")
        assert set(after_objects.tables) == set(
            [
                exp.to_table(s.table_name()).text("this")
                for s in (model_a_prod_snapshot, model_d_snapshot)
            ]
        )
        assert after_objects.views == [
            exp.to_table(model_b_prod_snapshot.table_name()).text("this")
        ]
