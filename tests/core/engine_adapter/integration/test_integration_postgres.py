import typing as t
from contextlib import contextmanager
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
from sqlmesh.utils import random_id

from tests.core.engine_adapter.integration import (
    TestContext,
    generate_pytest_params,
    ENGINES_BY_NAME,
    IntegrationTestEngine,
    TEST_SCHEMA,
)


def _cleanup_user(engine_adapter: PostgresEngineAdapter, user_name: str) -> None:
    """Helper function to clean up a PostgreSQL user and all their dependencies."""
    try:
        engine_adapter.execute(f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE usename = '{user_name}' AND pid <> pg_backend_pid()
        """)
        engine_adapter.execute(f'DROP OWNED BY "{user_name}"')
        engine_adapter.execute(f'DROP USER IF EXISTS "{user_name}"')
    except Exception:
        pass


@contextmanager
def create_users(
    engine_adapter: PostgresEngineAdapter, *role_names: str
) -> t.Iterator[t.Dict[str, t.Dict[str, str]]]:
    """Create a set of Postgres users and yield their credentials."""
    created_users = []
    roles = {}

    try:
        for role_name in role_names:
            user_name = f"test_{role_name}"
            _cleanup_user(engine_adapter, user_name)

        for role_name in role_names:
            user_name = f"test_{role_name}"
            password = random_id()
            engine_adapter.execute(f"CREATE USER \"{user_name}\" WITH PASSWORD '{password}'")
            engine_adapter.execute(f'GRANT USAGE ON SCHEMA public TO "{user_name}"')
            created_users.append(user_name)
            roles[role_name] = {"username": user_name, "password": password}

        yield roles

    finally:
        for user_name in created_users:
            _cleanup_user(engine_adapter, user_name)


def create_engine_adapter_for_role(
    role_credentials: t.Dict[str, str], ctx: TestContext, config: Config
) -> PostgresEngineAdapter:
    """Create a PostgreSQL adapter for a specific role to test authentication and permissions."""
    from sqlmesh.core.config import PostgresConnectionConfig

    gateway = ctx.gateway
    assert gateway in config.gateways
    connection_config = config.gateways[gateway].connection
    assert isinstance(connection_config, PostgresConnectionConfig)

    role_connection_config = PostgresConnectionConfig(
        host=connection_config.host,
        port=connection_config.port,
        database=connection_config.database,
        user=role_credentials["username"],
        password=role_credentials["password"],
        keepalives_idle=connection_config.keepalives_idle,
        connect_timeout=connection_config.connect_timeout,
        role=connection_config.role,
        sslmode=connection_config.sslmode,
        application_name=connection_config.application_name,
    )

    return t.cast(PostgresEngineAdapter, role_connection_config.create_engine_adapter())


@contextmanager
def engine_adapter_for_role(
    role_credentials: t.Dict[str, str], ctx: TestContext, config: Config
) -> t.Iterator[PostgresEngineAdapter]:
    """Context manager that yields a PostgresEngineAdapter and ensures it is closed."""
    adapter = create_engine_adapter_for_role(role_credentials, ctx, config)
    try:
        yield adapter
    finally:
        adapter.close()


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


# Grants Integration Tests


def test_grants_apply_on_table(
    engine_adapter: PostgresEngineAdapter, ctx: TestContext, config: Config
):
    with create_users(engine_adapter, "reader", "writer", "admin") as roles:
        table = ctx.table("grants_test_table")
        engine_adapter.create_table(
            table, {"id": exp.DataType.build("INT"), "name": exp.DataType.build("VARCHAR(50)")}
        )

        engine_adapter.execute(f"INSERT INTO {table} VALUES (1, 'test')")

        grants_config = {
            "SELECT": [roles["reader"]["username"], roles["admin"]["username"]],
            "INSERT": [roles["writer"]["username"], roles["admin"]["username"]],
            "DELETE": [roles["admin"]["username"]],
        }

        engine_adapter._apply_grants_config(table, grants_config)

        schema_name = table.db
        for role_data in roles.values():
            engine_adapter.execute(
                f'GRANT USAGE ON SCHEMA "{schema_name}" TO "{role_data["username"]}"'
            )

        current_grants = engine_adapter._get_current_grants_config(table)

        assert "SELECT" in current_grants
        assert roles["reader"]["username"] in current_grants["SELECT"]
        assert roles["admin"]["username"] in current_grants["SELECT"]

        assert "INSERT" in current_grants
        assert roles["writer"]["username"] in current_grants["INSERT"]
        assert roles["admin"]["username"] in current_grants["INSERT"]

        assert "DELETE" in current_grants
        assert roles["admin"]["username"] in current_grants["DELETE"]

        # Reader should be able to SELECT but not INSERT
        with engine_adapter_for_role(roles["reader"], ctx, config) as reader_adapter:
            result = reader_adapter.fetchone(f"SELECT COUNT(*) FROM {table}")
            assert result == (1,), "Reader should be able to SELECT from table"

        with engine_adapter_for_role(roles["reader"], ctx, config) as reader_adapter:
            with pytest.raises(Exception):
                reader_adapter.execute(f"INSERT INTO {table} VALUES (2, 'test2')")

        # Writer should be able to INSERT but not SELECT
        with engine_adapter_for_role(roles["writer"], ctx, config) as writer_adapter:
            writer_adapter.execute(f"INSERT INTO {table} VALUES (3, 'test3')")

        with engine_adapter_for_role(roles["writer"], ctx, config) as writer_adapter:
            with pytest.raises(Exception):
                writer_adapter.fetchone(f"SELECT COUNT(*) FROM {table}")

        # Admin should be able to SELECT, INSERT, and DELETE
        with engine_adapter_for_role(roles["admin"], ctx, config) as admin_adapter:
            result = admin_adapter.fetchone(f"SELECT COUNT(*) FROM {table}")
            assert result == (2,), "Admin should be able to SELECT from table"

            admin_adapter.execute(f"INSERT INTO {table} VALUES (4, 'test4')")
            admin_adapter.execute(f"DELETE FROM {table} WHERE id = 4")


def test_grants_apply_on_view(
    engine_adapter: PostgresEngineAdapter, ctx: TestContext, config: Config
):
    with create_users(engine_adapter, "reader", "admin") as roles:
        base_table = ctx.table("grants_base_table")
        engine_adapter.create_table(
            base_table,
            {"id": exp.DataType.build("INT"), "value": exp.DataType.build("VARCHAR(50)")},
        )

        view_table = ctx.table("grants_test_view")
        engine_adapter.create_view(view_table, exp.select().from_(base_table))

        # Grant schema access for authentication tests
        test_schema = view_table.db
        for role_credentials in roles.values():
            engine_adapter.execute(
                f'GRANT USAGE ON SCHEMA "{test_schema}" TO "{role_credentials["username"]}"'
            )

        grants_config = {"SELECT": [roles["reader"]["username"], roles["admin"]["username"]]}

        engine_adapter._apply_grants_config(view_table, grants_config)

        current_grants = engine_adapter._get_current_grants_config(view_table)
        assert "SELECT" in current_grants
        assert roles["reader"]["username"] in current_grants["SELECT"]
        assert roles["admin"]["username"] in current_grants["SELECT"]

        # Test actual authentication - reader should be able to SELECT from view
        with engine_adapter_for_role(roles["reader"], ctx, config) as reader_adapter:
            reader_adapter.fetchone(f"SELECT COUNT(*) FROM {view_table}")


def test_grants_revoke(engine_adapter: PostgresEngineAdapter, ctx: TestContext, config: Config):
    with create_users(engine_adapter, "reader", "writer") as roles:
        table = ctx.table("grants_revoke_test")
        engine_adapter.create_table(table, {"id": exp.DataType.build("INT")})
        engine_adapter.execute(f"INSERT INTO {table} VALUES (1)")

        # Grant schema access for authentication tests
        test_schema = table.db
        for role_credentials in roles.values():
            engine_adapter.execute(
                f'GRANT USAGE ON SCHEMA "{test_schema}" TO "{role_credentials["username"]}"'
            )

        initial_grants = {
            "SELECT": [roles["reader"]["username"], roles["writer"]["username"]],
            "INSERT": [roles["writer"]["username"]],
        }
        engine_adapter._apply_grants_config(table, initial_grants)

        initial_current_grants = engine_adapter._get_current_grants_config(table)
        assert roles["reader"]["username"] in initial_current_grants.get("SELECT", [])
        assert roles["writer"]["username"] in initial_current_grants.get("SELECT", [])
        assert roles["writer"]["username"] in initial_current_grants.get("INSERT", [])

        # Verify reader can SELECT before revoke
        with engine_adapter_for_role(roles["reader"], ctx, config) as reader_adapter:
            reader_adapter.fetchone(f"SELECT COUNT(*) FROM {table}")

        revoke_grants = {
            "SELECT": [roles["reader"]["username"]],
            "INSERT": [roles["writer"]["username"]],
        }
        engine_adapter._revoke_grants_config(table, revoke_grants)

        current_grants_after = engine_adapter._get_current_grants_config(table)

        assert roles["reader"]["username"] not in current_grants_after.get("SELECT", [])
        assert roles["writer"]["username"] in current_grants_after.get("SELECT", [])
        assert roles["writer"]["username"] not in current_grants_after.get("INSERT", [])

        # Verify reader can NO LONGER SELECT after revoke
        with engine_adapter_for_role(roles["reader"], ctx, config) as reader_adapter:
            with pytest.raises(Exception):
                reader_adapter.fetchone(f"SELECT COUNT(*) FROM {table}")

        # Verify writer can still SELECT but not INSERT after revoke
        with engine_adapter_for_role(roles["writer"], ctx, config) as writer_adapter:
            result = writer_adapter.fetchone(f"SELECT COUNT(*) FROM {table}")
            assert result is not None
            assert result[0] == 1
        with engine_adapter_for_role(roles["writer"], ctx, config) as writer_adapter:
            with pytest.raises(Exception):
                writer_adapter.execute(f"INSERT INTO {table} VALUES (2)")


def test_grants_sync(engine_adapter: PostgresEngineAdapter, ctx: TestContext, config: Config):
    with create_users(engine_adapter, "user1", "user2", "user3") as roles:
        table = ctx.table("grants_sync_test")
        engine_adapter.create_table(
            table, {"id": exp.DataType.build("INT"), "data": exp.DataType.build("TEXT")}
        )

        initial_grants = {
            "SELECT": [roles["user1"]["username"], roles["user2"]["username"]],
            "INSERT": [roles["user1"]["username"]],
        }
        engine_adapter._apply_grants_config(table, initial_grants)

        initial_current_grants = engine_adapter._get_current_grants_config(table)
        assert roles["user1"]["username"] in initial_current_grants.get("SELECT", [])
        assert roles["user2"]["username"] in initial_current_grants.get("SELECT", [])
        assert roles["user1"]["username"] in initial_current_grants.get("INSERT", [])

        target_grants = {
            "SELECT": [roles["user2"]["username"], roles["user3"]["username"]],
            "UPDATE": [roles["user3"]["username"]],
        }
        engine_adapter._sync_grants_config(table, target_grants)

        final_grants = engine_adapter._get_current_grants_config(table)

        assert set(final_grants.get("SELECT", [])) == {
            roles["user2"]["username"],
            roles["user3"]["username"],
        }
        assert set(final_grants.get("UPDATE", [])) == {roles["user3"]["username"]}
        assert final_grants.get("INSERT", []) == []


def test_grants_sync_empty_config(
    engine_adapter: PostgresEngineAdapter, ctx: TestContext, config: Config
):
    with create_users(engine_adapter, "user") as roles:
        table = ctx.table("grants_empty_test")
        engine_adapter.create_table(table, {"id": exp.DataType.build("INT")})

        initial_grants = {
            "SELECT": [roles["user"]["username"]],
            "INSERT": [roles["user"]["username"]],
        }
        engine_adapter._apply_grants_config(table, initial_grants)

        initial_current_grants = engine_adapter._get_current_grants_config(table)
        assert roles["user"]["username"] in initial_current_grants.get("SELECT", [])
        assert roles["user"]["username"] in initial_current_grants.get("INSERT", [])

        engine_adapter._sync_grants_config(table, {})

        final_grants = engine_adapter._get_current_grants_config(table)
        assert final_grants == {}


def test_grants_case_insensitive_grantees(
    engine_adapter: PostgresEngineAdapter, ctx: TestContext, config: Config
):
    with create_users(engine_adapter, "test_reader", "test_writer") as roles:
        table = ctx.table("grants_quoted_test")
        engine_adapter.create_table(table, {"id": exp.DataType.build("INT")})

        test_schema = table.db
        for role_credentials in roles.values():
            engine_adapter.execute(
                f'GRANT USAGE ON SCHEMA "{test_schema}" TO "{role_credentials["username"]}"'
            )

        reader = roles["test_reader"]["username"]
        writer = roles["test_writer"]["username"]

        grants_config = {"SELECT": [reader, writer.upper()]}
        engine_adapter._apply_grants_config(table, grants_config)

        # Grantees are still in lowercase
        current_grants = engine_adapter._get_current_grants_config(table)
        assert reader in current_grants.get("SELECT", [])
        assert writer in current_grants.get("SELECT", [])

        # Revoke writer
        grants_config = {"SELECT": [reader.upper()]}
        engine_adapter._sync_grants_config(table, grants_config)

        current_grants = engine_adapter._get_current_grants_config(table)
        assert reader in current_grants.get("SELECT", [])
        assert writer not in current_grants.get("SELECT", [])
