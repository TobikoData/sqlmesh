import json
import typing as t
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core.dialect import parse_one
from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    ModelKind,
    ModelKindName,
    Seed,
    SeedKind,
    SeedModel,
    SqlModel,
)
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotTableInfo,
    missing_intervals,
)
from sqlmesh.core.state_sync import CachingStateSync, EngineAdapterStateSync
from sqlmesh.core.state_sync.base import SCHEMA_VERSION, SQLGLOT_VERSION, Versions
from sqlmesh.utils.date import now_timestamp, to_datetime, to_timestamp
from sqlmesh.utils.errors import SQLMeshError


@pytest.fixture
def state_sync(duck_conn):
    state_sync = EngineAdapterStateSync(
        create_engine_adapter(lambda: duck_conn, "duckdb"), schema=c.SQLMESH
    )
    state_sync.migrate()
    return state_sync


@pytest.fixture
def snapshots(make_snapshot: t.Callable) -> t.List[Snapshot]:
    return [
        make_snapshot(
            SqlModel(
                name="a",
                query=parse_one("select 1, ds"),
            ),
            version="a",
        ),
        make_snapshot(
            SqlModel(
                name="b",
                query=parse_one("select 2, ds"),
            ),
            version="b",
        ),
    ]


def promote_snapshots(
    state_sync: EngineAdapterStateSync,
    snapshots: t.List[Snapshot],
    environment: str,
    no_gaps: bool = False,
) -> t.Tuple[t.List[SnapshotTableInfo], t.List[SnapshotTableInfo]]:
    env = Environment(
        name=environment,
        snapshots=[snapshot.table_info for snapshot in snapshots],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id="test_plan_id",
    )
    return state_sync.promote(env, no_gaps=no_gaps)


def delete_versions(state_sync: EngineAdapterStateSync) -> None:
    state_sync.engine_adapter.drop_table(state_sync.versions_table)


def test_push_snapshots(
    state_sync: EngineAdapterStateSync,
    make_snapshot: t.Callable,
) -> None:
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
        )
    )
    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            query=parse_one("select 2, ds"),
        )
    )

    with pytest.raises(
        SQLMeshError,
        match=r".*has not been versioned.*",
    ):
        state_sync.push_snapshots([snapshot_a, snapshot_b])

    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot_b.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    snapshot_b.version = "2"

    state_sync.push_snapshots([snapshot_a, snapshot_b])

    assert state_sync.get_snapshots([snapshot_a.snapshot_id, snapshot_b.snapshot_id]) == {
        snapshot_a.snapshot_id: snapshot_a,
        snapshot_b.snapshot_id: snapshot_b,
    }

    with pytest.raises(
        SQLMeshError,
        match=r".*already exists.*",
    ):
        state_sync.push_snapshots([snapshot_a])

    with pytest.raises(
        SQLMeshError,
        match=r".*already exists.*",
    ):
        state_sync.push_snapshots([snapshot_a, snapshot_b])

    # test serialization
    state_sync.push_snapshots(
        [
            make_snapshot(
                SqlModel(
                    name="a",
                    kind=ModelKind(name=ModelKindName.FULL),
                    query=parse_one(
                        """
            select 'x' + ' ' as y,
                    "z" + '\' as z,
        """
                    ),
                ),
                version="1",
            )
        ]
    )


def test_duplicates(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable) -> None:
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
        ),
        version="1",
    )
    snapshot_b = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
        ),
        version="1",
    )
    snapshot_c = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
        ),
        version="1",
    )
    snapshot_b.updated_ts = snapshot_a.updated_ts + 1
    snapshot_c.updated_ts = 0
    state_sync.push_snapshots([snapshot_a])
    state_sync._push_snapshots([snapshot_a])
    state_sync._push_snapshots([snapshot_b])
    state_sync._push_snapshots([snapshot_c])
    assert (
        state_sync.get_snapshots([snapshot_a])[snapshot_a.snapshot_id].updated_ts
        == snapshot_b.updated_ts
    )


def test_delete_snapshots(state_sync: EngineAdapterStateSync, snapshots: t.List[Snapshot]) -> None:
    state_sync.push_snapshots(snapshots)
    snapshot_ids = [s.snapshot_id for s in snapshots]
    assert state_sync.get_snapshots(snapshot_ids)
    state_sync.delete_snapshots(snapshot_ids)
    assert not state_sync.get_snapshots(snapshot_ids)


def test_snapshots_exists(state_sync: EngineAdapterStateSync, snapshots: t.List[Snapshot]) -> None:
    state_sync.push_snapshots(snapshots)
    snapshot_ids = {snapshot.snapshot_id for snapshot in snapshots}
    assert state_sync.snapshots_exist(snapshot_ids) == snapshot_ids


def get_snapshot_intervals(state_sync, snapshot):
    return state_sync._get_snapshot_intervals([snapshot])[-1][0]


def test_add_interval(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable) -> None:
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 1, ds"),
        ),
        version="a",
    )

    state_sync.push_snapshots([snapshot])

    state_sync.add_interval(snapshot, "2020-01-01", "20200101")
    assert get_snapshot_intervals(state_sync, snapshot).intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-02")),
    ]

    state_sync.add_interval(snapshot, "20200101", to_datetime("2020-01-04"))
    assert get_snapshot_intervals(state_sync, snapshot).intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-04")),
    ]

    state_sync.add_interval(snapshot, to_datetime("2020-01-05"), "2020-01-10")
    assert get_snapshot_intervals(state_sync, snapshot).intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-04")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-11")),
    ]

    state_sync.add_interval(snapshot, to_datetime("2020-01-11"), "2020-01-15", is_dev=True)
    assert get_snapshot_intervals(state_sync, snapshot).intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-04")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-16")),
    ]

    snapshot.change_category = SnapshotChangeCategory.FORWARD_ONLY
    state_sync.add_interval(snapshot, to_datetime("2020-01-16"), "2020-01-20", is_dev=True)
    intervals = get_snapshot_intervals(state_sync, snapshot)
    assert intervals.intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-04")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-16")),
    ]
    assert intervals.dev_intervals == [
        (to_timestamp("2020-01-16"), to_timestamp("2020-01-21")),
    ]


def test_remove_interval(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable) -> None:
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 1, ds"),
        ),
        version="a",
    )
    snapshot_b = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 2::INT, '2022-01-01'::TEXT AS ds"),
        ),
        version="a",
    )
    state_sync.push_snapshots([snapshot_a, snapshot_b])
    state_sync.add_interval(snapshot_a, "2020-01-01", "2020-01-10")
    state_sync.add_interval(snapshot_b, "2020-01-11", "2020-01-30")

    state_sync.remove_interval([snapshot_a], "2020-01-15", "2020-01-17")

    snapshots = state_sync.get_snapshots([snapshot_a, snapshot_b])

    assert snapshots[snapshot_a.snapshot_id].intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-15")),
        (to_timestamp("2020-01-18"), to_timestamp("2020-01-31")),
    ]
    assert snapshots[snapshot_b.snapshot_id].intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-15")),
        (to_timestamp("2020-01-18"), to_timestamp("2020-01-31")),
    ]


def test_get_snapshot_intervals(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
) -> None:
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 1, ds"),
        ),
        version="a",
    )

    state_sync.push_snapshots([snapshot_a])
    state_sync.add_interval(snapshot_a, "2020-01-01", "2020-01-01")

    snapshot_b = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 2, ds"),
        ),
        version="a",
    )
    state_sync.push_snapshots([snapshot_b])

    assert get_snapshot_intervals(state_sync, snapshot_b).intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-02")),
    ]


def test_compact_intervals(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable) -> None:
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 1, ds"),
        ),
        version="a",
    )

    state_sync.push_snapshots([snapshot])

    state_sync.add_interval(snapshot, "2020-01-01", "2020-01-10")
    state_sync.add_interval(snapshot, "2020-01-11", "2020-01-15")
    state_sync.remove_interval([snapshot], "2020-01-05", "2020-01-12")
    state_sync.add_interval(snapshot, "2020-01-12", "2020-01-16")
    state_sync.remove_interval([snapshot], "2020-01-14", "2020-01-16")

    expected_intervals = [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-05")),
        (to_timestamp("2020-01-12"), to_timestamp("2020-01-14")),
    ]

    assert get_snapshot_intervals(state_sync, snapshot).intervals == expected_intervals

    state_sync.compact_intervals()
    assert get_snapshot_intervals(state_sync, snapshot).intervals == expected_intervals

    # Make sure compaction is idempotent.
    state_sync.compact_intervals()
    assert get_snapshot_intervals(state_sync, snapshot).intervals == expected_intervals


def test_promote_snapshots(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
        ),
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            kind=ModelKind(name=ModelKindName.FULL),
            query=parse_one("select * from a"),
        ),
        nodes={"a": snapshot_a.model},
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_c = make_snapshot(
        SqlModel(
            name="c",
            query=parse_one("select 3, ds"),
        ),
    )
    snapshot_c.categorize_as(SnapshotChangeCategory.BREAKING)

    with pytest.raises(
        SQLMeshError,
        match=r"Missing snapshots.*",
    ):
        promote_snapshots(state_sync, [snapshot_a], "prod")

    state_sync.push_snapshots([snapshot_a, snapshot_b, snapshot_c])

    added, removed = promote_snapshots(state_sync, [snapshot_a, snapshot_b], "prod")

    assert set(added) == set([snapshot_a.table_info, snapshot_b.table_info])
    assert not removed
    added, removed = promote_snapshots(
        state_sync,
        [snapshot_a, snapshot_b, snapshot_c],
        "prod",
    )
    assert set(added) == set(
        [
            snapshot_a.table_info,
            snapshot_b.table_info,
            snapshot_c.table_info,
        ]
    )
    assert not removed

    added, removed = promote_snapshots(
        state_sync,
        [snapshot_a, snapshot_b],
        "prod",
    )
    assert set(added) == {snapshot_a.table_info, snapshot_b.table_info}
    assert set(removed) == {snapshot_c.table_info}

    snapshot_d = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 2, ds"),
        ),
    )
    snapshot_d.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot_d])
    added, removed = promote_snapshots(state_sync, [snapshot_d], "prod")
    assert set(added) == {snapshot_d.table_info}
    assert set(removed) == {snapshot_b.table_info}


def test_promote_snapshots_parent_plan_id_mismatch(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
        ),
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot])
    promote_snapshots(state_sync, [snapshot], "prod")

    new_environment = Environment(
        name="prod",
        snapshots=[snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="new_plan_id",
        previous_plan_id="test_plan_id",
    )

    stale_new_environment = Environment(
        name="prod",
        snapshots=[snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="stale_new_plan_id",
        previous_plan_id="test_plan_id",
    )

    state_sync.promote(new_environment)

    with pytest.raises(
        SQLMeshError,
        match=r".*is no longer valid.*",
    ):
        state_sync.promote(stale_new_environment)


def test_promote_snapshots_no_gaps(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    model = SqlModel(
        name="a",
        query=parse_one("select 1, ds"),
        kind=IncrementalByTimeRangeKind(time_column="ds"),
        cron="@daily",
    )

    snapshot = make_snapshot(model, version="a")
    snapshot.change_category = SnapshotChangeCategory.BREAKING
    state_sync.push_snapshots([snapshot])
    state_sync.add_interval(snapshot, "2022-01-01", "2022-01-02")
    promote_snapshots(state_sync, [snapshot], "prod", no_gaps=True)

    new_snapshot_same_version = make_snapshot(model, version="a")
    new_snapshot_same_version.change_category = SnapshotChangeCategory.INDIRECT_NON_BREAKING
    new_snapshot_same_version.fingerprint = snapshot.fingerprint.copy(
        update={"data_hash": "new_snapshot_same_version"}
    )
    state_sync.push_snapshots([new_snapshot_same_version])
    state_sync.add_interval(new_snapshot_same_version, "2022-01-03", "2022-01-03")
    promote_snapshots(state_sync, [new_snapshot_same_version], "prod", no_gaps=True)

    new_snapshot_missing_interval = make_snapshot(model, version="b")
    new_snapshot_missing_interval.change_category = SnapshotChangeCategory.BREAKING
    new_snapshot_missing_interval.fingerprint = snapshot.fingerprint.copy(
        update={"data_hash": "new_snapshot_missing_interval"}
    )
    state_sync.push_snapshots([new_snapshot_missing_interval])
    state_sync.add_interval(new_snapshot_missing_interval, "2022-01-01", "2022-01-02")
    with pytest.raises(
        SQLMeshError,
        match=r"Detected gaps in snapshot.*",
    ):
        promote_snapshots(state_sync, [new_snapshot_missing_interval], "prod", no_gaps=True)

    new_snapshot_same_interval = make_snapshot(model, version="c")
    new_snapshot_same_interval.change_category = SnapshotChangeCategory.BREAKING
    new_snapshot_same_interval.fingerprint = snapshot.fingerprint.copy(
        update={"data_hash": "new_snapshot_same_interval"}
    )
    state_sync.push_snapshots([new_snapshot_same_interval])
    state_sync.add_interval(new_snapshot_same_interval, "2022-01-01", "2022-01-03")
    promote_snapshots(state_sync, [new_snapshot_same_interval], "prod", no_gaps=True)


def test_finalize(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
        ),
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot_a])
    promote_snapshots(state_sync, [snapshot_a], "prod")

    env = state_sync.get_environment("prod")
    assert env
    state_sync.finalize(env)

    env = state_sync.get_environment("prod")
    assert env
    assert env.finalized_ts is not None

    env.plan_id = "different_plan_id"
    with pytest.raises(
        SQLMeshError,
        match=r"Plan 'different_plan_id' is no longer valid for the target environment 'prod'.*",
    ):
        state_sync.finalize(env)


def test_start_date_gap(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    model = SqlModel(
        name="a",
        query=parse_one("select 1, ds"),
        start="2022-01-01",
        kind=IncrementalByTimeRangeKind(time_column="ds"),
        cron="@daily",
    )

    snapshot = make_snapshot(model, version="a")
    snapshot.change_category = SnapshotChangeCategory.BREAKING
    state_sync.push_snapshots([snapshot])
    state_sync.add_interval(snapshot, "2022-01-01", "2022-01-03")
    promote_snapshots(state_sync, [snapshot], "prod")

    model = SqlModel(
        name="a",
        query=parse_one("select 1, ds"),
        start="2022-01-02",
        kind=IncrementalByTimeRangeKind(time_column="ds"),
        cron="@daily",
    )

    snapshot = make_snapshot(model, version="b")
    snapshot.change_category = SnapshotChangeCategory.BREAKING
    state_sync.push_snapshots([snapshot])
    state_sync.add_interval(snapshot, "2022-01-03", "2022-01-04")
    with pytest.raises(
        SQLMeshError,
        match=r"Detected gaps in snapshot.*",
    ):
        promote_snapshots(state_sync, [snapshot], "prod", no_gaps=True)

    state_sync.add_interval(snapshot, "2022-01-02", "2022-01-03")
    promote_snapshots(state_sync, [snapshot], "prod", no_gaps=True)


def test_delete_expired_environments(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, ds"),
        ),
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot])

    now_ts = now_timestamp()

    env_a = Environment(
        name="test_environment_a",
        snapshots=[snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id="test_plan_id",
        expiration_ts=now_ts - 1000,
    )
    state_sync.promote(env_a)

    env_b = env_a.copy(update={"name": "test_environment_b", "expiration_ts": now_ts + 1000})
    state_sync.promote(env_b)

    assert state_sync.get_environment(env_a.name) == env_a
    assert state_sync.get_environment(env_b.name) == env_b

    deleted_environments = state_sync.delete_expired_environments()
    assert deleted_environments == [env_a]

    assert state_sync.get_environment(env_a.name) is None
    assert state_sync.get_environment(env_b.name) == env_b


def test_environment_start_as_timestamp(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, ds"),
        ),
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot])

    now_ts = now_timestamp()

    env = Environment(
        name="test_environment_a",
        snapshots=[snapshot.table_info],
        start_at=now_ts,
        end_at=None,
        plan_id="test_plan_id",
        previous_plan_id="test_plan_id",
        expiration_ts=now_ts - 1000,
    )
    state_sync.promote(env)

    stored_env = state_sync.get_environment(env.name)
    assert stored_env
    assert stored_env.start_at == to_datetime(now_ts).isoformat()


def test_unpause_snapshots(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    snapshot = make_snapshot(
        SqlModel(
            name="test_snapshot",
            query=parse_one("select 1, ds"),
            cron="@daily",
        ),
        version="a",
    )
    assert not snapshot.unpaused_ts
    state_sync.push_snapshots([snapshot])

    unpaused_dt = "2022-01-01"
    state_sync.unpause_snapshots([snapshot], unpaused_dt)

    actual_snapshot = state_sync.get_snapshots([snapshot])[snapshot.snapshot_id]
    assert actual_snapshot.unpaused_ts
    assert actual_snapshot.unpaused_ts == to_timestamp(unpaused_dt)

    new_snapshot = make_snapshot(
        SqlModel(name="test_snapshot", query=parse_one("select 2, ds"), cron="@daily"),
        version="a",
    )
    assert not new_snapshot.unpaused_ts
    state_sync.push_snapshots([new_snapshot])
    state_sync.unpause_snapshots([new_snapshot], unpaused_dt)

    actual_snapshots = state_sync.get_snapshots([snapshot, new_snapshot])
    assert not actual_snapshots[snapshot.snapshot_id].unpaused_ts
    assert actual_snapshots[new_snapshot.snapshot_id].unpaused_ts == to_timestamp(unpaused_dt)


def test_unpause_snapshots_remove_intervals(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
):
    snapshot = make_snapshot(
        SqlModel(
            name="test_snapshot",
            query=parse_one("select 1, ds"),
            cron="@daily",
        ),
        version="a",
    )
    state_sync.push_snapshots([snapshot])
    state_sync.add_interval(snapshot, "2023-01-01", "2023-01-05")

    new_snapshot = make_snapshot(
        SqlModel(name="test_snapshot", query=parse_one("select 2, ds"), cron="@daily"),
        version="a",
    )
    new_snapshot.effective_from = "2023-01-03"
    state_sync.push_snapshots([new_snapshot])
    state_sync.add_interval(snapshot, "2023-01-06", "2023-01-06")
    state_sync.unpause_snapshots([new_snapshot], "2023-01-06")

    actual_snapshots = state_sync.get_snapshots([snapshot, new_snapshot])
    assert actual_snapshots[new_snapshot.snapshot_id].intervals == [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-03")),
    ]
    assert actual_snapshots[snapshot.snapshot_id].intervals == [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-03")),
    ]


def test_get_version(state_sync: EngineAdapterStateSync) -> None:
    # fresh install should not raise
    assert state_sync.get_versions() == Versions(
        schema_version=SCHEMA_VERSION, sqlglot_version=SQLGLOT_VERSION
    )

    # Start with a clean slate.
    state_sync = EngineAdapterStateSync(
        create_engine_adapter(duckdb.connect, "duckdb"), schema=c.SQLMESH
    )

    with pytest.raises(
        SQLMeshError,
        match=rf"SQLMesh \(local\) is using version '{SCHEMA_VERSION}' which is ahead of '0'",
    ):
        state_sync.get_versions()

    state_sync.migrate()

    # migration version is behind, always raise
    state_sync._update_versions(schema_version=SCHEMA_VERSION + 1)
    error = rf"SQLMesh \(local\) is using version '{SCHEMA_VERSION}' which is behind '{SCHEMA_VERSION + 1}'"

    with pytest.raises(SQLMeshError, match=error):
        state_sync.get_versions()

    with pytest.raises(SQLMeshError, match=error):
        state_sync.get_versions(validate=False)

    # migration version is ahead, only raise when validate is true
    state_sync._update_versions(schema_version=SCHEMA_VERSION - 1)
    with pytest.raises(
        SQLMeshError,
        match=rf"SQLMesh \(local\) is using version '{SCHEMA_VERSION}' which is ahead of '{SCHEMA_VERSION - 1}'",
    ):
        state_sync.get_versions()
    state_sync.get_versions(validate=False)

    # patch version sqlglot doesn't matter
    major, minor, patch, *_ = SQLGLOT_VERSION.split(".")
    sqlglot_version = f"{major}.{minor}.{int(patch) + 1}"
    state_sync._update_versions(sqlglot_version=sqlglot_version)
    state_sync.get_versions(validate=False)

    # sqlmesh version is behind, always raise
    sqlglot_version = f"{major}.{int(minor) + 1}.{patch}"
    error = rf"SQLGlot \(local\) is using version '{SQLGLOT_VERSION}' which is behind '{sqlglot_version}'"
    state_sync._update_versions(sqlglot_version=sqlglot_version)
    with pytest.raises(SQLMeshError, match=error):
        state_sync.get_versions(validate=False)

    # sqlmesh version is ahead, only raise with validate is true
    sqlglot_version = f"{major}.{int(minor) - 1}.{patch}"
    error = rf"SQLGlot \(local\) is using version '{SQLGLOT_VERSION}' which is ahead of '{sqlglot_version}'"
    state_sync._update_versions(sqlglot_version=sqlglot_version)
    with pytest.raises(SQLMeshError, match=error):
        state_sync.get_versions()
    state_sync.get_versions(validate=False)


def test_migrate(state_sync: EngineAdapterStateSync, mocker: MockerFixture) -> None:
    migrate_rows_mock = mocker.patch("sqlmesh.core.state_sync.EngineAdapterStateSync._migrate_rows")
    backup_state_mock = mocker.patch("sqlmesh.core.state_sync.EngineAdapterStateSync._backup_state")
    state_sync.migrate()
    migrate_rows_mock.assert_not_called()
    backup_state_mock.assert_not_called()

    # Start with a clean slate.
    state_sync = EngineAdapterStateSync(
        create_engine_adapter(duckdb.connect, "duckdb"), schema=c.SQLMESH
    )

    state_sync.migrate()
    migrate_rows_mock.assert_called_once()
    backup_state_mock.assert_called_once()
    assert state_sync.get_versions() == Versions(
        schema_version=SCHEMA_VERSION, sqlglot_version=SQLGLOT_VERSION
    )


def test_rollback(state_sync: EngineAdapterStateSync, mocker: MockerFixture) -> None:
    with pytest.raises(
        SQLMeshError,
        match="There are no prior migrations to roll back to.",
    ):
        state_sync.rollback()

    restore_table_spy = mocker.spy(state_sync, "_restore_table")
    state_sync._backup_state()

    state_sync.rollback()
    restore_table_spy.assert_any_call("sqlmesh._snapshots", "sqlmesh._snapshots_backup")
    restore_table_spy.assert_any_call("sqlmesh._environments", "sqlmesh._environments_backup")
    restore_table_spy.assert_any_call("sqlmesh._versions", "sqlmesh._versions_backup")
    assert not state_sync.engine_adapter.table_exists("sqlmesh._snapshots_backup")
    assert not state_sync.engine_adapter.table_exists("sqlmesh._environments_backup")
    assert not state_sync.engine_adapter.table_exists("sqlmesh._versions_backup")


def test_migrate_rows(state_sync: EngineAdapterStateSync, mocker: MockerFixture) -> None:
    delete_versions(state_sync)

    state_sync.engine_adapter.replace_query(
        "sqlmesh._snapshots",
        pd.read_json("tests/fixtures/migrations/snapshots.json"),
        columns_to_types={
            "name": exp.DataType.build("text"),
            "identifier": exp.DataType.build("text"),
            "version": exp.DataType.build("text"),
            "snapshot": exp.DataType.build("text"),
        },
    )

    state_sync.engine_adapter.replace_query(
        "sqlmesh._environments",
        pd.read_json("tests/fixtures/migrations/environments.json"),
        columns_to_types={
            "name": exp.DataType.build("text"),
            "snapshots": exp.DataType.build("text"),
            "start_at": exp.DataType.build("text"),
            "end_at": exp.DataType.build("text"),
            "plan_id": exp.DataType.build("text"),
            "previous_plan_id": exp.DataType.build("text"),
            "expiration_ts": exp.DataType.build("bigint"),
        },
    )

    old_snapshots = state_sync.engine_adapter.fetchdf("select * from sqlmesh._snapshots")
    old_environments = state_sync.engine_adapter.fetchdf("select * from sqlmesh._environments")

    state_sync.migrate(skip_backup=True)

    new_snapshots = state_sync.engine_adapter.fetchdf("select * from sqlmesh._snapshots")
    new_environments = state_sync.engine_adapter.fetchdf("select * from sqlmesh._environments")

    assert len(old_snapshots) == len(new_snapshots)
    assert len(old_environments) == len(new_environments)

    start = "2023-01-01"
    end = "2023-01-07"

    assert not missing_intervals(
        state_sync.get_snapshots(
            t.cast(Environment, state_sync.get_environment("staging")).snapshots
        ).values(),
        start=start,
        end=end,
    )

    dev_snapshots = state_sync.get_snapshots(
        t.cast(Environment, state_sync.get_environment("dev")).snapshots
    ).values()

    assert not missing_intervals(dev_snapshots, start=start, end=end)

    assert not missing_intervals(dev_snapshots, start="2023-01-08", end="2023-01-10") == 8

    for s in state_sync.get_snapshots(None).values():
        if not s.is_symbolic:
            assert s.intervals

    customer_revenue_by_day = new_snapshots.loc[
        new_snapshots["name"] == "sushi.customer_revenue_by_day"
    ].iloc[0]
    assert json.loads(customer_revenue_by_day["snapshot"])["node"]["query"].startswith(
        "JINJA_QUERY_BEGIN"
    )


def test_backup_state(state_sync: EngineAdapterStateSync, mocker: MockerFixture) -> None:
    state_sync.engine_adapter.replace_query(
        "sqlmesh._snapshots",
        pd.read_json("tests/fixtures/migrations/snapshots.json"),
        columns_to_types={
            "name": exp.DataType.build("text"),
            "identifier": exp.DataType.build("text"),
            "version": exp.DataType.build("text"),
            "snapshot": exp.DataType.build("text"),
        },
    )

    state_sync._backup_state()
    pd.testing.assert_frame_equal(
        state_sync.engine_adapter.fetchdf("select * from sqlmesh._snapshots"),
        state_sync.engine_adapter.fetchdf("select * from sqlmesh._snapshots_backup"),
    )


def test_restore_snapshots_table(state_sync: EngineAdapterStateSync) -> None:
    snapshot_columns_to_types = {
        "name": exp.DataType.build("text"),
        "identifier": exp.DataType.build("text"),
        "version": exp.DataType.build("text"),
        "snapshot": exp.DataType.build("text"),
    }
    state_sync.engine_adapter.replace_query(
        "sqlmesh._snapshots",
        pd.read_json("tests/fixtures/migrations/snapshots.json"),
        columns_to_types=snapshot_columns_to_types,
    )

    old_snapshots = state_sync.engine_adapter.fetchdf("select * from sqlmesh._snapshots")
    old_snapshots_count = state_sync.engine_adapter.fetchone(
        "select count(*) from sqlmesh._snapshots"
    )
    assert old_snapshots_count == (12,)
    state_sync._backup_state()

    state_sync.engine_adapter.delete_from("sqlmesh._snapshots", "TRUE")
    snapshots_count = state_sync.engine_adapter.fetchone("select count(*) from sqlmesh._snapshots")
    assert snapshots_count == (0,)
    state_sync._restore_table(
        table_name="sqlmesh._snapshots",
        backup_table_name="sqlmesh._snapshots_backup",
    )

    new_snapshots = state_sync.engine_adapter.fetchdf("select * from sqlmesh._snapshots")
    pd.testing.assert_frame_equal(
        old_snapshots,
        new_snapshots,
    )


def test_seed_hydration(
    state_sync: EngineAdapterStateSync,
    make_snapshot: t.Callable,
):
    snapshot = make_snapshot(
        SeedModel(
            name="a",
            kind=SeedKind(path="./path/to/seed"),
            seed=Seed(content="header\n1\n2"),
            column_hashes={"header": "hash"},
            depends_on=set(),
        )
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot])

    assert snapshot.model.is_hydrated
    assert snapshot.model.seed.content == "header\n1\n2"

    stored_snapshot = state_sync.get_snapshots([snapshot.snapshot_id], hydrate_seeds=False)[
        snapshot.snapshot_id
    ]
    assert isinstance(stored_snapshot.model, SeedModel)
    assert not stored_snapshot.model.is_hydrated
    assert stored_snapshot.model.seed.content == ""

    stored_snapshot = state_sync.get_snapshots([snapshot.snapshot_id], hydrate_seeds=True)[
        snapshot.snapshot_id
    ]
    assert isinstance(stored_snapshot.model, SeedModel)
    assert stored_snapshot.model.is_hydrated
    assert stored_snapshot.model.seed.content == "header\n1\n2"


def test_nodes_exist(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
        )
    )

    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    assert not state_sync.nodes_exist([snapshot.name])

    state_sync.push_snapshots([snapshot])

    assert state_sync.nodes_exist([snapshot.name]) == {snapshot.name}


def test_invalidate_environment(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, ds"),
        ),
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot])

    original_expiration_ts = now_timestamp() + 100000

    env = Environment(
        name="test_environment",
        snapshots=[snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id="test_plan_id",
        expiration_ts=original_expiration_ts,
    )
    state_sync.promote(env)

    assert not state_sync.delete_expired_environments()
    state_sync.invalidate_environment("test_environment")

    stored_env = state_sync.get_environment("test_environment")
    assert stored_env
    assert stored_env.expiration_ts and stored_env.expiration_ts < original_expiration_ts

    deleted_environments = state_sync.delete_expired_environments()
    assert len(deleted_environments) == 1
    assert deleted_environments[0].name == "test_environment"

    with pytest.raises(SQLMeshError, match="Cannot invalidate the production environment."):
        state_sync.invalidate_environment("prod")


def test_cache(state_sync, make_snapshot, mocker):
    cache = CachingStateSync(state_sync, ttl=10)

    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 'a', 'ds'"),
        ),
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    now_timestamp = mocker.patch("sqlmesh.core.state_sync.cache.now_timestamp")
    now_timestamp.return_value = to_timestamp("2023-01-01 00:00:00")

    # prime the cache with a cached missing snapshot
    assert not cache.get_snapshots([snapshot.snapshot_id])

    # item is cached and shouldn't hit state sync
    with patch.object(state_sync, "get_snapshots") as mock:
        assert not cache.get_snapshots([snapshot.snapshot_id])
        mock.assert_not_called()

    # prime the cache with a real snapshot
    cache.push_snapshots([snapshot])
    assert cache.get_snapshots([snapshot.snapshot_id]) == {snapshot.snapshot_id: snapshot}

    # cache hit
    with patch.object(state_sync, "get_snapshots") as mock:
        assert cache.get_snapshots([snapshot.snapshot_id]) == {snapshot.snapshot_id: snapshot}
        mock.assert_not_called()

    # clear the cache by adding intervals
    cache.add_interval(snapshot, "2020-01-01", "2020-01-01")
    with patch.object(state_sync, "get_snapshots") as mock:
        assert not cache.get_snapshots([snapshot.snapshot_id])
        mock.assert_called()

    # clear the cache by removing intervals
    cache.remove_interval([snapshot], "2020-01-01", "2020-01-01")

    # prime the cache
    assert cache.get_snapshots([snapshot.snapshot_id]) == {snapshot.snapshot_id: snapshot}

    # cache hit half way
    now_timestamp.return_value = to_timestamp("2023-01-01 00:00:05")
    with patch.object(state_sync, "get_snapshots") as mock:
        assert cache.get_snapshots([snapshot.snapshot_id])
        mock.not_called()

    # no cache hit
    now_timestamp.return_value = to_timestamp("2023-01-01 00:00:11")
    with patch.object(state_sync, "get_snapshots") as mock:
        assert not cache.get_snapshots([snapshot.snapshot_id])
        mock.assert_called()
