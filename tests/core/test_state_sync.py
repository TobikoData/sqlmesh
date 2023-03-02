import typing as t

import pytest
from sqlglot import parse_one

from sqlmesh.core.context import Context
from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    ModelKind,
    ModelKindName,
    SqlModel,
)
from sqlmesh.core.snapshot import Snapshot, SnapshotTableInfo
from sqlmesh.core.state_sync import EngineAdapterStateSync
from sqlmesh.utils.date import now_timestamp, to_datetime, to_timestamp
from sqlmesh.utils.errors import SQLMeshError


@pytest.fixture
def state_sync(duck_conn):
    state_sync = EngineAdapterStateSync(
        create_engine_adapter(lambda: duck_conn, "duckdb"),
        "sqlmesh",
    )
    state_sync.init_schema()
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

    snapshot_a.set_version()
    snapshot_b.set_version("2")
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


def test_get_snapshots_with_same_version(
    state_sync: EngineAdapterStateSync,
    make_snapshot: t.Callable,
    snapshots: t.List[Snapshot],
) -> None:
    snapshot_a = snapshots[0]

    snapshot_a_new = make_snapshot(
        SqlModel(
            name=snapshot_a.name,
            query=parse_one("select 3, ds"),
        ),
        version=snapshot_a.version,
    )
    state_sync.push_snapshots(snapshots + [snapshot_a_new])

    assert state_sync.get_snapshots_with_same_version([snapshot_a_new]) == [
        snapshot_a,
        snapshot_a_new,
    ]


def test_snapshots_exists(state_sync: EngineAdapterStateSync, snapshots: t.List[Snapshot]) -> None:
    state_sync.push_snapshots(snapshots)
    snapshot_ids = {snapshot.snapshot_id for snapshot in snapshots}
    assert state_sync.snapshots_exist(snapshot_ids) == snapshot_ids


def test_add_interval(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable) -> None:
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 1, ds"),
        ),
        version="a",
    )
    snapshot_id = snapshot.snapshot_id

    with pytest.raises(
        SQLMeshError,
        match=r".*was not found.*",
    ):
        state_sync.add_interval(snapshot_id, 0, 1)

    state_sync.push_snapshots([snapshot])
    state_sync.add_interval(snapshot_id, "2020-01-01", "20200101")
    assert state_sync.get_snapshots([snapshot_id])[snapshot_id].intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-02")),
    ]
    state_sync.add_interval(snapshot_id, "20200101", to_datetime("2020-01-04"))
    assert state_sync.get_snapshots([snapshot_id])[snapshot_id].intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-04")),
    ]
    state_sync.add_interval(snapshot_id, to_datetime("2020-01-05"), "2020-01-10")
    assert state_sync.get_snapshots([snapshot_id])[snapshot_id].intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-04")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-11")),
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
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-11"))
    ]
    assert snapshots[snapshot_b.snapshot_id].intervals == [
        (to_timestamp("2020-01-11"), to_timestamp("2020-01-15")),
        (to_timestamp("2020-01-18"), to_timestamp("2020-01-31")),
    ]


def test_promote_snapshots(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
        ),
        version="a",
    )
    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            kind=ModelKind(name=ModelKindName.FULL),
            query=parse_one("select * from a"),
        ),
        models={"a": snapshot_a.model},
        version="b",
    )
    snapshot_c = make_snapshot(
        SqlModel(
            name="c",
            query=parse_one("select 3, ds"),
        ),
        version="c",
    )

    with pytest.raises(
        SQLMeshError,
        match=r"Missing snapshots.*",
    ):
        promote_snapshots(state_sync, [snapshot_a], "prod")

    state_sync.push_snapshots([snapshot_a, snapshot_b, snapshot_c])

    with pytest.raises(
        SQLMeshError,
        match=r"Did you mean to promote all.*",
    ):
        promote_snapshots(state_sync, [snapshot_b], "prod")

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

    with pytest.raises(
        SQLMeshError,
        match=r"Did you mean to promote all.*",
    ):
        promote_snapshots(state_sync, [snapshot_b], "prod")

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
        version="d",
    )
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
        version="a",
    )

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
    snapshot.add_interval("2022-01-01", "2022-01-02")
    state_sync.push_snapshots([snapshot])
    promote_snapshots(state_sync, [snapshot], "prod", no_gaps=True)

    new_snapshot_same_version = make_snapshot(model, version="a")
    new_snapshot_same_version.fingerprint = snapshot.fingerprint.copy(
        update={"data_hash": "new_snapshot_same_version"}
    )
    new_snapshot_same_version.add_interval("2022-01-03", "2022-01-03")
    state_sync.push_snapshots([new_snapshot_same_version])
    promote_snapshots(state_sync, [new_snapshot_same_version], "prod", no_gaps=True)

    new_snapshot_missing_interval = make_snapshot(model, version="b")
    new_snapshot_missing_interval.fingerprint = snapshot.fingerprint.copy(
        update={"data_hash": "new_snapshot_missing_interval"}
    )
    new_snapshot_missing_interval.add_interval("2022-01-01", "2022-01-02")
    state_sync.push_snapshots([new_snapshot_missing_interval])
    with pytest.raises(
        SQLMeshError,
        match=r"Detected gaps in snapshot.*",
    ):
        promote_snapshots(state_sync, [new_snapshot_missing_interval], "prod", no_gaps=True)

    new_snapshot_same_interval = make_snapshot(model, version="c")
    new_snapshot_same_interval.fingerprint = snapshot.fingerprint.copy(
        update={"data_hash": "new_snapshot_same_interval"}
    )
    new_snapshot_same_interval.add_interval("2022-01-01", "2022-01-03")
    state_sync.push_snapshots([new_snapshot_same_interval])
    promote_snapshots(state_sync, [new_snapshot_same_interval], "prod", no_gaps=True)


def test_delete_expired_environments(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, ds"),
        ),
        version="a",
    )

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


def test_missing_intervals(sushi_context_pre_scheduling: Context) -> None:
    sushi_context = sushi_context_pre_scheduling
    state_sync = sushi_context.state_reader
    missing = state_sync.missing_intervals("prod", "2022-01-01", "2022-01-07", latest="2022-01-07")
    assert missing
    assert missing == sushi_context.state_reader.missing_intervals(
        sushi_context.snapshots.values(), "2022-01-01", "2022-01-07", "2022-01-07"
    )


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
