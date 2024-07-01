import json
import re
import typing as t
from unittest.mock import call, patch

import duckdb
import pandas as pd
import pytest
from freezegun import freeze_time
from pytest_mock.plugin import MockerFixture
from sqlglot import exp

from sqlmesh.core import constants as c
from sqlmesh.core.config import EnvironmentSuffixTarget
from sqlmesh.core.dialect import parse_one, schema_
from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import (
    FullKind,
    IncrementalByTimeRangeKind,
    ModelKindName,
    Seed,
    SeedKind,
    SeedModel,
    SqlModel,
)
from sqlmesh.core.model.definition import ExternalModel
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotId,
    SnapshotIntervals,
    SnapshotTableCleanupTask,
    missing_intervals,
)
from sqlmesh.core.state_sync import (
    CachingStateSync,
    EngineAdapterStateSync,
    cleanup_expired_views,
)
from sqlmesh.core.state_sync.base import (
    SCHEMA_VERSION,
    SQLGLOT_VERSION,
    PromotionResult,
    Versions,
)
from sqlmesh.utils.date import now_timestamp, to_datetime, to_timestamp
from sqlmesh.utils.errors import SQLMeshError

pytestmark = pytest.mark.slow


@pytest.fixture
def state_sync(duck_conn, tmp_path):
    state_sync = EngineAdapterStateSync(
        create_engine_adapter(lambda: duck_conn, "duckdb"), schema=c.SQLMESH, context_path=tmp_path
    )
    state_sync.migrate(default_catalog=None)
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
    no_gaps_snapshot_names: t.Optional[t.Set[str]] = None,
    environment_suffix_target: EnvironmentSuffixTarget = EnvironmentSuffixTarget.SCHEMA,
    environment_catalog_mapping: t.Optional[t.Dict[re.Pattern, str]] = None,
) -> PromotionResult:
    env = Environment.from_environment_catalog_mapping(
        environment_catalog_mapping or {},
        name=environment,
        suffix_target=environment_suffix_target,
        snapshots=[snapshot.table_info for snapshot in snapshots],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id="test_plan_id",
    )
    return state_sync.promote(
        env, no_gaps_snapshot_names=no_gaps_snapshot_names if no_gaps else set()
    )


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
                    kind=FullKind(),
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


def test_snapshots_exists(state_sync: EngineAdapterStateSync, snapshots: t.List[Snapshot]) -> None:
    state_sync.push_snapshots(snapshots)
    snapshot_ids = {snapshot.snapshot_id for snapshot in snapshots}
    assert state_sync.snapshots_exist(snapshot_ids) == snapshot_ids


@pytest.fixture
def get_snapshot_intervals(state_sync) -> t.Callable[[Snapshot], t.Optional[SnapshotIntervals]]:
    def _get_snapshot_intervals(snapshot: Snapshot) -> t.Optional[SnapshotIntervals]:
        intervals = state_sync._get_snapshot_intervals([snapshot])[-1]
        return intervals[0] if intervals else None

    return _get_snapshot_intervals


def test_add_interval(
    state_sync: EngineAdapterStateSync,
    make_snapshot: t.Callable,
    get_snapshot_intervals: t.Callable,
) -> None:
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
    assert get_snapshot_intervals(snapshot).intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-02")),
    ]

    state_sync.add_interval(snapshot, "20200101", to_datetime("2020-01-04"))
    assert get_snapshot_intervals(snapshot).intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-04")),
    ]

    state_sync.add_interval(snapshot, to_datetime("2020-01-05"), "2020-01-10")
    assert get_snapshot_intervals(snapshot).intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-04")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-11")),
    ]

    snapshot.change_category = SnapshotChangeCategory.FORWARD_ONLY
    state_sync.add_interval(snapshot, to_datetime("2020-01-16"), "2020-01-20", is_dev=True)
    intervals = get_snapshot_intervals(snapshot)
    assert intervals.intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-04")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-11")),
    ]
    assert intervals.dev_intervals == [
        (to_timestamp("2020-01-16"), to_timestamp("2020-01-21")),
    ]


def test_add_interval_partial(
    state_sync: EngineAdapterStateSync,
    make_snapshot: t.Callable,
    get_snapshot_intervals: t.Callable,
) -> None:
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 1, ds"),
        ),
        version="a",
    )

    state_sync.push_snapshots([snapshot])

    state_sync.add_interval(snapshot, "2023-01-01", to_timestamp("2023-01-01") + 1000)
    assert get_snapshot_intervals(snapshot) is None

    state_sync.add_interval(snapshot, "2023-01-01", to_timestamp("2023-01-02") + 1000)
    assert get_snapshot_intervals(snapshot).intervals == [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
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

    state_sync.remove_interval(
        [(snapshot_a, snapshot_a.inclusive_exclusive("2020-01-15", "2020-01-17"))],
        remove_shared_versions=True,
    )

    snapshots = state_sync.get_snapshots([snapshot_a, snapshot_b])

    assert snapshots[snapshot_a.snapshot_id].intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-15")),
        (to_timestamp("2020-01-18"), to_timestamp("2020-01-31")),
    ]
    assert snapshots[snapshot_b.snapshot_id].intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-15")),
        (to_timestamp("2020-01-18"), to_timestamp("2020-01-31")),
    ]


def test_remove_interval_missing_snapshot(
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
    # Remove snapshot b in order to test the scenario where it is missing
    state_sync.delete_snapshots([snapshot_b.snapshot_id])

    snapshots = state_sync.get_snapshots([snapshot_a, snapshot_b])
    assert len(snapshots) == 1
    assert snapshots[snapshot_a.snapshot_id].intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-31")),
    ]

    state_sync.remove_interval(
        [(snapshot_a, snapshot_a.inclusive_exclusive("2020-01-15", "2020-01-17"))],
        remove_shared_versions=True,
    )

    snapshots = state_sync.get_snapshots([snapshot_a, snapshot_b])
    assert len(snapshots) == 1
    assert snapshots[snapshot_a.snapshot_id].intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-15")),
        (to_timestamp("2020-01-18"), to_timestamp("2020-01-31")),
    ]


def test_refresh_snapshot_intervals(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
) -> None:
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 1, ds"),
        ),
        version="a",
    )

    state_sync.push_snapshots([snapshot])
    state_sync.add_interval(snapshot, "2023-01-01", "2023-01-01")
    assert not snapshot.intervals

    state_sync.refresh_snapshot_intervals([snapshot])
    assert snapshot.intervals == [(to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))]


def test_get_snapshot_intervals(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable, get_snapshot_intervals
) -> None:
    state_sync.SNAPSHOT_BATCH_SIZE = 1

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

    snapshot_c = make_snapshot(
        SqlModel(
            name="c",
            cron="@daily",
            query=parse_one("select 3, ds"),
        ),
        version="c",
    )
    state_sync.push_snapshots([snapshot_c])
    state_sync.add_interval(snapshot_c, "2020-01-03", "2020-01-03")

    a_intervals = get_snapshot_intervals(snapshot_a)
    c_intervals = get_snapshot_intervals(snapshot_c)
    assert a_intervals.intervals == [(to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))]
    assert c_intervals.intervals == [(to_timestamp("2020-01-03"), to_timestamp("2020-01-04"))]


def test_compact_intervals(
    state_sync: EngineAdapterStateSync,
    make_snapshot: t.Callable,
    get_snapshot_intervals: t.Callable,
) -> None:
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
    state_sync.remove_interval(
        [(snapshot, snapshot.inclusive_exclusive("2020-01-05", "2020-01-12"))]
    )
    state_sync.add_interval(snapshot, "2020-01-12", "2020-01-16")
    state_sync.remove_interval(
        [(snapshot, snapshot.inclusive_exclusive("2020-01-14", "2020-01-16"))]
    )

    expected_intervals = [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-05")),
        (to_timestamp("2020-01-12"), to_timestamp("2020-01-14")),
    ]

    assert get_snapshot_intervals(snapshot).intervals == expected_intervals

    state_sync.compact_intervals()
    assert get_snapshot_intervals(snapshot).intervals == expected_intervals

    # Make sure compaction is idempotent.
    state_sync.compact_intervals()
    assert get_snapshot_intervals(snapshot).intervals == expected_intervals


def test_compact_intervals_delete_batches(
    state_sync: EngineAdapterStateSync,
    make_snapshot: t.Callable,
    mocker: MockerFixture,
) -> None:
    snapshot = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 1, ds"),
        ),
        version="a",
    )

    delete_from_mock = mocker.patch.object(state_sync.engine_adapter, "delete_from")
    state_sync.INTERVAL_BATCH_SIZE = 2

    state_sync.push_snapshots([snapshot])

    state_sync.add_interval(snapshot, "2020-01-01", "2020-01-11")
    state_sync.add_interval(snapshot, "2020-01-01", "2020-01-12")
    state_sync.add_interval(snapshot, "2020-01-01", "2020-01-13")
    state_sync.add_interval(snapshot, "2020-01-01", "2020-01-14")
    state_sync.add_interval(snapshot, "2020-01-01", "2020-01-15")

    state_sync.compact_intervals()

    delete_from_mock.assert_has_calls([call(state_sync.intervals_table, mocker.ANY)] * 3)


def test_promote_snapshots(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, ds"),
        ),
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b_old = make_snapshot(
        SqlModel(
            name="b",
            kind=FullKind(),
            query=parse_one("select 2 from a"),
        ),
        nodes={"a": snapshot_a.model},
    )
    snapshot_b_old.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            kind=FullKind(),
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

    state_sync.push_snapshots([snapshot_a, snapshot_b_old, snapshot_b, snapshot_c])

    promotion_result = promote_snapshots(state_sync, [snapshot_a, snapshot_b_old], "prod")

    assert set(promotion_result.added) == set([snapshot_a.table_info, snapshot_b_old.table_info])
    assert not promotion_result.removed
    assert not promotion_result.removed_environment_naming_info
    promotion_result = promote_snapshots(
        state_sync,
        [snapshot_a, snapshot_b_old, snapshot_c],
        "prod",
    )
    assert set(promotion_result.added) == {
        snapshot_a.table_info,
        snapshot_b_old.table_info,
        snapshot_c.table_info,
    }
    assert not promotion_result.removed
    assert not promotion_result.removed_environment_naming_info

    prev_snapshot_b_old_updated_ts = snapshot_b_old.updated_ts
    prev_snapshot_c_updated_ts = snapshot_c.updated_ts

    promotion_result = promote_snapshots(
        state_sync,
        [snapshot_a, snapshot_b],
        "prod",
    )
    assert set(promotion_result.added) == {snapshot_a.table_info, snapshot_b.table_info}
    assert set(promotion_result.removed) == {snapshot_c.table_info}
    assert promotion_result.removed_environment_naming_info
    assert promotion_result.removed_environment_naming_info.suffix_target.is_schema
    assert (
        state_sync.get_snapshots([snapshot_c])[snapshot_c.snapshot_id].updated_ts
        > prev_snapshot_c_updated_ts
    )
    assert (
        state_sync.get_snapshots([snapshot_b_old])[snapshot_b_old.snapshot_id].updated_ts
        > prev_snapshot_b_old_updated_ts
    )

    snapshot_d = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 2, ds"),
        ),
    )
    snapshot_d.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot_d])
    promotion_result = promote_snapshots(state_sync, [snapshot_d], "prod")
    assert set(promotion_result.added) == {snapshot_d.table_info}
    assert set(promotion_result.removed) == {snapshot_b.table_info}
    assert promotion_result.removed_environment_naming_info
    assert promotion_result.removed_environment_naming_info.suffix_target.is_schema


def test_promote_snapshots_suffix_change(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
):
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
            kind=FullKind(),
            query=parse_one("select * from a"),
        ),
        nodes={"a": snapshot_a.model},
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot_a, snapshot_b])

    promotion_result = promote_snapshots(
        state_sync,
        [snapshot_a, snapshot_b],
        "prod",
        environment_suffix_target=EnvironmentSuffixTarget.TABLE,
    )

    assert set(promotion_result.added) == {snapshot_a.table_info, snapshot_b.table_info}
    assert not promotion_result.removed
    assert not promotion_result.removed_environment_naming_info

    snapshot_c = make_snapshot(
        SqlModel(
            name="c",
            query=parse_one("select 3, ds"),
        ),
    )
    snapshot_c.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot_c])

    promotion_result = promote_snapshots(
        state_sync,
        [snapshot_b, snapshot_c],
        "prod",
        environment_suffix_target=EnvironmentSuffixTarget.SCHEMA,
    )

    # We still only add the snapshots that are included in the promotion
    assert set(promotion_result.added) == {snapshot_b.table_info, snapshot_c.table_info}
    # B does not get removed because the suffix target change doesn't affect it due to running in prod.
    assert set(promotion_result.removed) == {snapshot_a.table_info}
    # Make sure the removed suffix target is correctly seen as table
    assert promotion_result.removed_environment_naming_info is not None
    assert promotion_result.removed_environment_naming_info.suffix_target.is_table

    promotion_result = promote_snapshots(
        state_sync,
        [snapshot_b, snapshot_c],
        "dev",
        environment_suffix_target=EnvironmentSuffixTarget.SCHEMA,
    )

    # We still only add the snapshots that are included in the promotion
    assert set(promotion_result.added) == {snapshot_b.table_info, snapshot_c.table_info}
    assert len(promotion_result.removed) == 0
    assert promotion_result.removed_environment_naming_info is None

    promotion_result = promote_snapshots(
        state_sync,
        [snapshot_b, snapshot_c],
        "dev",
        environment_suffix_target=EnvironmentSuffixTarget.TABLE,
    )

    # All snapshots are promoted due to suffix target change
    assert set(promotion_result.added) == {
        snapshot_b.table_info,
        snapshot_c.table_info,
    }
    # All snapshots are removed due to suffix target change
    assert set(promotion_result.removed) == {
        snapshot_b.table_info,
        snapshot_c.table_info,
    }
    assert promotion_result.removed_environment_naming_info is not None
    assert promotion_result.removed_environment_naming_info.suffix_target.is_schema


def test_promote_snapshots_catalog_name_override_change(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
):
    snapshot_a = make_snapshot(
        SqlModel(
            name="catalog1.schema.a",
            query=parse_one("select 1, ds"),
        ),
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(
        SqlModel(
            name="catalog1.schema.b",
            kind=FullKind(),
            query=parse_one("select * from a"),
        ),
        nodes={"a": snapshot_a.model},
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_c = make_snapshot(
        SqlModel(
            name="catalog2.schema.c",
            kind=FullKind(),
            query=parse_one("select * from a"),
        ),
        nodes={"a": snapshot_a.model},
    )
    snapshot_c.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot_a, snapshot_b, snapshot_c])

    promotion_result = promote_snapshots(
        state_sync,
        [snapshot_a, snapshot_b, snapshot_c],
        "prod",
        environment_catalog_mapping={},
    )

    assert set(promotion_result.added) == {
        snapshot_a.table_info,
        snapshot_b.table_info,
        snapshot_c.table_info,
    }
    assert not promotion_result.removed
    assert not promotion_result.removed_environment_naming_info

    snapshot_d = make_snapshot(
        SqlModel(
            name="catalog1.schema.d",
            query=parse_one("select 3, ds"),
        ),
    )
    snapshot_d.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot_d])

    promotion_result = promote_snapshots(
        state_sync,
        [snapshot_b, snapshot_c, snapshot_d],
        "prod",
        environment_catalog_mapping={
            re.compile("^prod$"): "catalog1",
        },
    )

    # We still only add the snapshots that are included in the promotion which means removing A
    assert set(promotion_result.added) == {
        snapshot_b.table_info,
        snapshot_c.table_info,
        snapshot_d.table_info,
    }
    # C is removed because of the catalog change. The new one will be created in the new catalog.
    # B is not removed because it's catalog did not change and therefore removing would actually result
    # in dropping what we just added.
    # A is removed because it was explicitly removed from the promotion.
    assert set(promotion_result.removed) == {snapshot_a.table_info, snapshot_c.table_info}
    # Make sure the removed suffix target correctly has the old catalog name set
    assert promotion_result.removed_environment_naming_info
    assert promotion_result.removed_environment_naming_info.catalog_name_override is None

    promotion_result = promote_snapshots(
        state_sync,
        [snapshot_b, snapshot_c, snapshot_d],
        "prod",
        environment_catalog_mapping={
            re.compile("^prod$"): "catalog2",
        },
    )

    # All are added since their catalog was changed
    assert set(promotion_result.added) == {
        snapshot_b.table_info,
        snapshot_c.table_info,
        snapshot_d.table_info,
    }
    # All are removed since there were moved from their old catalog location
    # Note that C has a catalog set in the model definition of `catalog2` which is what we moved to so you might think
    # it shouldn't be removed, but its actual catalog was `catalog1` because of the previous override so therefore
    # it should be removed from `catalog1`.
    assert set(promotion_result.removed) == {
        snapshot_b.table_info,
        snapshot_c.table_info,
        snapshot_d.table_info,
    }
    # Make sure the removed suffix target correctly has the old catalog name set
    assert promotion_result.removed_environment_naming_info
    assert promotion_result.removed_environment_naming_info.catalog_name_override == "catalog1"


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
        start="2022-01-01",
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

    # We should skip the gaps check if the snapshot is not representative.
    promote_snapshots(
        state_sync,
        [new_snapshot_missing_interval],
        "prod",
        no_gaps=True,
        no_gaps_snapshot_names=set(),
    )


@freeze_time("2023-01-08 16:00:00")
def test_promote_snapshots_no_gaps_lookback(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
):
    model = SqlModel(
        name="a",
        cron="@hourly",
        query=parse_one("select 1, ds"),
        kind=IncrementalByTimeRangeKind(time_column="ds", lookback=1),
        start="2023-01-01",
    )

    snapshot = make_snapshot(model, version="a")
    snapshot.change_category = SnapshotChangeCategory.BREAKING
    state_sync.push_snapshots([snapshot])
    state_sync.add_interval(snapshot, "2023-01-01", "2023-01-08 15:00:00")
    promote_snapshots(state_sync, [snapshot], "prod", no_gaps=True)

    assert now_timestamp() == to_timestamp("2023-01-08 16:00:00")

    new_snapshot_same_version = make_snapshot(model, version="b")
    new_snapshot_same_version.change_category = SnapshotChangeCategory.BREAKING
    new_snapshot_same_version.fingerprint = snapshot.fingerprint.copy(
        update={"data_hash": "new_snapshot_same_version"}
    )
    state_sync.push_snapshots([new_snapshot_same_version])
    state_sync.add_interval(new_snapshot_same_version, "2023-01-01", "2023-01-08 15:00:00")
    promote_snapshots(state_sync, [new_snapshot_same_version], "prod", no_gaps=True)


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


def test_delete_expired_snapshots(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    now_ts = now_timestamp()

    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, ds"),
        ),
    )
    snapshot.ttl = "in 10 seconds"
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot.updated_ts = now_ts - 15000

    new_snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, b, ds"),
        ),
    )
    new_snapshot.ttl = "in 10 seconds"
    new_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    new_snapshot.version = snapshot.version
    new_snapshot.updated_ts = now_ts - 11000

    state_sync.push_snapshots([snapshot, new_snapshot])
    assert set(state_sync.get_snapshots(None)) == {snapshot.snapshot_id, new_snapshot.snapshot_id}

    assert state_sync.delete_expired_snapshots() == [
        SnapshotTableCleanupTask(snapshot=snapshot.table_info, dev_table_only=True),
        SnapshotTableCleanupTask(snapshot=new_snapshot.table_info, dev_table_only=False),
    ]

    assert not state_sync.get_snapshots(None)


def test_delete_expired_snapshots_seed(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
):
    now_ts = now_timestamp()

    snapshot = make_snapshot(
        SeedModel(
            name="a",
            kind=SeedKind(path="./path/to/seed"),
            seed=Seed(content="header\n1\n2"),
            column_hashes={"header": "hash"},
            depends_on=set(),
        ),
    )
    snapshot.ttl = "in 10 seconds"
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot.updated_ts = now_ts - 15000

    state_sync.push_snapshots([snapshot])
    assert set(state_sync.get_snapshots(None)) == {snapshot.snapshot_id}

    assert state_sync.delete_expired_snapshots() == [
        SnapshotTableCleanupTask(snapshot=snapshot.table_info, dev_table_only=False),
    ]

    assert not state_sync.get_snapshots(None)


def test_delete_expired_snapshots_batching(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
):
    state_sync.SNAPSHOT_BATCH_SIZE = 1
    now_ts = now_timestamp()

    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, ds"),
        ),
    )
    snapshot_a.ttl = "in 10 seconds"
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot_a.updated_ts = now_ts - 15000

    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            query=parse_one("select a, b, ds"),
        ),
    )
    snapshot_b.ttl = "in 10 seconds"
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot_b.updated_ts = now_ts - 11000

    state_sync.push_snapshots([snapshot_a, snapshot_b])
    assert set(state_sync.get_snapshots(None)) == {snapshot_a.snapshot_id, snapshot_b.snapshot_id}

    assert state_sync.delete_expired_snapshots() == [
        SnapshotTableCleanupTask(snapshot=snapshot_a.table_info, dev_table_only=False),
        SnapshotTableCleanupTask(snapshot=snapshot_b.table_info, dev_table_only=False),
    ]

    assert not state_sync.get_snapshots(None)


def test_delete_expired_snapshots_promoted(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable, mocker: MockerFixture
):
    now_ts = now_timestamp()

    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, ds"),
        ),
    )
    snapshot.ttl = "in 10 seconds"
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot.updated_ts = now_ts - 15000

    state_sync.push_snapshots([snapshot])

    env = Environment(
        name="test_environment",
        snapshots=[snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id="test_plan_id",
    )
    state_sync.promote(env)

    assert not state_sync.delete_expired_snapshots()
    assert set(state_sync.get_snapshots(None)) == {snapshot.snapshot_id}

    env.snapshots = []
    state_sync.promote(env)

    now_timestamp_mock = mocker.patch("sqlmesh.core.state_sync.engine_adapter.now_timestamp")
    now_timestamp_mock.return_value = now_timestamp() + 11000

    assert state_sync.delete_expired_snapshots() == [
        SnapshotTableCleanupTask(snapshot=snapshot.table_info, dev_table_only=False)
    ]
    assert not state_sync.get_snapshots(None)


def test_delete_expired_snapshots_dev_table_cleanup_only(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
):
    now_ts = now_timestamp()

    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, ds"),
        ),
    )
    snapshot.ttl = "in 10 seconds"
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot.updated_ts = now_ts - 15000

    new_snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, b, ds"),
        ),
    )
    new_snapshot.ttl = "in 10 seconds"
    new_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    new_snapshot.version = snapshot.version
    new_snapshot.updated_ts = now_ts - 5000

    state_sync.push_snapshots([snapshot, new_snapshot])
    assert set(state_sync.get_snapshots(None)) == {snapshot.snapshot_id, new_snapshot.snapshot_id}

    assert state_sync.delete_expired_snapshots() == [
        SnapshotTableCleanupTask(snapshot=snapshot.table_info, dev_table_only=True)
    ]

    assert set(state_sync.get_snapshots(None)) == {new_snapshot.snapshot_id}


def test_delete_expired_snapshots_shared_dev_table(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
):
    now_ts = now_timestamp()

    snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, ds"),
        ),
    )
    snapshot.ttl = "in 10 seconds"
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot.updated_ts = now_ts - 15000

    new_snapshot = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, b, ds"),
        ),
    )
    new_snapshot.ttl = "in 10 seconds"
    new_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    new_snapshot.version = snapshot.version
    new_snapshot.temp_version = snapshot.temp_version_get_or_generate()
    new_snapshot.updated_ts = now_ts - 5000

    state_sync.push_snapshots([snapshot, new_snapshot])
    assert set(state_sync.get_snapshots(None)) == {snapshot.snapshot_id, new_snapshot.snapshot_id}

    assert not state_sync.delete_expired_snapshots()  # No dev table cleanup
    assert set(state_sync.get_snapshots(None)) == {new_snapshot.snapshot_id}


def test_delete_expired_snapshots_ignore_ttl(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, ds"),
        )
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.NON_BREAKING)

    snapshot_b = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, b, ds"),
        ),
        version="2",
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.NON_BREAKING)

    snapshot_c = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select a, b, c, ds"),
        ),
    )
    snapshot_c.categorize_as(SnapshotChangeCategory.NON_BREAKING)

    state_sync.push_snapshots([snapshot_a, snapshot_b, snapshot_c])

    env = Environment(
        name="test_environment",
        snapshots=[snapshot_a.table_info, snapshot_b.table_info],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id="test_plan_id",
    )
    state_sync.promote(env)

    # default TTL = 1 week, nothing to clean up yet if we take TTL into account
    assert not state_sync.delete_expired_snapshots()

    # If we ignore TTL, only snapshot_c should get cleaned up because snapshot_a and snapshot_b are part of an environment
    assert snapshot_a.table_info != snapshot_b.table_info != snapshot_c.table_info
    assert state_sync.delete_expired_snapshots(ignore_ttl=True) == [
        SnapshotTableCleanupTask(snapshot=snapshot_c.table_info, dev_table_only=False)
    ]


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
    assert stored_env.start_at == to_datetime(now_ts).replace(tzinfo=None).isoformat(sep=" ")


def test_unpause_snapshots(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    snapshot = make_snapshot(
        SqlModel(
            name="test_snapshot",
            query=parse_one("select 1, ds"),
            cron="@daily",
        ),
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot.version = "a"

    assert not snapshot.unpaused_ts
    state_sync.push_snapshots([snapshot])

    unpaused_dt = "2022-01-01"
    state_sync.unpause_snapshots([snapshot], unpaused_dt)

    actual_snapshot = state_sync.get_snapshots([snapshot])[snapshot.snapshot_id]
    assert actual_snapshot.unpaused_ts
    assert actual_snapshot.unpaused_ts == to_timestamp(unpaused_dt)

    new_snapshot = make_snapshot(
        SqlModel(name="test_snapshot", query=parse_one("select 2, ds"), cron="@daily")
    )
    new_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    new_snapshot.version = "a"

    assert not new_snapshot.unpaused_ts
    state_sync.push_snapshots([new_snapshot])
    state_sync.unpause_snapshots([new_snapshot], unpaused_dt)

    actual_snapshots = state_sync.get_snapshots([snapshot, new_snapshot])
    assert not actual_snapshots[snapshot.snapshot_id].unpaused_ts
    assert actual_snapshots[new_snapshot.snapshot_id].unpaused_ts == to_timestamp(unpaused_dt)

    assert actual_snapshots[snapshot.snapshot_id].unrestorable
    assert not actual_snapshots[new_snapshot.snapshot_id].unrestorable


def test_unrestorable_snapshot(state_sync: EngineAdapterStateSync, make_snapshot: t.Callable):
    snapshot = make_snapshot(
        SqlModel(
            name="test_snapshot",
            query=parse_one("select 1, ds"),
            cron="@daily",
        ),
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot.version = "a"

    assert not snapshot.unpaused_ts
    state_sync.push_snapshots([snapshot])

    unpaused_dt = "2022-01-01"
    state_sync.unpause_snapshots([snapshot], unpaused_dt)

    actual_snapshot = state_sync.get_snapshots([snapshot])[snapshot.snapshot_id]
    assert actual_snapshot.unpaused_ts
    assert actual_snapshot.unpaused_ts == to_timestamp(unpaused_dt)

    new_indirect_non_breaking_snapshot = make_snapshot(
        SqlModel(name="test_snapshot", query=parse_one("select 2, ds"), cron="@daily")
    )
    new_indirect_non_breaking_snapshot.categorize_as(SnapshotChangeCategory.INDIRECT_NON_BREAKING)
    new_indirect_non_breaking_snapshot.version = "a"

    assert not new_indirect_non_breaking_snapshot.unpaused_ts
    state_sync.push_snapshots([new_indirect_non_breaking_snapshot])
    state_sync.unpause_snapshots([new_indirect_non_breaking_snapshot], unpaused_dt)

    actual_snapshots = state_sync.get_snapshots([snapshot, new_indirect_non_breaking_snapshot])
    assert not actual_snapshots[snapshot.snapshot_id].unpaused_ts
    assert actual_snapshots[
        new_indirect_non_breaking_snapshot.snapshot_id
    ].unpaused_ts == to_timestamp(unpaused_dt)

    assert not actual_snapshots[snapshot.snapshot_id].unrestorable
    assert not actual_snapshots[new_indirect_non_breaking_snapshot.snapshot_id].unrestorable

    new_forward_only_snapshot = make_snapshot(
        SqlModel(name="test_snapshot", query=parse_one("select 3, ds"), cron="@daily")
    )
    new_forward_only_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    new_forward_only_snapshot.version = "a"

    assert not new_forward_only_snapshot.unpaused_ts
    state_sync.push_snapshots([new_forward_only_snapshot])
    state_sync.unpause_snapshots([new_forward_only_snapshot], unpaused_dt)

    actual_snapshots = state_sync.get_snapshots(
        [snapshot, new_indirect_non_breaking_snapshot, new_forward_only_snapshot]
    )
    assert not actual_snapshots[snapshot.snapshot_id].unpaused_ts
    assert not actual_snapshots[new_indirect_non_breaking_snapshot.snapshot_id].unpaused_ts
    assert actual_snapshots[new_forward_only_snapshot.snapshot_id].unpaused_ts == to_timestamp(
        unpaused_dt
    )

    assert actual_snapshots[snapshot.snapshot_id].unrestorable
    assert actual_snapshots[new_indirect_non_breaking_snapshot.snapshot_id].unrestorable
    assert not actual_snapshots[new_forward_only_snapshot.snapshot_id].unrestorable


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
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot.version = "a"
    state_sync.push_snapshots([snapshot])
    state_sync.add_interval(snapshot, "2023-01-01", "2023-01-05")

    new_snapshot = make_snapshot(
        SqlModel(name="test_snapshot", query=parse_one("select 2, ds"), cron="@daily"),
        version="a",
    )
    new_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    new_snapshot.version = "a"
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


def test_version_schema(state_sync: EngineAdapterStateSync, tmp_path) -> None:
    from sqlmesh import __version__ as SQLMESH_VERSION

    # fresh install should not raise
    assert state_sync.get_versions() == Versions(
        schema_version=SCHEMA_VERSION,
        sqlglot_version=SQLGLOT_VERSION,
        sqlmesh_version=SQLMESH_VERSION,
    )

    # Start with a clean slate.
    state_sync = EngineAdapterStateSync(
        create_engine_adapter(duckdb.connect, "duckdb"), schema=c.SQLMESH, context_path=tmp_path
    )

    with pytest.raises(
        SQLMeshError,
        match=rf"SQLMesh \(local\) is using version '{SCHEMA_VERSION}' which is ahead of '0'",
    ):
        state_sync.get_versions()

    state_sync.migrate(default_catalog=None)

    # migration version is behind, always raise
    state_sync._update_versions(schema_version=SCHEMA_VERSION + 1)
    error = (
        rf"SQLMesh \(local\) is using version '{SCHEMA_VERSION}' which is behind '{SCHEMA_VERSION + 1}' \(remote\). "
        rf"""Please upgrade SQLMesh \('pip install --upgrade "sqlmesh=={SQLMESH_VERSION}"' command\)."""
    )

    with pytest.raises(SQLMeshError, match=error):
        state_sync.get_versions()

    # should no longer raise
    state_sync.get_versions(validate=False)

    # migration version is ahead, only raise when validate is true
    state_sync._update_versions(schema_version=SCHEMA_VERSION - 1)
    with pytest.raises(
        SQLMeshError,
        match=rf"SQLMesh \(local\) is using version '{SCHEMA_VERSION}' which is ahead of '{SCHEMA_VERSION - 1}'",
    ):
        state_sync.get_versions()
    state_sync.get_versions(validate=False)


def test_version_sqlmesh(state_sync: EngineAdapterStateSync) -> None:
    from sqlmesh import __version__ as SQLMESH_VERSION
    from sqlmesh import __version_tuple__ as SQLMESH_VERSION_TUPLE

    # patch version sqlmesh doesn't matter
    major, minor, patch, *_ = SQLMESH_VERSION_TUPLE
    new_patch = (
        f"dev{int(patch[3:]) + 1}"  # type: ignore
        if isinstance(patch, str) and patch.startswith("dev")
        else f"{int(patch) + 1}"
    )
    sqlmesh_version_patch_bump = f"{major}.{minor}.{new_patch}"
    state_sync._update_versions(sqlmesh_version=sqlmesh_version_patch_bump)
    state_sync.get_versions(validate=False)

    # sqlmesh version is behind
    sqlmesh_version_minor_bump = f"{major}.{int(minor) + 1}.{patch}"
    error = (
        rf"SQLMesh \(local\) is using version '{SQLMESH_VERSION}' which is behind '{sqlmesh_version_minor_bump}' \(remote\). "
        rf"""Please upgrade SQLMesh \('pip install --upgrade "sqlmesh=={sqlmesh_version_minor_bump}"' command\)."""
    )
    state_sync._update_versions(sqlmesh_version=sqlmesh_version_minor_bump)
    with pytest.raises(SQLMeshError, match=error):
        state_sync.get_versions()
    state_sync.get_versions(validate=False)

    # sqlmesh version is ahead
    sqlmesh_version_minor_decrease = f"{major}.{int(minor) - 1}.{patch}"
    error = rf"SQLMesh \(local\) is using version '{SQLMESH_VERSION}' which is ahead of '{sqlmesh_version_minor_decrease}'"
    state_sync._update_versions(sqlmesh_version=sqlmesh_version_minor_decrease)
    with pytest.raises(SQLMeshError, match=error):
        state_sync.get_versions()
    state_sync.get_versions(validate=False)


def test_version_sqlglot(state_sync: EngineAdapterStateSync) -> None:
    # patch version sqlglot doesn't matter
    major, minor, patch, *_ = SQLGLOT_VERSION.split(".")
    sqlglot_version = f"{major}.{minor}.{int(patch) + 1}"
    state_sync._update_versions(sqlglot_version=sqlglot_version)
    state_sync.get_versions(validate=False)

    # sqlglot version is behind
    sqlglot_version = f"{major}.{int(minor) + 1}.{patch}"
    error = (
        rf"SQLGlot \(local\) is using version '{SQLGLOT_VERSION}' which is behind '{sqlglot_version}' \(remote\). "
        rf"""Please upgrade SQLGlot \('pip install --upgrade "sqlglot=={sqlglot_version}"' command\)."""
    )
    state_sync._update_versions(sqlglot_version=sqlglot_version)
    with pytest.raises(SQLMeshError, match=error):
        state_sync.get_versions()
    state_sync.get_versions(validate=False)

    # sqlglot version is ahead
    sqlglot_version = f"{major}.{int(minor) - 1}.{patch}"
    error = rf"SQLGlot \(local\) is using version '{SQLGLOT_VERSION}' which is ahead of '{sqlglot_version}'"
    state_sync._update_versions(sqlglot_version=sqlglot_version)
    with pytest.raises(SQLMeshError, match=error):
        state_sync.get_versions()
    state_sync.get_versions(validate=False)


def test_empty_versions() -> None:
    for empty_versions in (
        Versions(),
        Versions(schema_version=None, sqlglot_version=None, sqlmesh_version=None),
    ):
        assert empty_versions.schema_version == 0
        assert empty_versions.sqlglot_version == "0.0.0"
        assert empty_versions.sqlmesh_version == "0.0.0"


def test_migrate(state_sync: EngineAdapterStateSync, mocker: MockerFixture, tmp_path) -> None:
    from sqlmesh import __version__ as SQLMESH_VERSION

    migrate_rows_mock = mocker.patch("sqlmesh.core.state_sync.EngineAdapterStateSync._migrate_rows")
    backup_state_mock = mocker.patch("sqlmesh.core.state_sync.EngineAdapterStateSync._backup_state")
    state_sync.migrate(default_catalog=None)
    migrate_rows_mock.assert_not_called()
    backup_state_mock.assert_not_called()

    # Start with a clean slate.
    state_sync = EngineAdapterStateSync(
        create_engine_adapter(duckdb.connect, "duckdb"), schema=c.SQLMESH, context_path=tmp_path
    )

    state_sync.migrate(default_catalog=None)
    migrate_rows_mock.assert_called_once()
    backup_state_mock.assert_called_once()
    assert state_sync.get_versions() == Versions(
        schema_version=SCHEMA_VERSION,
        sqlglot_version=SQLGLOT_VERSION,
        sqlmesh_version=SQLMESH_VERSION,
    )

    assert (
        state_sync.engine_adapter.fetchone(
            "SELECT COUNT(*) FROM sqlmesh._snapshots WHERE expiration_ts IS NULL"
        )[0]
        == 0
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
    calls = {(a.sql(), b.sql()) for (a, b), _ in restore_table_spy.call_args_list}
    assert (
        f"{state_sync.schema}._snapshots",
        f"{state_sync.schema}._snapshots_backup",
    ) in calls
    assert (
        f"{state_sync.schema}._environments",
        f"{state_sync.schema}._environments_backup",
    ) in calls
    assert (
        f"{state_sync.schema}._versions",
        f"{state_sync.schema}._versions_backup",
    ) in calls
    assert not state_sync.engine_adapter.table_exists(f"{state_sync.schema}._snapshots_backup")
    assert not state_sync.engine_adapter.table_exists(f"{state_sync.schema}._environments_backup")
    assert not state_sync.engine_adapter.table_exists(f"{state_sync.schema}._versions_backup")


def test_first_migration_failure(duck_conn, mocker: MockerFixture, tmp_path) -> None:
    state_sync = EngineAdapterStateSync(
        create_engine_adapter(lambda: duck_conn, "duckdb"), schema=c.SQLMESH, context_path=tmp_path
    )
    mocker.patch.object(state_sync, "_migrate_rows", side_effect=Exception("mocked error"))
    with pytest.raises(
        SQLMeshError,
        match="SQLMesh migration failed.",
    ):
        state_sync.migrate(default_catalog=None)
    assert not state_sync.engine_adapter.table_exists(state_sync.snapshots_table)
    assert not state_sync.engine_adapter.table_exists(state_sync.environments_table)
    assert not state_sync.engine_adapter.table_exists(state_sync.versions_table)
    assert not state_sync.engine_adapter.table_exists(state_sync.intervals_table)


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

    state_sync.engine_adapter.drop_table("sqlmesh._seeds")

    old_snapshots = state_sync.engine_adapter.fetchdf("select * from sqlmesh._snapshots")
    old_environments = state_sync.engine_adapter.fetchdf("select * from sqlmesh._environments")

    state_sync.migrate(default_catalog=None, skip_backup=True)

    new_snapshots = state_sync.engine_adapter.fetchdf("select * from sqlmesh._snapshots")
    new_environments = state_sync.engine_adapter.fetchdf("select * from sqlmesh._environments")

    assert len(old_snapshots) * 2 == len(new_snapshots)
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

    assert all(s.migrated for s in dev_snapshots)
    assert all(s.change_category is not None for s in dev_snapshots)

    assert not missing_intervals(dev_snapshots, start=start, end=end)

    assert not missing_intervals(dev_snapshots, start="2023-01-08", end="2023-01-10") == 8

    for s in state_sync.get_snapshots(None).values():
        if not s.is_symbolic:
            assert s.intervals

    customer_revenue_by_day = new_snapshots.loc[
        new_snapshots["name"] == '"sushi"."customer_revenue_by_day"'
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

    stored_snapshot = state_sync.get_snapshots([snapshot.snapshot_id])[snapshot.snapshot_id]
    assert isinstance(stored_snapshot.model, SeedModel)
    assert not stored_snapshot.model.is_hydrated
    assert stored_snapshot.model.seed.content == ""


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
    cache.remove_interval([(snapshot, snapshot.inclusive_exclusive("2020-01-01", "2020-01-01"))])

    # prime the cache
    assert cache.get_snapshots([snapshot.snapshot_id]) == {snapshot.snapshot_id: snapshot}

    # cache hit half way
    now_timestamp.return_value = to_timestamp("2023-01-01 00:00:05")
    with patch.object(state_sync, "get_snapshots") as mock:
        assert cache.get_snapshots([snapshot.snapshot_id])
        mock.assert_not_called()

    # no cache hit
    now_timestamp.return_value = to_timestamp("2023-01-01 00:00:11")
    with patch.object(state_sync, "get_snapshots") as mock:
        assert not cache.get_snapshots([snapshot.snapshot_id])
        mock.assert_called()


def test_cleanup_expired_views(
    mocker: MockerFixture, state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
):
    adapter = mocker.MagicMock()
    adapter.dialect = None
    snapshot_a = make_snapshot(SqlModel(name="catalog.schema.a", query=parse_one("select 1, ds")))
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot_b = make_snapshot(SqlModel(name="catalog.schema.b", query=parse_one("select 1, ds")))
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)
    # Make sure that we don't drop schemas from external models
    snapshot_external_model = make_snapshot(
        ExternalModel(name="catalog.external_schema.external_table", kind=ModelKindName.EXTERNAL)
    )
    snapshot_external_model.categorize_as(SnapshotChangeCategory.BREAKING)
    schema_environment = Environment(
        name="test_environment",
        suffix_target=EnvironmentSuffixTarget.SCHEMA,
        snapshots=[
            snapshot_a.table_info,
            snapshot_b.table_info,
            snapshot_external_model.table_info,
        ],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id="test_plan_id",
        catalog_name_override="catalog_override",
    )
    snapshot_c = make_snapshot(SqlModel(name="catalog.schema.c", query=parse_one("select 1, ds")))
    snapshot_c.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot_d = make_snapshot(SqlModel(name="catalog.schema.d", query=parse_one("select 1, ds")))
    snapshot_d.categorize_as(SnapshotChangeCategory.BREAKING)
    table_environment = Environment(
        name="test_environment",
        suffix_target=EnvironmentSuffixTarget.TABLE,
        snapshots=[
            snapshot_c.table_info,
            snapshot_d.table_info,
            snapshot_external_model.table_info,
        ],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id="test_plan_id",
        catalog_name_override="catalog_override",
    )
    cleanup_expired_views(adapter, [schema_environment, table_environment])
    assert adapter.drop_schema.called
    assert adapter.drop_view.called
    assert adapter.drop_schema.call_args_list == [
        call(
            schema_("schema__test_environment", "catalog_override"),
            ignore_if_not_exists=True,
            cascade=True,
        )
    ]
    assert sorted(adapter.drop_view.call_args_list) == [
        call("catalog_override.schema.c__test_environment", ignore_if_not_exists=True),
        call("catalog_override.schema.d__test_environment", ignore_if_not_exists=True),
    ]


def test_max_interval_end_for_environment(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
) -> None:
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 1, ds"),
        ),
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            cron="@daily",
            query=parse_one("select 2, ds"),
        ),
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot_a, snapshot_b])

    state_sync.add_interval(snapshot_a, "2023-01-01", "2023-01-01")
    state_sync.add_interval(snapshot_a, "2023-01-02", "2023-01-02")
    state_sync.add_interval(snapshot_b, "2023-01-01", "2023-01-01")

    environment_name = "test_max_interval_end_for_environment"

    assert state_sync.max_interval_end_for_environment(environment_name) is None

    state_sync.promote(
        Environment(
            name=environment_name,
            snapshots=[snapshot_a.table_info, snapshot_b.table_info],
            start_at="2023-01-01",
            end_at="2023-01-03",
            plan_id="test_plan_id",
            previous_finalized_snapshots=[snapshot_b.table_info],
        )
    )

    assert state_sync.max_interval_end_for_environment(environment_name) == to_timestamp(
        "2023-01-03"
    )


def test_max_interval_end_for_environment_ensure_finalized_snapshots(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
) -> None:
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 1, ds"),
        ),
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            cron="@daily",
            query=parse_one("select 2, ds"),
        ),
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot_a, snapshot_b])

    state_sync.add_interval(snapshot_a, "2023-01-01", "2023-01-01")
    state_sync.add_interval(snapshot_a, "2023-01-02", "2023-01-02")
    state_sync.add_interval(snapshot_b, "2023-01-01", "2023-01-01")

    environment_name = "test_max_interval_end_for_environment"

    assert (
        state_sync.max_interval_end_for_environment(
            environment_name, ensure_finalized_snapshots=True
        )
        is None
    )

    state_sync.promote(
        Environment(
            name=environment_name,
            snapshots=[snapshot_a.table_info, snapshot_b.table_info],
            start_at="2023-01-01",
            end_at="2023-01-03",
            plan_id="test_plan_id",
            previous_finalized_snapshots=[snapshot_b.table_info],
        )
    )

    assert state_sync.max_interval_end_for_environment(
        environment_name, ensure_finalized_snapshots=True
    ) == to_timestamp("2023-01-02")


def test_greatest_common_interval_end(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
) -> None:
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 1, ds"),
        ),
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            cron="@daily",
            query=parse_one("select 2, ds"),
        ),
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot_a, snapshot_b])

    state_sync.add_interval(snapshot_a, "2023-01-01", "2023-01-01")
    state_sync.add_interval(snapshot_a, "2023-01-02", "2023-01-02")
    state_sync.add_interval(snapshot_a, "2023-01-03", "2023-01-03")
    state_sync.add_interval(snapshot_b, "2023-01-01", "2023-01-01")
    state_sync.add_interval(snapshot_b, "2023-01-02", "2023-01-02")

    environment_name = "test_max_interval_end_for_environment"

    assert state_sync.greatest_common_interval_end(environment_name, {snapshot_a.name}) is None

    state_sync.promote(
        Environment(
            name=environment_name,
            snapshots=[snapshot_a.table_info, snapshot_b.table_info],
            start_at="2023-01-01",
            end_at="2023-01-03",
            plan_id="test_plan_id",
            previous_finalized_snapshots=[snapshot_b.table_info],
        )
    )

    assert state_sync.greatest_common_interval_end(
        environment_name, {snapshot_a.name}
    ) == to_timestamp("2023-01-04")

    assert state_sync.greatest_common_interval_end(
        environment_name, {snapshot_b.name}
    ) == to_timestamp("2023-01-03")

    assert state_sync.greatest_common_interval_end(
        environment_name, {snapshot_a.name, snapshot_b.name}
    ) == to_timestamp("2023-01-03")

    assert state_sync.greatest_common_interval_end(environment_name, {"missing"}) == to_timestamp(
        "2023-01-03"
    )
    assert state_sync.greatest_common_interval_end(environment_name, set()) is None


def test_greatest_common_interval_end_ensure_finalized_snapshots(
    state_sync: EngineAdapterStateSync, make_snapshot: t.Callable
) -> None:
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            cron="@daily",
            query=parse_one("select 1, ds"),
        ),
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            cron="@daily",
            query=parse_one("select 2, ds"),
        ),
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot_a, snapshot_b])

    state_sync.add_interval(snapshot_a, "2023-01-01", "2023-01-01")
    state_sync.add_interval(snapshot_a, "2023-01-02", "2023-01-02")
    state_sync.add_interval(snapshot_a, "2023-01-03", "2023-01-03")
    state_sync.add_interval(snapshot_b, "2023-01-01", "2023-01-01")
    state_sync.add_interval(snapshot_b, "2023-01-02", "2023-01-02")

    environment_name = "test_max_interval_end_for_environment"

    assert state_sync.greatest_common_interval_end(environment_name, {snapshot_a.name}) is None

    state_sync.promote(
        Environment(
            name=environment_name,
            snapshots=[snapshot_a.table_info, snapshot_b.table_info],
            start_at="2023-01-01",
            end_at="2023-01-03",
            plan_id="test_plan_id",
            previous_finalized_snapshots=[snapshot_b.table_info],
        )
    )

    assert state_sync.greatest_common_interval_end(
        environment_name, {snapshot_a.name}, ensure_finalized_snapshots=True
    ) == to_timestamp("2023-01-03")

    assert state_sync.greatest_common_interval_end(
        environment_name, {snapshot_b.name}, ensure_finalized_snapshots=True
    ) == to_timestamp("2023-01-03")

    assert state_sync.greatest_common_interval_end(
        environment_name, {snapshot_a.name, snapshot_b.name}, ensure_finalized_snapshots=True
    ) == to_timestamp("2023-01-03")

    assert state_sync.greatest_common_interval_end(
        environment_name, {"missing"}, ensure_finalized_snapshots=True
    ) == to_timestamp("2023-01-03")
    assert state_sync.greatest_common_interval_end(environment_name, set()) is None


def test_get_snapshots(mocker):
    mock = mocker.MagicMock()
    cache = CachingStateSync(mock)
    cache.get_snapshots([])
    mock.get_snapshots.assert_not_called()


def test_snapshot_batching(state_sync, mocker, make_snapshot):
    mock = mocker.Mock()

    state_sync.SNAPSHOT_BATCH_SIZE = 2
    state_sync.engine_adapter = mock

    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1")), "1")
    snapshot_b = make_snapshot(SqlModel(name="a", query=parse_one("select 2")), "2")
    snapshot_c = make_snapshot(SqlModel(name="a", query=parse_one("select 3")), "3")

    state_sync.delete_snapshots(
        (
            snapshot_a,
            snapshot_b,
            snapshot_c,
        )
    )
    calls = mock.delete_from.call_args_list
    assert mock.delete_from.call_args_list == [
        call(
            exp.to_table("sqlmesh._snapshots"),
            where=parse_one(
                f"(name, identifier) in (('\"a\"', '{snapshot_b.identifier}'), ('\"a\"', '{snapshot_a.identifier}'))"
            ),
        ),
        call(
            exp.to_table("sqlmesh._snapshots"),
            where=parse_one(f"(name, identifier) in (('\"a\"', '{snapshot_c.identifier}'))"),
        ),
    ]

    mock.fetchall.side_effect = [
        [
            [
                make_snapshot(
                    SqlModel(name="a", query=parse_one("select 1")),
                ).json(),
                "a",
                "1",
                "1",
            ],
            [
                make_snapshot(
                    SqlModel(name="a", query=parse_one("select 2")),
                ).json(),
                "a",
                "2",
                "2",
            ],
        ],
        [
            [
                make_snapshot(
                    SqlModel(name="a", query=parse_one("select 3")),
                ).json(),
                "a",
                "3",
                "3",
            ],
        ],
    ]

    snapshots = state_sync._get_snapshots(
        (
            SnapshotId(name="a", identifier="1"),
            SnapshotId(name="a", identifier="2"),
            SnapshotId(name="a", identifier="3"),
        ),
        hydrate_intervals=False,
    )
    assert len(snapshots) == 3
    calls = mock.fetchall.call_args_list
    assert len(calls) == 2


def test_seed_model_metadata_update(
    state_sync: EngineAdapterStateSync,
    make_snapshot: t.Callable,
):
    model = SeedModel(
        name="a",
        kind=SeedKind(path="./path/to/seed"),
        seed=Seed(content="header\n1\n2"),
        column_hashes={"header": "hash"},
        depends_on=set(),
    )
    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    state_sync.push_snapshots([snapshot])

    model = model.copy(update={"owner": "jen"})
    new_snapshot = make_snapshot(model)
    new_snapshot.previous_versions = snapshot.all_versions
    new_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)

    assert snapshot.fingerprint != new_snapshot.fingerprint
    assert snapshot.version == new_snapshot.version

    state_sync.push_snapshots([new_snapshot])
    assert len(state_sync.get_snapshots([new_snapshot, snapshot])) == 2
