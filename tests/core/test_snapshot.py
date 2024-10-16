import pickle
import json
import typing as t
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, to_column

from sqlmesh.core.audit import StandaloneAudit
from sqlmesh.core.config import (
    AutoCategorizationMode,
    CategorizerConfig,
    EnvironmentSuffixTarget,
)
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse, parse_one
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.model import (
    FullKind,
    IncrementalByTimeRangeKind,
    IncrementalUnmanagedKind,
    Model,
    Seed,
    SeedKind,
    SeedModel,
    SqlModel,
    create_seed_model,
    load_sql_based_model,
)
from sqlmesh.core.model.kind import TimeColumn, ModelKindName
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    QualifiedViewName,
    Snapshot,
    SnapshotId,
    SnapshotChangeCategory,
    SnapshotFingerprint,
    SnapshotTableInfo,
    earliest_start_date,
    fingerprint_from_node,
    has_paused_forward_only,
    missing_intervals,
)
from sqlmesh.core.snapshot.cache import SnapshotCache
from sqlmesh.core.snapshot.categorizer import categorize_change
from sqlmesh.core.snapshot.definition import display_name
from sqlmesh.utils import AttributeDict
from sqlmesh.utils.date import to_date, to_datetime, to_timestamp
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroInfo


@pytest.fixture
def parent_model():
    return SqlModel(
        name="parent.tbl",
        kind=dict(time_column="ds", name=ModelKindName.INCREMENTAL_BY_TIME_RANGE),
        dialect="spark",
        query=parse_one("SELECT 1, ds"),
    )


@pytest.fixture
def model():
    return SqlModel(
        name="name",
        kind=dict(time_column="ds", batch_size=30, name=ModelKindName.INCREMENTAL_BY_TIME_RANGE),
        owner="owner",
        dialect="spark",
        cron="1 0 * * *",
        start="2020-01-01",
        query=parse_one("SELECT @EACH([1, 2], x -> x), ds FROM parent.tbl"),
    )


@pytest.fixture
def snapshot(
    model: Model,
    parent_model: Model,
    monkeypatch: MonkeyPatch,
    mocker: MockerFixture,
    make_snapshot,
):
    mock = mocker.Mock()
    mock.return_value = to_datetime("2022-09-23T00:12:53+00:00")
    monkeypatch.setattr("sqlmesh.utils.date.now", mock)
    snapshot = make_snapshot(
        model,
        nodes={parent_model.fqn: parent_model, model.fqn: model},
    )
    snapshot.version = snapshot.fingerprint.to_version()
    return snapshot


def test_json(snapshot: Snapshot):
    assert json.loads(snapshot.json()) == {
        "created_ts": 1663891973000,
        "ttl": "in 1 week",
        "fingerprint": snapshot.fingerprint.dict(),
        "intervals": [],
        "dev_intervals": [],
        "node": {
            "audits": [],
            "clustered_by": [],
            "cron": "1 0 * * *",
            "kind": {
                "name": "INCREMENTAL_BY_TIME_RANGE",
                "time_column": {"column": "`ds`"},
                "batch_size": 30,
                "forward_only": False,
                "on_destructive_change": "ERROR",
                "disable_restatement": False,
                "dialect": "spark",
            },
            "mapping_schema": {},
            "inline_audits": {},
            "start": "2020-01-01",
            "dialect": "spark",
            "name": "name",
            "partitioned_by": [],
            "project": "",
            "owner": "owner",
            "query": "SELECT @EACH([1, 2], x -> x), ds FROM parent.tbl",
            "jinja_macros": {
                "create_builtins_module": "sqlmesh.utils.jinja",
                "global_objs": {},
                "packages": {},
                "root_macros": {},
                "top_level_packages": [],
            },
            "source_type": "sql",
            "tags": [],
            "grains": [],
            "references": [],
            "allow_partials": False,
            "signals": [],
            "enabled": True,
            "extract_dependencies_from_query": True,
        },
        "audits": [],
        "name": '"name"',
        "parents": [{"name": '"parent"."tbl"', "identifier": snapshot.parents[0].identifier}],
        "previous_versions": [],
        "updated_ts": 1663891973000,
        "version": snapshot.fingerprint.to_version(),
        "migrated": False,
        "unrestorable": False,
    }


def test_add_interval(snapshot: Snapshot, make_snapshot):
    with pytest.raises(ValueError):
        snapshot.add_interval("2020-01-02", "2020-01-01")

    snapshot.add_interval("2020-01-01", "2020-01-01")
    assert snapshot.intervals == [(to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))]

    snapshot.add_interval("2020-01-02", "2020-01-02")
    assert snapshot.intervals == [(to_timestamp("2020-01-01"), to_timestamp("2020-01-03"))]

    snapshot.add_interval("2020-01-04", "2020-01-05")
    assert snapshot.intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-03")),
        (to_timestamp("2020-01-04"), to_timestamp("2020-01-06")),
    ]
    snapshot.add_interval("2019-12-31", "2019-12-31")
    assert snapshot.intervals == [
        (to_timestamp("2019-12-31"), to_timestamp("2020-01-03")),
        (to_timestamp("2020-01-04"), to_timestamp("2020-01-06")),
    ]
    snapshot.add_interval("2020-01-03", "2020-01-03")
    assert snapshot.intervals == [
        (to_timestamp("2019-12-31"), to_timestamp("2020-01-06")),
    ]
    snapshot.add_interval("2019-12-25", "2019-12-26")
    assert snapshot.intervals == [
        (to_timestamp("2019-12-25"), to_timestamp("2019-12-27")),
        (to_timestamp("2019-12-31"), to_timestamp("2020-01-06")),
    ]
    snapshot.add_interval("2019-01-01", "2020-01-30")
    assert snapshot.intervals == [
        (to_timestamp("2019-01-01"), to_timestamp("2020-01-31")),
    ]

    snapshot.add_interval("2020-01-01", "2020-01-31 00:00:00")
    assert snapshot.intervals == [
        (to_timestamp("2019-01-01"), to_timestamp("2020-01-31")),
    ]
    snapshot.add_interval("2019-01-01 00:00:00", "2020-01-31 00:00:01")
    assert snapshot.intervals == [
        (to_timestamp("2019-01-01"), to_timestamp("2020-01-31")),
    ]
    snapshot.add_interval("2018-12-31 23:59:59", "2020-01-31 12:00:01")
    assert snapshot.intervals == [
        (to_timestamp("2019-01-01"), to_timestamp("2020-01-31")),
    ]

    new_snapshot = make_snapshot(snapshot.model)
    new_snapshot.add_interval("2020-01-29", "2020-02-01")
    new_snapshot.add_interval("2020-02-05", "2020-02-10")
    new_snapshot.merge_intervals(snapshot)
    assert new_snapshot.intervals == [
        (to_timestamp("2019-01-01"), to_timestamp("2020-02-02")),
        (to_timestamp("2020-02-05"), to_timestamp("2020-02-11")),
    ]


def test_add_interval_dev(snapshot: Snapshot, make_snapshot):
    snapshot.version = "existing_version"
    snapshot.change_category = SnapshotChangeCategory.FORWARD_ONLY

    snapshot.add_interval("2020-01-01", "2020-01-01")
    assert snapshot.intervals == [(to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))]

    snapshot.add_interval("2020-01-02", "2020-01-02", is_dev=True)
    assert snapshot.intervals == [(to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))]
    assert snapshot.dev_intervals == [(to_timestamp("2020-01-02"), to_timestamp("2020-01-03"))]

    new_snapshot = make_snapshot(snapshot.model)
    new_snapshot.merge_intervals(snapshot)
    assert new_snapshot.intervals == [(to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))]
    assert new_snapshot.dev_intervals == []

    new_snapshot = make_snapshot(snapshot.model)
    new_snapshot.previous_versions = snapshot.all_versions
    new_snapshot.migrated = True
    new_snapshot.merge_intervals(snapshot)
    assert new_snapshot.intervals == [(to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))]
    assert new_snapshot.dev_intervals == [(to_timestamp("2020-01-02"), to_timestamp("2020-01-03"))]


def test_add_interval_partial(snapshot: Snapshot, make_snapshot):
    snapshot.add_interval("2023-01-01 00:00:00", "2023-01-01 23:59:59")
    assert snapshot.intervals == []

    snapshot.add_interval("2023-01-01 00:00:00", "2023-01-01 14:00:00")
    assert snapshot.intervals == []

    snapshot.add_interval("2023-01-01 15:00:00", "2023-01-03 00:00:00")
    assert snapshot.intervals == [
        (to_timestamp("2023-01-02"), to_timestamp("2023-01-03")),
    ]

    monthly_snapshot = make_snapshot(
        SqlModel(name="test_model", cron="0 7 1 * *", query=parse_one("SELECT 1"))
    )
    monthly_snapshot.add_interval("2023-01-01 15:00:00", "2023-01-01 16:00:00")
    assert monthly_snapshot.intervals == []

    monthly_snapshot.add_interval("2023-01-01 15:00:00", "2023-03-01 16:00:00")
    assert monthly_snapshot.intervals == [
        (to_timestamp("2023-02-01"), to_timestamp("2023-03-01")),
    ]


def test_missing_intervals(snapshot: Snapshot):
    snapshot.add_interval("2020-01-01", "2020-01-01")
    snapshot.add_interval("2020-01-03", "2020-01-05")
    assert snapshot.missing_intervals("2020-01-01", "2020-01-01") == []
    assert snapshot.missing_intervals("2020-01-02", "2020-01-02") == [
        (to_timestamp("2020-01-02"), to_timestamp("2020-01-03"))
    ]
    assert snapshot.missing_intervals("2020-01-02", "2020-01-03") == [
        (to_timestamp("2020-01-02"), to_timestamp("2020-01-03"))
    ]
    assert snapshot.missing_intervals("2020-01-01", "2020-01-03") == [
        (to_timestamp("2020-01-02"), to_timestamp("2020-01-03"))
    ]
    assert snapshot.missing_intervals("2020-01-03", "2020-01-03") == []
    assert snapshot.missing_intervals("2020-01-03", "2020-01-05") == []
    assert snapshot.missing_intervals("2020-01-03", "2020-01-06") == [
        (to_timestamp("2020-01-06"), to_timestamp("2020-01-07"))
    ]
    assert snapshot.missing_intervals("2020-01-03", "2020-01-07") == [
        (to_timestamp("2020-01-06"), to_timestamp("2020-01-07")),
        (to_timestamp("2020-01-07"), to_timestamp("2020-01-08")),
    ]
    assert snapshot.missing_intervals("2020-01-03 00:00:01", "2020-01-05 00:00:02") == []
    assert snapshot.missing_intervals("2020-01-03 00:00:01", "2020-01-07 00:00:02") == [
        (to_timestamp("2020-01-06"), to_timestamp("2020-01-07")),
    ]


def test_missing_intervals_partial(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="test_model",
            kind=IncrementalByTimeRangeKind(time_column=TimeColumn(column="ds")),
            owner="owner",
            cron="@daily",
            query=parse_one("SELECT 1, ds FROM name"),
            allow_partials=True,
        )
    )

    start = "2023-01-01"
    end_ts = to_timestamp(start) + 1000
    assert snapshot.missing_intervals(start, end_ts) == [
        (to_timestamp(start), end_ts),
    ]
    assert snapshot.missing_intervals(start, end_ts, execution_time=end_ts) == [
        (to_timestamp(start), end_ts)
    ]
    assert snapshot.missing_intervals(start, start) == [
        (to_timestamp(start), to_timestamp("2023-01-02")),
    ]
    assert snapshot.missing_intervals(start, start, execution_time=start, ignore_cron=True) == []
    assert snapshot.missing_intervals(start, start, execution_time=end_ts, end_bounded=True) == []


def test_missing_intervals_end_bounded_with_lookback(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="test_model",
            kind=IncrementalByTimeRangeKind(time_column=TimeColumn(column="ds"), lookback=1),
            owner="owner",
            cron="@daily",
            query=parse_one("SELECT 1, ds FROM name"),
        )
    )

    start = "2023-01-01"
    end = "2023-01-02"

    snapshot.intervals = [(to_timestamp(start), to_timestamp(end))]

    execution_ts = to_timestamp("2023-01-03")
    assert snapshot.missing_intervals(start, start, execution_time=execution_ts) == []
    assert snapshot.missing_intervals(start, end, execution_time=execution_ts) == [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
        (to_timestamp("2023-01-02"), to_timestamp("2023-01-03")),
    ]


def test_missing_intervals_end_bounded_with_ignore_cron(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="test_model",
            kind=IncrementalByTimeRangeKind(time_column=TimeColumn(column="ds")),
            owner="owner",
            cron="1 0 * * *",
            query=parse_one("SELECT 1, ds FROM name"),
        )
    )

    start = "2023-01-01"
    end = "2023-01-03"

    snapshot.intervals = [(to_timestamp(start), to_timestamp("2023-01-02"))]

    execution_ts = to_timestamp(end)
    assert snapshot.missing_intervals(start, end, execution_time=execution_ts) == []
    assert snapshot.missing_intervals(
        start, end, execution_time=execution_ts, ignore_cron=True
    ) == [
        (to_timestamp("2023-01-02"), to_timestamp(end)),
    ]
    assert (
        snapshot.missing_intervals(
            start, to_datetime(end), execution_time=execution_ts, end_bounded=True
        )
        == []
    )
    assert snapshot.missing_intervals(
        start, to_datetime(end), execution_time=execution_ts, ignore_cron=True, end_bounded=True
    ) == [
        (to_timestamp("2023-01-02"), to_timestamp(end)),
    ]


def test_missing_intervals_past_end_date_with_lookback(make_snapshot):
    snapshot: Snapshot = make_snapshot(  # type: ignore
        SqlModel(
            name="test_model",
            kind=IncrementalByTimeRangeKind(time_column=TimeColumn(column="ds"), lookback=2),
            owner="owner",
            cron="@daily",
            query=parse_one("SELECT 1, ds FROM name"),
            start="2023-01-01",
            end="2023-01-05",  # inclusive, equivalent to to_timestamp('2023-01-05 23:59:59.999999')
        )
    )

    start_time = to_timestamp("2023-01-01")
    end_time = to_timestamp(
        "2023-01-06"
    )  # exclusive because to_timestamp() returns a timestamp and not a date
    assert snapshot.inclusive_exclusive(snapshot.node.start, snapshot.node.end) == (
        start_time,
        end_time,
    )

    # baseline - all intervals missing
    assert snapshot.missing_intervals(start_time, end_time, execution_time=end_time) == [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
        (to_timestamp("2023-01-02"), to_timestamp("2023-01-03")),
        (to_timestamp("2023-01-03"), to_timestamp("2023-01-04")),
        (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
        (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
    ]

    # fully backfill model - no intervals missing
    snapshot.add_interval(start_time, end_time)

    # even though lookback=2, because every interval has been filled,
    # there should be no missing intervals
    assert snapshot.missing_intervals(start_time, end_time, execution_time=end_time) == []

    # however, when running for a new interval, this triggers lookback
    # in this case, we remove the most recent interval (the one for 2023-01-05) to simulate it being new
    # since lookback=2 days, this triggers missing intervals for 2023-01-03, 2023-01-04, 2023-01-05
    snapshot.remove_interval(interval=(to_timestamp("2023-01-05"), to_timestamp("2023-01-06")))
    assert snapshot.missing_intervals(start_time, end_time, execution_time=end_time) == [
        (to_timestamp("2023-01-03"), to_timestamp("2023-01-04")),
        (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
        (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
    ]

    # put the interval we just removed back to make the model fully backfilled again
    snapshot.add_interval(to_timestamp("2023-01-05"), to_timestamp("2023-01-06"))
    assert snapshot.missing_intervals(start_time, end_time, execution_time=end_time) == []

    # running on the end date + 1 day (2023-01-07)
    # 2023-01-06 "would" run and since lookback=2 this pulls in 2023-01-04 and 2023-01-05 as well
    # however, only 2023-01-04 and 2023-01-05 are within the model end date
    end_time = to_timestamp("2023-01-07")
    assert snapshot.missing_intervals(start_time, end_time, execution_time=end_time) == [
        (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
        (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
    ]

    # running on the end date + 2 days (2023-01-08)
    # 2023-01-07 "would" run and since lookback=2 this pulls in 2023-01-06 and 2023-01-05 as well
    # however, only 2023-01-05 is within the model end date
    end_time = to_timestamp("2023-01-08")
    assert snapshot.missing_intervals(start_time, end_time, execution_time=end_time) == [
        (to_timestamp("2023-01-05"), to_timestamp("2023-01-06"))
    ]

    # running on the end date + 3 days (2023-01-09)
    # no missing intervals because subtracting 2 days for lookback exceeds the models end date
    end_time = to_timestamp("2023-01-09")
    assert snapshot.missing_intervals(start_time, end_time, execution_time=end_time) == []

    # running way in the future, no missing intervals because subtracting 2 days for lookback still exceeds the models end date
    end_time = to_timestamp("2024-01-01")
    assert snapshot.missing_intervals(start_time, end_time, execution_time=end_time) == []


def test_incremental_time_self_reference(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column=TimeColumn(column="ds"), batch_size=1),
            owner="owner",
            dialect="",
            cron="@daily",
            start="1 week ago",
            query=parse_one("SELECT id, @end_ds as ds FROM name"),
        )
    )
    snapshot.add_interval(to_date("1 week ago"), to_date("1 day ago"))
    assert snapshot.missing_intervals(to_date("1 week ago"), to_date("1 day ago")) == []
    # Remove should take away not only 3 days ago but also everything after since this model
    # depends on past
    interval = snapshot.get_removal_interval(to_date("3 days ago"), to_date("3 days ago"))
    snapshot.remove_interval(interval)
    assert snapshot.missing_intervals(to_date("1 week ago"), to_date("1 day ago")) == [
        (to_timestamp(to_date("3 days ago")), to_timestamp(to_date("2 days ago"))),
        (to_timestamp(to_date("2 days ago")), to_timestamp(to_date("1 days ago"))),
        (to_timestamp(to_date("1 days ago")), to_timestamp(to_date("today"))),
    ]


def test_lookback(snapshot: Snapshot, make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column="ds", lookback=2),
            cron="0 5 * * *",
            start="2023-01-01",
            query=parse_one("SELECT ds FROM parent.tbl"),
        )
    )

    assert snapshot.missing_intervals("2023-01-01", "2023-01-01") == [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
    ]

    snapshot.add_interval("2023-01-01", "2023-01-04")
    assert snapshot.missing_intervals("2023-01-01", "2023-01-04") == []
    assert snapshot.missing_intervals("2023-01-01", "2023-01-05") == [
        (to_timestamp("2023-01-03"), to_timestamp("2023-01-04")),
        (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
        (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
    ]

    snapshot.add_interval("2023-01-06", "2023-01-07")
    assert snapshot.missing_intervals("2023-01-03", "2023-01-03") == []
    assert snapshot.missing_intervals("2023-01-05", "2023-01-05") == [
        (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
    ]
    assert snapshot.missing_intervals("2023-01-06", "2023-01-07") == []
    assert snapshot.missing_intervals("2023-01-05", "2023-01-08") == [
        (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
        (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
        (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
        (to_timestamp("2023-01-08"), to_timestamp("2023-01-09")),
    ]

    snapshot.add_interval("2023-01-28", "2023-01-29")
    assert snapshot.missing_intervals("2023-01-27", "2023-01-27", "2023-01-30 05:00:00") == [
        (to_timestamp("2023-01-27"), to_timestamp("2023-01-28")),
    ]
    assert snapshot.missing_intervals("2023-01-28", "2023-01-29", "2023-01-31 05:00:00") == []
    assert snapshot.missing_intervals("2023-01-28", "2023-01-30", "2023-01-31 05:00:00") == [
        (to_timestamp("2023-01-28"), to_timestamp("2023-01-29")),
        (to_timestamp("2023-01-29"), to_timestamp("2023-01-30")),
        (to_timestamp("2023-01-30"), to_timestamp("2023-01-31")),
    ]

    assert snapshot.missing_intervals("2023-01-28", "2023-01-30", "2023-01-31 04:00:00") == []


def test_seed_intervals(make_snapshot):
    snapshot_a = make_snapshot(
        SeedModel(
            name="a",
            kind=SeedKind(path="./path/to/seed"),
            seed=Seed(content="content"),
            column_hashes={"col": "hash"},
            depends_on=set(),
        )
    )

    assert snapshot_a.missing_intervals("2020-01-01", "2020-01-01") == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))
    ]
    snapshot_a.add_interval("2020-01-01", "2020-01-01")
    assert snapshot_a.missing_intervals("2020-01-02", "2020-01-02") == []


def test_missing_interval_smaller_than_interval_unit(make_snapshot):
    snapshot_daily = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column="ds", batch_size=30),
            owner="owner",
            dialect="spark",
            cron="@daily",
            start="2020-01-01",
            query=parse_one("SELECT @EACH([1, 2], x -> x), ds FROM parent.tbl"),
        )
    )

    assert snapshot_daily.missing_intervals("2020-01-01 00:00:05", "2020-01-01 23:59:59") == []
    assert snapshot_daily.missing_intervals("2020-01-01 00:00:00", "2020-01-02 00:00:00") == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))
    ]

    snapshot_hourly = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column="ds", batch_size=30),
            owner="owner",
            dialect="spark",
            cron="@hourly",
            start="2020-01-01",
            query=parse_one("SELECT @EACH([1, 2], x -> x), ds FROM parent.tbl"),
        )
    )

    assert snapshot_hourly.missing_intervals("2020-01-01 00:00:00", "2020-01-01 00:59:00") == []
    assert snapshot_hourly.missing_intervals("2020-01-01 00:00:00", "2020-01-01 01:00:00") == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-01 01:00:00"))
    ]

    snapshot_end_categorical = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column="ds", batch_size=30),
            owner="owner",
            dialect="spark",
            cron="@daily",
            start="2020-01-01",
            query=parse_one("SELECT @EACH([1, 2], x -> x), ds FROM parent.tbl"),
        )
    )

    assert snapshot_end_categorical.missing_intervals("2020-01-01 00:00:00", "2020-01-01") == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))
    ]

    snapshot_partial = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column="ds", batch_size=30),
            owner="owner",
            dialect="spark",
            cron="@daily",
            start="2020-01-01",
            query=parse_one("SELECT @EACH([1, 2], x -> x), ds FROM parent.tbl"),
            allow_partials=True,
        )
    )

    assert snapshot_partial.missing_intervals("2020-01-01 00:00:05", "2020-01-01 23:59:59") == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-01 23:59:59"))
    ]
    assert snapshot_partial.missing_intervals("2020-01-01 00:00:00", "2020-01-02 00:00:00") == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))
    ]


def test_remove_intervals(snapshot: Snapshot):
    snapshot.add_interval("2020-01-01", "2020-01-01")
    snapshot.remove_interval(snapshot.get_removal_interval("2020-01-01", "2020-01-01"))
    assert snapshot.intervals == []

    snapshot.add_interval("2020-01-01", "2020-01-01")
    snapshot.add_interval("2020-01-03", "2020-01-03")
    snapshot.remove_interval(snapshot.get_removal_interval("2020-01-01", "2020-01-01"))
    assert snapshot.intervals == [(to_timestamp("2020-01-03"), to_timestamp("2020-01-04"))]

    snapshot.remove_interval(snapshot.get_removal_interval("2020-01-01", "2020-01-05"))
    assert snapshot.intervals == []

    snapshot.add_interval("2020-01-01", "2020-01-05")
    snapshot.add_interval("2020-01-07", "2020-01-10")
    snapshot.remove_interval(snapshot.get_removal_interval("2020-01-03", "2020-01-04"))
    assert snapshot.intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-03")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-06")),
        (to_timestamp("2020-01-07"), to_timestamp("2020-01-11")),
    ]
    snapshot.remove_interval(snapshot.get_removal_interval("2020-01-01", "2020-01-01"))
    assert snapshot.intervals == [
        (to_timestamp("2020-01-02"), to_timestamp("2020-01-03")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-06")),
        (to_timestamp("2020-01-07"), to_timestamp("2020-01-11")),
    ]
    snapshot.remove_interval(snapshot.get_removal_interval("2020-01-10", "2020-01-10"))
    assert snapshot.intervals == [
        (to_timestamp("2020-01-02"), to_timestamp("2020-01-03")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-06")),
        (to_timestamp("2020-01-07"), to_timestamp("2020-01-10")),
    ]
    snapshot.remove_interval(snapshot.get_removal_interval("2020-01-07", "2020-01-07"))
    assert snapshot.intervals == [
        (to_timestamp("2020-01-02"), to_timestamp("2020-01-03")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-06")),
        (to_timestamp("2020-01-08"), to_timestamp("2020-01-10")),
    ]
    snapshot.remove_interval(snapshot.get_removal_interval("2020-01-06", "2020-01-21"))
    assert snapshot.intervals == [
        (to_timestamp("2020-01-02"), to_timestamp("2020-01-03")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-06")),
    ]
    snapshot.remove_interval(snapshot.get_removal_interval("2019-01-01", "2022-01-01"))
    assert snapshot.intervals == []


def test_get_removal_intervals_full_history_restatement_model(make_snapshot):
    execution_time = to_timestamp("2024-01-02")
    model = SqlModel(
        name="name",
        kind=IncrementalUnmanagedKind(),
        query=parse_one("SELECT 1"),
    )
    snapshot = make_snapshot(model)
    snapshot.add_interval("2020-01-01", "2024-01-01")

    interval = snapshot.get_removal_interval(
        "2023-01-01",
        "2023-01-01",
        execution_time=execution_time,
        is_preview=True,
    )
    assert interval == (to_timestamp("2023-01-01"), execution_time)

    interval = snapshot.get_removal_interval(
        "2023-01-01", "2023-01-01", execution_time=execution_time
    )
    assert interval == (to_timestamp("2020-01-01"), execution_time)
    snapshot.remove_interval(interval)

    assert not snapshot.intervals
    interval = snapshot.get_removal_interval(
        "2023-01-01", "2023-01-01", execution_time=execution_time
    )
    assert interval == (to_timestamp("2023-01-01"), execution_time)


each_macro = lambda: "test"  # noqa: E731


def test_fingerprint(model: Model, parent_model: Model):
    original_model = deepcopy(model)
    fingerprint = fingerprint_from_node(model, nodes={})

    original_fingerprint = SnapshotFingerprint(
        data_hash="720070643",
        metadata_hash="2793463216",
    )

    assert fingerprint == original_fingerprint

    with_parent_fingerprint = fingerprint_from_node(model, nodes={'"parent"."tbl"': parent_model})
    assert with_parent_fingerprint != fingerprint
    assert int(with_parent_fingerprint.parent_data_hash) > 0
    assert int(with_parent_fingerprint.parent_metadata_hash) > 0

    assert (
        fingerprint_from_node(
            model,
            nodes={
                '"parent"."tbl"': SqlModel(**{**model.dict(), "query": parse_one("select 2, ds")})
            },
        )
        != with_parent_fingerprint
    )
    model = SqlModel(**{**model.dict(), "query": parse_one("select 1, ds")})
    new_fingerprint = fingerprint_from_node(model, nodes={})
    assert new_fingerprint != fingerprint
    assert new_fingerprint.data_hash != fingerprint.data_hash
    assert new_fingerprint.metadata_hash == fingerprint.metadata_hash

    model = SqlModel(**{**model.dict(), "query": parse_one("select 1, ds -- annotation")})
    fingerprint = fingerprint_from_node(model, nodes={})
    assert new_fingerprint != fingerprint
    assert new_fingerprint.data_hash == fingerprint.data_hash
    assert new_fingerprint.metadata_hash != fingerprint.metadata_hash

    model = SqlModel(
        **{**original_model.dict(), "pre_statements": [parse_one("CREATE TABLE test")]}
    )
    fingerprint = fingerprint_from_node(model, nodes={})
    assert new_fingerprint != fingerprint
    assert new_fingerprint.data_hash != fingerprint.data_hash
    assert new_fingerprint.metadata_hash == fingerprint.metadata_hash

    model = SqlModel(**{**original_model.dict(), "post_statements": [parse_one("DROP TABLE test")]})
    fingerprint = fingerprint_from_node(model, nodes={})
    assert new_fingerprint != fingerprint
    assert new_fingerprint.data_hash != fingerprint.data_hash
    assert new_fingerprint.metadata_hash == fingerprint.metadata_hash


def test_fingerprint_seed_model():
    expressions = parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path '../seeds/waiter_names.csv'
            )
        );
    """
    )

    expected_fingerprint = SnapshotFingerprint(
        data_hash="2233743260",
        metadata_hash="3403817841",
    )

    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))
    actual_fingerprint = fingerprint_from_node(model, nodes={})
    assert actual_fingerprint == expected_fingerprint

    # Make sure that the fingerprint doesn't change when the model is dehydrated.
    dehydrated_model = model.to_dehydrated()
    assert fingerprint_from_node(dehydrated_model, nodes={}) == expected_fingerprint

    updated_model = model.copy(
        update={
            "columns_to_types_": {
                "id": exp.DataType.build("int"),
                "name": exp.DataType.build("text"),
            }
        }
    )
    updated_actual_fingerprint = fingerprint_from_node(updated_model, nodes={})
    assert updated_actual_fingerprint.data_hash != expected_fingerprint.data_hash
    assert updated_actual_fingerprint.metadata_hash == expected_fingerprint.metadata_hash


def test_fingerprint_jinja_macros(model: Model):
    model = SqlModel(
        **{
            **model.dict(),
            "jinja_macros": JinjaMacroRegistry(
                root_macros={
                    "test_macro": MacroInfo(
                        definition="{% macro test_macro() %}a{% endmacro %}", depends_on=[]
                    )
                }
            ),
        }
    )
    original_fingerprint = SnapshotFingerprint(
        data_hash="1896101134",
        metadata_hash="2793463216",
    )

    fingerprint = fingerprint_from_node(model, nodes={})
    assert fingerprint == original_fingerprint
    model = model.copy()

    model.jinja_macros.root_macros["test_macro"] = MacroInfo(
        definition="{% macro test_macro() %}b{% endmacro %}", depends_on=[]
    )
    updated_fingerprint = fingerprint_from_node(model, nodes={})
    assert updated_fingerprint.data_hash != original_fingerprint.data_hash
    assert updated_fingerprint.metadata_hash == original_fingerprint.metadata_hash


@pytest.mark.parametrize("global_obj_key", ["refs", "sources", "vars"])
def test_fingerprint_jinja_macros_global_objs(model: Model, global_obj_key: str):
    model = SqlModel(
        **{
            **model.dict(),
            "jinja_macros": JinjaMacroRegistry(),
        }
    )
    fingerprint = fingerprint_from_node(model, nodes={})
    model = model.copy()
    model.jinja_macros.global_objs[global_obj_key] = AttributeDict({"test": "test"})
    updated_fingerprint = fingerprint_from_node(model, nodes={})
    assert updated_fingerprint.data_hash != fingerprint.data_hash
    assert updated_fingerprint.metadata_hash == fingerprint.metadata_hash


def test_fingerprint_builtin_audits(model: Model, parent_model: Model):
    fingerprint = fingerprint_from_node(model, nodes={})

    model = SqlModel.parse_obj(
        {**model.dict(), "audits": [("unique_values", {"columns": exp.convert([to_column("a")])})]}
    )
    new_fingerprint = fingerprint_from_node(model, nodes={})
    assert new_fingerprint != fingerprint


def test_fingerprint_standalone_audits(parent_model: Model):
    audit = StandaloneAudit(
        name="test_standalone_audit",
        query=parse_one(f"SELECT cola FROM {parent_model.name} WHERE cola IS NULL"),
    )
    fingerprint = fingerprint_from_node(audit, nodes={parent_model.name: parent_model})

    new_audit = StandaloneAudit(
        name="test_standalone_audit",
        query=parse_one(f"SELECT colb FROM {parent_model.name} WHERE colb IS NULL"),
    )
    new_fingerprint = fingerprint_from_node(new_audit, nodes={parent_model.name: parent_model})

    assert new_fingerprint != fingerprint
    assert new_fingerprint.data_hash == fingerprint.data_hash
    assert new_fingerprint.metadata_hash != fingerprint.metadata_hash


def test_fingerprint_virtual_properties(model: Model, parent_model: Model):
    original_model = deepcopy(model)
    fingerprint = fingerprint_from_node(model, nodes={})

    updated_model = SqlModel(
        **original_model.dict(),
        virtual_properties=parse_one("(labels = [('test-virtual-label', 'label-virtual-value')],)"),
    )
    assert "labels" in updated_model.virtual_properties
    updated_fingerprint = fingerprint_from_node(updated_model, nodes={})

    assert updated_fingerprint != fingerprint
    assert updated_fingerprint.metadata_hash != fingerprint.metadata_hash
    assert updated_fingerprint.data_hash == fingerprint.data_hash


def test_stamp(model: Model):
    original_fingerprint = fingerprint_from_node(model, nodes={})

    stamped_model = SqlModel(**{**model.dict(), "stamp": "test_stamp"})
    stamped_fingerprint = fingerprint_from_node(stamped_model, nodes={})

    assert original_fingerprint != stamped_fingerprint


def test_table_name(snapshot: Snapshot, make_snapshot: t.Callable):
    # Mimic a direct breaking change.
    snapshot.fingerprint = SnapshotFingerprint(
        data_hash="1", metadata_hash="1", parent_data_hash="1"
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot.previous_versions = ()
    assert snapshot.table_name(is_deployable=True) == "sqlmesh__default.name__3078928823"
    assert snapshot.table_name(is_deployable=False) == "sqlmesh__default.name__3078928823__temp"

    # Mimic an indirect non-breaking change.
    previous_data_version = snapshot.data_version
    assert previous_data_version.physical_schema == "sqlmesh__default"
    snapshot.fingerprint = SnapshotFingerprint(
        data_hash="1", metadata_hash="1", parent_data_hash="2"
    )
    snapshot.previous_versions = (previous_data_version,)
    snapshot.categorize_as(SnapshotChangeCategory.INDIRECT_NON_BREAKING)
    assert snapshot.table_name(is_deployable=True) == "sqlmesh__default.name__3078928823"
    # Indirect non-breaking snapshots reuse the dev table as well.
    assert snapshot.table_name(is_deployable=False) == "sqlmesh__default.name__3078928823__temp"

    # Mimic a direct forward-only change.
    snapshot.fingerprint = SnapshotFingerprint(
        data_hash="2", metadata_hash="1", parent_data_hash="1"
    )
    snapshot.previous_versions = (previous_data_version,)
    snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    assert snapshot.table_name(is_deployable=True) == "sqlmesh__default.name__3078928823"
    assert snapshot.table_name(is_deployable=False) == "sqlmesh__default.name__3049392110__temp"

    fully_qualified_snapshot = make_snapshot(
        SqlModel(name='"my-catalog".db.table', query=parse_one("select 1, ds"))
    )
    fully_qualified_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    assert (
        fully_qualified_snapshot.table_name()
        == f'"my-catalog".sqlmesh__db.db__table__{fully_qualified_snapshot.version}'
    )
    non_fully_qualified_snapshot = make_snapshot(
        SqlModel(
            name="db.table", query=parse_one("select 1, ds"), default_catalog='"other-catalog"'
        )
    )
    non_fully_qualified_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    assert (
        non_fully_qualified_snapshot.table_name(is_deployable=True)
        == f'"other-catalog".sqlmesh__db.db__table__{non_fully_qualified_snapshot.version}'
    )


def test_categorize_change_sql(make_snapshot):
    old_snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))

    config = CategorizerConfig(sql=AutoCategorizationMode.SEMI)

    # A projection has been added.
    assert (
        categorize_change(
            new=make_snapshot(SqlModel(name="a", query=parse_one("select 1, 2, ds"))),
            old=old_snapshot,
            config=config,
        )
        == SnapshotChangeCategory.NON_BREAKING
    )

    # A complex projection has been added.
    assert (
        categorize_change(
            new=make_snapshot(
                SqlModel(
                    name="a", query=parse_one("select 1, fun(another_fun(a + 1) * 2)::INT, ds")
                )
            ),
            old=old_snapshot,
            config=config,
        )
        == SnapshotChangeCategory.NON_BREAKING
    )

    # Multiple projections have been added.
    assert (
        categorize_change(
            new=make_snapshot(SqlModel(name="a", query=parse_one("select 1, 2, a, b, ds"))),
            old=old_snapshot,
            config=config,
        )
        == SnapshotChangeCategory.NON_BREAKING
    )

    # No change.
    with pytest.raises(SQLMeshError):
        categorize_change(old_snapshot, old_snapshot, config=config)

    # A projection has been removed.
    assert (
        categorize_change(
            new=make_snapshot(SqlModel(name="a", query=parse_one("select ds"))),
            old=old_snapshot,
            config=config,
        )
        is None
    )

    # A projection has been replaced.
    assert (
        categorize_change(
            new=make_snapshot(SqlModel(name="a", query=parse_one("select 2, ds"))),
            old=old_snapshot,
            config=config,
        )
        is None
    )

    # A projection has been moved.
    assert (
        categorize_change(
            new=make_snapshot(SqlModel(name="a", query=parse_one("select ds, 1"))),
            old=old_snapshot,
            config=config,
        )
        is None
    )

    # A WHERE clause has been added.
    assert (
        categorize_change(
            new=make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds WHERE a = 2"))),
            old=old_snapshot,
            config=config,
        )
        is None
    )

    # A FROM clause has been added.
    assert (
        categorize_change(
            new=make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds FROM test_table"))),
            old=old_snapshot,
            config=config,
        )
        is None
    )

    # DISTINCT has been added.
    assert (
        categorize_change(
            new=make_snapshot(SqlModel(name="a", query=parse_one("select DISTINCT 1, ds"))),
            old=old_snapshot,
            config=config,
        )
        is None
    )

    # An EXPLODE projection has been added.
    assert (
        categorize_change(
            new=make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds, explode(a)"))),
            old=old_snapshot,
            config=config,
        )
        is None
    )

    # An EXPLODE_OUTER projection has been added.
    assert (
        categorize_change(
            new=make_snapshot(
                SqlModel(name="a", query=parse_one("select 1, ds, explode_outer(a)"))
            ),
            old=old_snapshot,
            config=config,
        )
        is None
    )

    # A POSEXPLODE projection has been added.
    assert (
        categorize_change(
            new=make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds, posexplode(a)"))),
            old=old_snapshot,
            config=config,
        )
        is None
    )

    # A POSEXPLODE_OUTER projection has been added.
    assert (
        categorize_change(
            new=make_snapshot(
                SqlModel(name="a", query=parse_one("select 1, ds, posexplode_outer(a)"))
            ),
            old=old_snapshot,
            config=config,
        )
        is None
    )

    # An UNNEST projection has been added.
    assert (
        categorize_change(
            new=make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds, unnest(a)"))),
            old=old_snapshot,
            config=config,
        )
        is None
    )

    # A metadata change occurred
    assert (
        categorize_change(
            new=make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds"), owner="foo")),
            old=old_snapshot,
            config=config,
        )
        is SnapshotChangeCategory.NON_BREAKING
    )

    # A UDTF subquery has been added.
    assert (
        categorize_change(
            new=make_snapshot(
                SqlModel(name="a", query=parse_one("select 1, ds, (select x from unnest(a) x)"))
            ),
            old=old_snapshot,
            config=config,
        )
        is SnapshotChangeCategory.NON_BREAKING
    )
    assert (
        categorize_change(
            new=make_snapshot(
                SqlModel(name="a", query=parse_one("select 1, ds, (select x from posexplode(a) x)"))
            ),
            old=old_snapshot,
            config=config,
        )
        is SnapshotChangeCategory.NON_BREAKING
    )


def test_categorize_change_seed(make_snapshot, tmp_path):
    config = CategorizerConfig(seed=AutoCategorizationMode.SEMI)
    model_name = "test_db.test_seed_model"
    seed_path = tmp_path / "seed.csv"
    model_kind = SeedKind(path=str(seed_path.absolute()))

    with open(seed_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
col_a,col_b,col_c
1,text_a,1.0
2,text_b,2.0"""
        )

    original_snapshot = make_snapshot(create_seed_model(model_name, model_kind))

    # New column.
    with open(seed_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
col_a,col_d,col_b,col_c
1,,text_a,1.0
2,test,text_b,2.0"""
        )

    assert (
        categorize_change(
            new=make_snapshot(create_seed_model(model_name, model_kind)),
            old=original_snapshot,
            config=config,
        )
        == SnapshotChangeCategory.NON_BREAKING
    )

    # Column removed.
    with open(seed_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
col_a,col_c
1,1.0
2,2.0"""
        )

    assert (
        categorize_change(
            new=make_snapshot(create_seed_model(model_name, model_kind)),
            old=original_snapshot,
            config=config,
        )
        is None
    )

    # Column renamed.
    with open(seed_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
col_a,col_b,col_d
1,text_a,1.0
2,text_b,2.0"""
        )

    assert (
        categorize_change(
            new=make_snapshot(create_seed_model(model_name, model_kind)),
            old=original_snapshot,
            config=config,
        )
        is None
    )

    # New row.
    with open(seed_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
col_a,col_b,col_c
1,text_a,1.0
2,text_b,2.0
3,text_c,3.0"""
        )

    assert (
        categorize_change(
            new=make_snapshot(create_seed_model(model_name, model_kind)),
            old=original_snapshot,
            config=config,
        )
        is None
    )

    # Deleted row.
    with open(seed_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
col_a,col_b,col_c
1,text_a,1.0"""
        )

    assert (
        categorize_change(
            new=make_snapshot(create_seed_model(model_name, model_kind)),
            old=original_snapshot,
            config=config,
        )
        is None
    )

    # Numeric column changed.
    with open(seed_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
col_a,col_b,col_c
1,text_a,1.0
2,text_b,3.0"""
        )

    assert (
        categorize_change(
            new=make_snapshot(create_seed_model(model_name, model_kind)),
            old=original_snapshot,
            config=config,
        )
        is None
    )

    # Text column changed.
    with open(seed_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
col_a,col_b,col_c
1,text_a,1.0
2,text_c,2.0"""
        )

    assert (
        categorize_change(
            new=make_snapshot(create_seed_model(model_name, model_kind)),
            old=original_snapshot,
            config=config,
        )
        is None
    )

    # Column type changed.
    with open(seed_path, "w", encoding="utf-8") as fd:
        fd.write(
            """
col_a,col_b,col_c
1,text_a,1.0
2.0,text_b,2.0"""
        )

    assert (
        categorize_change(
            new=make_snapshot(create_seed_model(model_name, model_kind)),
            old=original_snapshot,
            config=config,
        )
        is None
    )


def test_effective_from(snapshot: Snapshot):
    previous_snapshot = deepcopy(snapshot)
    previous_snapshot.add_interval("2023-01-01", "2023-01-05")
    previous_snapshot.add_interval("2023-01-07", "2023-01-09")

    new_snapshot_same_fingerprint = deepcopy(snapshot)
    new_snapshot_same_fingerprint.effective_from = "2023-01-04"
    new_snapshot_same_fingerprint.merge_intervals(previous_snapshot)

    assert new_snapshot_same_fingerprint.intervals == [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-06")),
        (to_timestamp("2023-01-07"), to_timestamp("2023-01-10")),
    ]

    new_snapshot_different_fingerprint = deepcopy(snapshot)
    new_snapshot_different_fingerprint.fingerprint = SnapshotFingerprint(
        data_hash="1", metadata_hash="1", parent_data_hash="1"
    )
    new_snapshot_different_fingerprint.effective_from = "2023-01-04"
    new_snapshot_different_fingerprint.merge_intervals(previous_snapshot)

    assert new_snapshot_different_fingerprint.intervals == [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-04")),
    ]


def test_physical_schema(snapshot: Snapshot):
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    assert snapshot.physical_schema == "sqlmesh__default"
    assert snapshot.data_version.physical_schema == "sqlmesh__default"
    assert snapshot.table_info.physical_schema == "sqlmesh__default"

    snapshot.physical_schema_ = "custom_schema"
    assert snapshot.physical_schema == "custom_schema"
    assert snapshot.data_version.physical_schema == "custom_schema"
    assert snapshot.table_info.physical_schema == "custom_schema"

    # Make sure the previous physical schema is preserved for forward-only snapshots.
    new_snapshot = deepcopy(snapshot)
    new_snapshot.previous_versions = (snapshot.data_version,)
    new_snapshot.physical_schema_ = None
    new_snapshot.version = None
    new_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)

    assert new_snapshot.physical_schema == "custom_schema"
    assert new_snapshot.data_version.physical_schema == "custom_schema"
    assert new_snapshot.table_info.physical_schema == "custom_schema"


def test_has_paused_forward_only(snapshot: Snapshot):
    assert not has_paused_forward_only([snapshot], [snapshot])

    snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    assert has_paused_forward_only([snapshot], [snapshot])

    snapshot.set_unpaused_ts("2023-01-01")
    assert not has_paused_forward_only([snapshot], [snapshot])


def test_inclusive_exclusive_monthly(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column=TimeColumn(column="ds"), batch_size=1),
            owner="owner",
            dialect="",
            cron="@monthly",
            start="1 week ago",
            query=parse_one("SELECT id, @end_ds as ds FROM name"),
        )
    )

    assert snapshot.inclusive_exclusive("2023-01-01", "2023-07-01") == (
        to_timestamp("2023-01-01"),
        to_timestamp("2023-07-01"),
    )

    assert snapshot.inclusive_exclusive("2023-01-01", "2023-07-06") == (
        to_timestamp("2023-01-01"),
        to_timestamp("2023-07-01"),
    )

    assert snapshot.inclusive_exclusive("2023-01-01", "2023-07-31") == (
        to_timestamp("2023-01-01"),
        to_timestamp("2023-08-01"),
    )


def test_inclusive_exclusive_hourly(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column=TimeColumn(column="ds"), batch_size=1),
            owner="owner",
            dialect="",
            cron="@hourly",
            start="2023-01-29",
            query=parse_one("SELECT id, @end_ds as ds FROM name"),
        )
    )

    target_date = "2023-01-29"
    target_dt = to_datetime(target_date)

    assert snapshot.missing_intervals(target_date, target_date) == [
        (
            to_timestamp(target_dt + timedelta(hours=h)),
            to_timestamp(target_dt + timedelta(hours=h + 1)),
        )
        for h in range(24)
    ]


def test_model_custom_cron(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column="ds"),
            cron="0 5 * * *",
            start="2023-01-01",
            query=parse_one("SELECT ds FROM parent.tbl"),
        )
    )

    # Run at 5;00AM
    assert snapshot.missing_intervals(
        "2023-01-29", "2023-01-29", execution_time="2023-01-30 05:00:00"
    ) == [
        (to_timestamp("2023-01-29"), to_timestamp("2023-01-30")),
    ]
    assert snapshot.missing_intervals(
        to_timestamp("2023-01-29"), to_timestamp("2023-01-30"), execution_time="2023-01-30 05:00:00"
    ) == [
        (to_timestamp("2023-01-29"), to_timestamp("2023-01-30")),
    ]

    # Run at 5;01AM
    assert snapshot.missing_intervals(
        "2023-01-29", "2023-01-29", execution_time="2023-01-30 05:01:00"
    ) == [
        (to_timestamp("2023-01-29"), to_timestamp("2023-01-30")),
    ]
    assert snapshot.missing_intervals(
        to_timestamp("2023-01-29"), to_timestamp("2023-01-30"), execution_time="2023-01-30 05:01:00"
    ) == [
        (to_timestamp("2023-01-29"), to_timestamp("2023-01-30")),
    ]

    # Run at 4:59AM
    assert (
        snapshot.missing_intervals("2023-01-29", "2023-01-29", execution_time="2023-01-30 04:59:00")
        == []
    )
    assert (
        snapshot.missing_intervals(
            to_timestamp("2023-01-29"),
            to_timestamp("2023-01-30"),
            execution_time="2023-01-30 04:59:00",
        )
        == []
    )

    # Run at 4:59AM and ignore cron
    assert snapshot.missing_intervals(
        "2023-01-29", "2023-01-29", execution_time="2023-01-30 04:59:00", ignore_cron=True
    ) == [
        (to_timestamp("2023-01-29"), to_timestamp("2023-01-30")),
    ]
    assert snapshot.missing_intervals(
        to_timestamp("2023-01-29"),
        to_timestamp("2023-01-30"),
        execution_time="2023-01-30 04:59:00",
        ignore_cron=True,
    ) == [
        (to_timestamp("2023-01-29"), to_timestamp("2023-01-30")),
    ]


def test_model_custom_interval_unit(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column="ds"),
            cron="0 5 * * *",
            interval_unit="hour",
            start="2023-01-01",
            query=parse_one("SELECT ds FROM parent.tbl"),
        )
    )

    assert len(snapshot.missing_intervals("2023-01-29", "2023-01-29")) == 24


def test_is_valid_start(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="test",
            query="SELECT 1 FROM test",
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        ),
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    assert snapshot.depends_on_self
    assert snapshot.is_valid_start("2023-01-01", "2023-01-01")
    assert snapshot.is_valid_start("2023-01-01", "2023-01-02")
    assert snapshot.is_valid_start("2023-01-02", "2023-01-01 10:00:00")
    assert not snapshot.is_valid_start("2023-01-02", "2023-01-01")
    snapshot.intervals = [(to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))]
    assert snapshot.is_valid_start("2023-01-01", "2023-01-01")
    assert snapshot.is_valid_start("2023-01-02", "2023-01-01")
    assert not snapshot.is_valid_start("2023-01-03", "2023-01-01")
    snapshot.intervals = []
    assert snapshot.is_valid_start("2023-01-01", "2023-01-01")
    assert snapshot.is_valid_start("2023-01-01", "2023-01-02")
    assert not snapshot.is_valid_start("2023-01-02", "2023-01-01")


@pytest.mark.parametrize(
    "qualified_view_name, environment_naming_info, expected",
    (
        (
            QualifiedViewName(catalog="a-b", schema_name="c", table="d"),
            EnvironmentNamingInfo(),
            '"a-b".c.d',
        ),
        (
            QualifiedViewName(catalog="a-b", schema_name="c-d", table="e"),
            EnvironmentNamingInfo(name="dev"),
            '"a-b"."c-d__dev".e',
        ),
        (
            QualifiedViewName(catalog="a-b", schema_name="c-d", table="e-f"),
            EnvironmentNamingInfo(name="dev", suffix_target=EnvironmentSuffixTarget.TABLE),
            '"a-b"."c-d"."e-f__dev"',
        ),
        (
            QualifiedViewName(catalog="a-b", schema_name="c-d", table="e-f"),
            EnvironmentNamingInfo(name="dev", suffix_target=EnvironmentSuffixTarget.SCHEMA),
            '"a-b"."c-d__dev"."e-f"',
        ),
        (
            QualifiedViewName(catalog="a-b", schema_name="c-d", table="e-f"),
            EnvironmentNamingInfo(name="dev", catalog_name_override="g-h"),
            '"g-h"."c-d__dev"."e-f"',
        ),
        (
            QualifiedViewName(schema_name="c-d", table="e-f"),
            EnvironmentNamingInfo(name="dev", catalog_name_override="g-h"),
            '"g-h"."c-d__dev"."e-f"',
        ),
        (
            QualifiedViewName(table="e-f"),
            EnvironmentNamingInfo(name="dev", catalog_name_override="g-h"),
            '"g-h".default__dev."e-f"',
        ),
        (QualifiedViewName(table="e-f"), EnvironmentNamingInfo(name="dev"), 'default__dev."e-f"'),
    ),
)
def test_qualified_view_name(qualified_view_name, environment_naming_info, expected):
    assert qualified_view_name.for_environment(environment_naming_info) == expected


def test_multi_interval_merge(make_snapshot):
    a = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column="ds"),
            cron="@daily",
            interval_unit="five_minute",
            start="2023-01-01",
            query=parse_one("SELECT ds FROM parent.tbl"),
        )
    )

    a.add_interval("2023-01-01 00:05:00", "2023-01-01 00:51:00")
    a_start, a_end = a.intervals[0]

    assert a_start == to_timestamp("2023-01-01 00:05:00")
    assert a_end == to_timestamp("2023-01-01 00:50:00")

    b = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column="ds"),
            cron="@daily",
            interval_unit="quarter_hour",
            start="2023-01-01",
            query=parse_one("SELECT ds FROM parent.tbl"),
        )
    )

    b.merge_intervals(a)
    b_start, b_end = b.intervals[0]

    assert b_start == to_timestamp("2023-01-01 00:15:00")
    assert b_end == to_timestamp("2023-01-01 00:45:00")


def test_earliest_start_date(sushi_context: Context):
    model_name = "sushi.waiter_names"
    fqn_name = '"memory"."sushi"."waiter_names"'
    assert sushi_context.get_snapshot(model_name, raise_if_missing=True).node.start is None

    cache: t.Dict[str, datetime] = {}
    earliest_start_date(sushi_context.snapshots.values(), cache)
    assert cache[fqn_name] == to_datetime("yesterday")


def test_deployability_index(make_snapshot):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("SELECT 1")))
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("SELECT 1")))
    snapshot_b.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    snapshot_b.parents = (snapshot_a.snapshot_id,)

    snapshot_c = make_snapshot(SqlModel(name="c", query=parse_one("SELECT 1")))
    snapshot_c.categorize_as(SnapshotChangeCategory.INDIRECT_BREAKING)
    snapshot_c.parents = (snapshot_b.snapshot_id,)

    snapshot_d = make_snapshot(SqlModel(name="d", query=parse_one("SELECT 1")))
    snapshot_d.categorize_as(SnapshotChangeCategory.INDIRECT_BREAKING)
    snapshot_d.parents = (snapshot_b.snapshot_id, snapshot_a.snapshot_id)

    snapshot_e = make_snapshot(SqlModel(name="e", query=parse_one("SELECT 1")))
    snapshot_e.categorize_as(SnapshotChangeCategory.NON_BREAKING)

    snapshot_f = make_snapshot(SqlModel(name="f", query=parse_one("SELECT 1")))
    snapshot_f.categorize_as(SnapshotChangeCategory.INDIRECT_BREAKING)
    snapshot_f.parents = (snapshot_e.snapshot_id, snapshot_a.snapshot_id)

    snapshot_g = make_snapshot(SqlModel(name="g", query=parse_one("SELECT 1")))
    snapshot_g.categorize_as(SnapshotChangeCategory.INDIRECT_NON_BREAKING)
    snapshot_g.parents = (snapshot_e.snapshot_id,)

    snapshots = {
        s.snapshot_id: s
        for s in [
            snapshot_a,
            snapshot_b,
            snapshot_c,
            snapshot_d,
            snapshot_e,
            snapshot_f,
            snapshot_g,
        ]
    }

    deployability_index = DeployabilityIndex.create(snapshots)

    assert deployability_index.is_deployable(snapshot_a)
    assert deployability_index.is_deployable(snapshot_e)
    assert deployability_index.is_deployable(snapshot_f)
    assert not deployability_index.is_deployable(snapshot_g)
    assert not deployability_index.is_deployable(snapshot_b)
    assert not deployability_index.is_deployable(snapshot_c)
    assert not deployability_index.is_deployable(snapshot_d)

    assert deployability_index.is_representative(snapshot_a)
    assert deployability_index.is_representative(snapshot_e)
    assert deployability_index.is_representative(snapshot_f)
    assert deployability_index.is_representative(snapshot_g)
    assert not deployability_index.is_representative(snapshot_b)
    assert not deployability_index.is_representative(snapshot_c)
    assert not deployability_index.is_representative(snapshot_d)

    all_deployable_index = deployability_index.all_deployable()
    assert all(all_deployable_index.is_deployable(s) for s in snapshots.values())
    assert all(all_deployable_index.is_representative(s) for s in snapshots.values())

    none_deployable_index = deployability_index.none_deployable()
    assert all(not none_deployable_index.is_deployable(s) for s in snapshots.values())
    assert all(not none_deployable_index.is_representative(s) for s in snapshots.values())


def test_deployability_index_unpaused_forward_only(make_snapshot):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("SELECT 1")))
    snapshot_a.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    snapshot_a.unpaused_ts = 1

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("SELECT 1")))
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot_b.parents = (snapshot_a.snapshot_id,)

    deplyability_index = DeployabilityIndex.create(
        {s.snapshot_id: s for s in [snapshot_a, snapshot_b]}
    )

    assert not deplyability_index.is_deployable(snapshot_a)
    assert deplyability_index.is_deployable(snapshot_b)

    assert deplyability_index.is_representative(snapshot_a)
    assert deplyability_index.is_representative(snapshot_b)


def test_deployability_index_uncategorized_forward_only_model(make_snapshot):
    model_a = SqlModel(
        name="a",
        query=parse_one("SELECT 1, ds"),
        kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
    )

    snapshot_a_old = make_snapshot(model_a)
    snapshot_a_old.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_a = make_snapshot(model_a)
    snapshot_a.previous_versions = snapshot_a_old.all_versions

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("SELECT 1")))
    snapshot_b.parents = (snapshot_a.snapshot_id,)

    deployability_index = DeployabilityIndex.create(
        {s.snapshot_id: s for s in [snapshot_a, snapshot_b]}
    )

    assert not deployability_index.is_deployable(snapshot_a)
    assert not deployability_index.is_deployable(snapshot_b)

    assert not deployability_index.is_representative(snapshot_a)
    assert not deployability_index.is_representative(snapshot_b)


def test_deployability_index_categorized_forward_only_model(make_snapshot):
    model_a = SqlModel(
        name="a",
        query=parse_one("SELECT 1, ds"),
        kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
    )

    snapshot_a_old = make_snapshot(model_a)
    snapshot_a_old.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_a = make_snapshot(model_a)
    snapshot_a.previous_versions = snapshot_a_old.all_versions
    snapshot_a.categorize_as(SnapshotChangeCategory.METADATA)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("SELECT 1")))
    snapshot_b.parents = (snapshot_a.snapshot_id,)
    snapshot_b.categorize_as(SnapshotChangeCategory.METADATA)

    # The fact that the model is forward only should be ignored if an actual category
    # has been assigned.
    deployability_index = DeployabilityIndex.create(
        {s.snapshot_id: s for s in [snapshot_a, snapshot_b]}
    )

    assert deployability_index.is_deployable(snapshot_a)
    assert deployability_index.is_deployable(snapshot_b)

    assert deployability_index.is_representative(snapshot_a)
    assert deployability_index.is_representative(snapshot_b)


def test_deployability_index_missing_parent(make_snapshot):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("SELECT 1")))
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("SELECT 1")))
    snapshot_b.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    snapshot_b.parents = (snapshot_a.snapshot_id,)

    deplyability_index = DeployabilityIndex.create({snapshot_b.snapshot_id: snapshot_b})

    assert not deplyability_index.is_deployable(snapshot_b)
    assert not deplyability_index.is_deployable(snapshot_a)


@pytest.mark.parametrize(
    "model_name, environment_naming_info, default_catalog, expected",
    (
        (
            "test_db.test_model",
            EnvironmentNamingInfo(),
            None,
            "test_db.test_model",
        ),
        (
            "test_db.test_model",
            EnvironmentNamingInfo(name="dev"),
            None,
            "test_db__dev.test_model",
        ),
        (
            "test_db.test_model",
            EnvironmentNamingInfo(name="dev", suffix_target=EnvironmentSuffixTarget.SCHEMA),
            None,
            "test_db__dev.test_model",
        ),
        (
            "test_db.test_model",
            EnvironmentNamingInfo(name="dev", suffix_target=EnvironmentSuffixTarget.TABLE),
            None,
            "test_db.test_model__dev",
        ),
        (
            "test_db.test_model",
            EnvironmentNamingInfo(
                name="dev",
                suffix_target=EnvironmentSuffixTarget.TABLE,
                catalog_name_override="catalog_override",
            ),
            None,
            "catalog_override.test_db.test_model__dev",
        ),
        (
            "original_catalog.test_db.test_model",
            EnvironmentNamingInfo(name="dev", suffix_target=EnvironmentSuffixTarget.TABLE),
            "default_catalog",
            "original_catalog.test_db.test_model__dev",
        ),
        (
            "original_catalog.test_db.test_model",
            EnvironmentNamingInfo(
                name="dev",
                suffix_target=EnvironmentSuffixTarget.TABLE,
                catalog_name_override="catalog_override",
            ),
            "default_catalog",
            "catalog_override.test_db.test_model__dev",
        ),
        (
            "test_db.test_model",
            EnvironmentNamingInfo(
                name="dev",
                suffix_target=EnvironmentSuffixTarget.TABLE,
                catalog_name_override="catalog_override",
            ),
            "default_catalog",
            "catalog_override.test_db.test_model__dev",
        ),
        (
            "test_db.test_model",
            EnvironmentNamingInfo(name="dev", suffix_target=EnvironmentSuffixTarget.TABLE),
            "default_catalog",
            "test_db.test_model__dev",
        ),
    ),
)
def test_display_name(
    make_snapshot, model_name, environment_naming_info, default_catalog, expected
):
    input_model = SqlModel(
        name=model_name,
        query=parse_one("SELECT 1, ds"),
        kind=IncrementalByTimeRangeKind(time_column="ds", forward_only=True),
        default_catalog=default_catalog,
    )
    input_snapshot = make_snapshot(input_model)
    assert display_name(input_snapshot, environment_naming_info, default_catalog) == expected


def test_missing_intervals_node_start_end(make_snapshot):
    input_model = SqlModel(
        name="test_model",
        query=parse_one("SELECT 1, ds"),
        kind=FullKind(),
        start="2024-03-12",
        end="2024-03-12",
    )
    snapshot = make_snapshot(input_model)
    assert missing_intervals([snapshot])[snapshot] == [
        (to_timestamp("2024-03-12"), to_timestamp("2024-03-13"))
    ]
    assert missing_intervals([snapshot], start="2024-03-12")[snapshot] == [
        (to_timestamp("2024-03-12"), to_timestamp("2024-03-13"))
    ]
    assert missing_intervals([snapshot], start="2024-03-12", end="2024-03-12")[snapshot] == [
        (to_timestamp("2024-03-12"), to_timestamp("2024-03-13"))
    ]
    assert missing_intervals([snapshot], start="2024-03-12", end="2024-03-14")[snapshot] == [
        (to_timestamp("2024-03-12"), to_timestamp("2024-03-13"))
    ]
    assert missing_intervals([snapshot], start="2024-03-01", end="2024-03-30")[snapshot] == [
        (to_timestamp("2024-03-12"), to_timestamp("2024-03-13"))
    ]
    assert missing_intervals([snapshot], start="2024-03-11", end=to_datetime("2024-03-12")) == {}
    assert missing_intervals([snapshot], start="2024-03-12", end=to_datetime("2024-03-12")) == {}
    assert missing_intervals([snapshot], start="2024-03-01", end=to_datetime("2024-03-12")) == {}
    assert missing_intervals([snapshot], start="2024-03-01", end=to_datetime("2024-03-10")) == {}
    assert missing_intervals([snapshot], start="2024-03-13", end="2024-03-14") == {}
    assert missing_intervals([snapshot], start="2024-03-14", end="2024-03-30") == {}


def test_external_model_audits(sushi_context):
    snapshot = sushi_context.get_snapshot("raw.demographics")
    assert snapshot.evaluatable
    assert len(snapshot.model.audits) == 2
    assert snapshot.intervals


def test_custom_model_kind(make_snapshot):
    from sqlmesh import CustomMaterialization

    class MyCustomStrategy(CustomMaterialization):
        pass

    snapshot = make_snapshot(
        SqlModel(
            name="test_model_name",
            kind=dict(
                name="CUSTOM",
                materialization="MyCustomStrategy",
                materialization_properties=parse_one("('test_key' = 'test_value')"),
            ),
            owner="owner",
            dialect="",
            cron="@hourly",
            start="2023-01-29",
            query=parse_one("SELECT id, @end_ds as ds FROM name"),
        )
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    assert snapshot.custom_materialization == "MyCustomStrategy"

    table_info = snapshot.table_info
    assert table_info.custom_materialization == "MyCustomStrategy"

    parsed_table_info = SnapshotTableInfo.parse_raw(table_info.json())
    assert parsed_table_info.custom_materialization == "MyCustomStrategy"


def test_ttl_ms(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="test_model_name",
            query=parse_one("SELECT 1"),
        ),
        ttl="in 1 week",
    )
    assert snapshot.ttl_ms == 604800000


def test_snapshot_cache(make_snapshot, tmp_path):
    cache_path = tmp_path / "snapshot_cache"
    cache = SnapshotCache(cache_path)

    snapshot = make_snapshot(
        SqlModel(
            name="test_model_name",
            query=parse_one("SELECT 1"),
        )
    )
    snapshot.add_interval("2024-01-01", "2024-01-02")
    snapshot.add_interval("2024-01-01", "2024-01-02", is_dev=True)

    loader_called_times = 0

    def _loader(snapshot_ids: t.Set[SnapshotId]) -> t.Collection[Snapshot]:
        nonlocal loader_called_times
        loader_called_times += 1
        return [snapshot]

    assert cache.get_or_load([snapshot.snapshot_id], _loader) == (
        {snapshot.snapshot_id: snapshot},
        set(),
    )
    assert cache.get_or_load([snapshot.snapshot_id], _loader) == (
        {snapshot.snapshot_id: snapshot},
        {snapshot.snapshot_id},
    )
    assert cache.get_or_load([snapshot.snapshot_id], _loader) == (
        {snapshot.snapshot_id: snapshot},
        {snapshot.snapshot_id},
    )
    assert loader_called_times == 1

    cached_snapshot = cache.get_or_load([snapshot.snapshot_id], _loader)[0][snapshot.snapshot_id]
    assert cached_snapshot.model._query_renderer._optimized_cache is not None
    assert cached_snapshot.model._data_hash is not None
    assert cached_snapshot.model._metadata_hash is not None
    assert not cached_snapshot.intervals
    assert not cached_snapshot.dev_intervals

    cache.clear()
    assert cache.get_or_load([snapshot.snapshot_id], _loader) == (
        {snapshot.snapshot_id: snapshot},
        set(),
    )
    assert loader_called_times == 2


def test_snapshot_pickle_intervals(make_snapshot):
    snapshot = make_snapshot(
        SqlModel(
            name="test_model_name",
            query=parse_one("SELECT 1"),
        )
    )
    snapshot.add_interval("2023-01-01", "2023-01-02")
    snapshot.add_interval("2023-01-01", "2023-01-02", is_dev=True)

    assert len(snapshot.intervals) > 0
    assert len(snapshot.dev_intervals) > 0

    loaded_snapshot = pickle.loads(pickle.dumps(snapshot))
    assert not loaded_snapshot.intervals
    assert not loaded_snapshot.dev_intervals
    assert len(snapshot.intervals) > 0
    assert len(snapshot.dev_intervals) > 0


def test_missing_intervals_interval_end_per_model(make_snapshot):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            start="2023-01-04",
            query=parse_one("SELECT 1"),
        ),
        version="a",
    )

    snapshot_b = make_snapshot(
        SqlModel(
            name="b",
            start="2023-01-04",
            query=parse_one("SELECT 2"),
        ),
        version="b",
    )

    assert missing_intervals(
        [snapshot_a, snapshot_b],
        start="2023-01-04",
        end="2023-01-10",
        interval_end_per_model={
            snapshot_a.name: to_timestamp("2023-01-09"),
            snapshot_b.name: to_timestamp("2023-01-06"),
        },
    ) == {
        snapshot_a: [
            (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
            (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
            (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
            (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            (to_timestamp("2023-01-08"), to_timestamp("2023-01-09")),
        ],
        snapshot_b: [
            (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
            (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
        ],
    }

    assert missing_intervals(
        [snapshot_a, snapshot_b],
        start="2023-01-08",
        end="2023-01-08",
        interval_end_per_model={
            snapshot_a.name: to_timestamp("2023-01-09"),
            snapshot_b.name: to_timestamp(
                "2023-01-06"
            ),  # The interval end is before the start. This should be ignored.
        },
    ) == {
        snapshot_a: [(to_timestamp("2023-01-08"), to_timestamp("2023-01-09"))],
        snapshot_b: [(to_timestamp("2023-01-08"), to_timestamp("2023-01-09"))],
    }
