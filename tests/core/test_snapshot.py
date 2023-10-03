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
from sqlmesh.core.config import AutoCategorizationMode, CategorizerConfig
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse, parse_one
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    Model,
    Seed,
    SeedKind,
    SeedModel,
    SqlModel,
    create_seed_model,
    load_sql_based_model,
)
from sqlmesh.core.model.kind import TimeColumn
from sqlmesh.core.snapshot import (
    QualifiedViewName,
    Snapshot,
    SnapshotChangeCategory,
    SnapshotFingerprint,
    categorize_change,
    earliest_start_date,
    fingerprint_from_node,
    has_paused_forward_only,
)
from sqlmesh.utils.date import to_date, to_datetime, to_timestamp
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroInfo


@pytest.fixture
def parent_model():
    return SqlModel(
        name="parent.tbl",
        kind=IncrementalByTimeRangeKind(time_column="ds"),
        dialect="spark",
        query=parse_one("SELECT 1, ds"),
    )


@pytest.fixture
def model():
    return SqlModel(
        name="name",
        kind=IncrementalByTimeRangeKind(time_column="ds", batch_size=30),
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
        nodes={parent_model.name: parent_model, model.name: model},
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
                "time_column": {"column": "ds"},
                "batch_size": 30,
                "forward_only": False,
                "disable_restatement": False,
            },
            "mapping_schema": {},
            "start": "2020-01-01",
            "dialect": "spark",
            "name": "name",
            "partitioned_by": [],
            "project": "",
            "owner": "owner",
            "query": "SELECT @EACH([1, 2], x -> x), ds FROM parent.tbl",
            "jinja_macros": {
                "global_objs": {},
                "packages": {},
                "root_macros": {},
                "top_level_packages": [],
            },
            "source_type": "sql",
            "tags": [],
            "grains": [],
            "references": [],
            "hash_raw_query": False,
            "allow_partials": False,
        },
        "audits": [],
        "name": "name",
        "parents": [{"name": "parent.tbl", "identifier": snapshot.parents[0].identifier}],
        "previous_versions": [],
        "indirect_versions": {},
        "updated_ts": 1663891973000,
        "version": snapshot.fingerprint.to_version(),
        "migrated": False,
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


def test_add_interval_dev(snapshot: Snapshot):
    snapshot.version = "existing_version"
    snapshot.change_category = SnapshotChangeCategory.FORWARD_ONLY

    snapshot.add_interval("2020-01-01", "2020-01-01")
    assert snapshot.intervals == [(to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))]

    snapshot.add_interval("2020-01-02", "2020-01-02", is_dev=True)
    assert snapshot.intervals == [(to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))]
    assert snapshot.dev_intervals == [(to_timestamp("2020-01-02"), to_timestamp("2020-01-03"))]


def test_add_interval_partial(snapshot: Snapshot):
    snapshot.add_interval("2023-01-01 00:00:00", "2023-01-01 23:59:59")
    assert snapshot.intervals == []

    snapshot.add_interval("2023-01-01 00:00:00", "2023-01-01 14:00:00")
    assert snapshot.intervals == []

    snapshot.add_interval("2023-01-01 15:00:00", "2023-01-03 00:00:00")
    assert snapshot.intervals == [
        (to_timestamp("2023-01-02"), to_timestamp("2023-01-03")),
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
        (to_timestamp(start), to_timestamp("2023-01-02")),
    ]
    assert snapshot.missing_intervals(start, end_ts, execution_time=end_ts) == [
        (to_timestamp(start), end_ts),
    ]
    assert snapshot.missing_intervals(start, start) == [
        (to_timestamp(start), to_timestamp("2023-01-02")),
    ]
    assert snapshot.missing_intervals(start, start, execution_time=start, ignore_cron=True) == []


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
    assert snapshot.missing_intervals("2023-01-01", "2023-01-02") == []

    snapshot.add_interval("2023-01-06", "2023-01-07")
    assert snapshot.missing_intervals("2023-01-03", "2023-01-03") == [
        (to_timestamp("2023-01-03"), to_timestamp("2023-01-04")),
    ]
    assert snapshot.missing_intervals("2023-01-04", "2023-01-04") == [
        (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
    ]
    assert snapshot.missing_intervals("2023-01-05", "2023-01-05") == [
        (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
    ]
    assert snapshot.missing_intervals("2023-01-03", "2023-01-05") == [
        (to_timestamp("2023-01-03"), to_timestamp("2023-01-04")),
        (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
        (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
    ]
    snapshot.add_interval("2023-01-05", "2023-01-05")
    assert snapshot.missing_intervals("2023-01-03", "2023-01-06") == [
        (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
    ]

    assert snapshot.missing_intervals("2023-01-29", "2023-01-29") == [
        (to_timestamp("2023-01-29"), to_timestamp("2023-01-30")),
    ]

    snapshot.add_interval("2023-01-28", "2023-01-29")
    assert snapshot.missing_intervals("2023-01-27", "2023-01-27", "2023-01-30 05:00:00") == [
        (to_timestamp("2023-01-27"), to_timestamp("2023-01-28")),
    ]
    assert snapshot.missing_intervals("2023-01-28", "2023-01-28", "2023-01-30 05:00:00") == [
        (to_timestamp("2023-01-28"), to_timestamp("2023-01-29")),
    ]
    assert snapshot.missing_intervals("2023-01-29", "2023-01-29", "2023-01-30 05:00:00") == [
        (to_timestamp("2023-01-29"), to_timestamp("2023-01-30")),
    ]
    assert snapshot.missing_intervals("2023-01-27", "2023-01-29", "2023-01-30 05:00:00") == [
        (to_timestamp("2023-01-27"), to_timestamp("2023-01-28")),
        (to_timestamp("2023-01-28"), to_timestamp("2023-01-29")),
        (to_timestamp("2023-01-29"), to_timestamp("2023-01-30")),
    ]

    snapshot.add_interval("2023-01-28", "2023-01-30")
    assert snapshot.missing_intervals("2023-01-27", "2023-01-27", "2023-01-30 05:00:00") == [
        (to_timestamp("2023-01-27"), to_timestamp("2023-01-28")),
    ]
    assert snapshot.missing_intervals("2023-01-28", "2023-01-28", "2023-01-30 05:00:00") == []
    assert snapshot.missing_intervals("2023-01-29", "2023-01-29", "2023-01-30 05:00:00") == []
    assert snapshot.missing_intervals("2023-01-27", "2023-01-29", "2023-01-30 05:00:00") == [
        (to_timestamp("2023-01-27"), to_timestamp("2023-01-28")),
    ]

    assert snapshot.missing_intervals("2023-01-30", "2023-01-30", "2023-01-30") == []


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


each_macro = lambda: "test"


def test_fingerprint(model: Model, parent_model: Model):
    original_model = deepcopy(model)
    fingerprint = fingerprint_from_node(model, nodes={})

    original_fingerprint = SnapshotFingerprint(
        data_hash="3811098861",
        metadata_hash="3858405978",
    )

    assert fingerprint == original_fingerprint

    with_parent_fingerprint = fingerprint_from_node(model, nodes={"parent.tbl": parent_model})
    assert with_parent_fingerprint != fingerprint
    assert int(with_parent_fingerprint.parent_data_hash) > 0
    assert int(with_parent_fingerprint.parent_metadata_hash) > 0

    assert (
        fingerprint_from_node(
            model,
            nodes={"parent.tbl": SqlModel(**{**model.dict(), "query": parse_one("select 2, ds")})},
        )
        != with_parent_fingerprint
    )
    model = SqlModel(**{**model.dict(), "query": parse_one("select 1, ds")})
    new_fingerprint = fingerprint_from_node(model, nodes={})
    assert new_fingerprint != fingerprint

    model = SqlModel(**{**model.dict(), "query": parse_one("select 1, ds -- annotation")})
    assert new_fingerprint != fingerprint_from_node(model, nodes={})

    model = SqlModel(
        **{**original_model.dict(), "pre_statements": [parse_one("CREATE TABLE test")]}
    )
    assert original_fingerprint != fingerprint_from_node(model, nodes={})

    model = SqlModel(**{**original_model.dict(), "post_statements": [parse_one("DROP TABLE test")]})
    assert original_fingerprint != fingerprint_from_node(model, nodes={})


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
        data_hash="3270932819",
        metadata_hash="1017437962",
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
        data_hash="2864998504",
        metadata_hash="3858405978",
    )

    fingerprint = fingerprint_from_node(model, nodes={})
    assert fingerprint == original_fingerprint

    model.jinja_macros.root_macros["test_macro"] = MacroInfo(
        definition="{% macro test_macro() %}b{% endmacro %}", depends_on=[]
    )
    updated_fingerprint = fingerprint_from_node(model, nodes={})
    assert updated_fingerprint.data_hash != original_fingerprint.data_hash
    assert updated_fingerprint.metadata_hash == original_fingerprint.metadata_hash


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
    assert snapshot.table_name(is_dev=False, for_read=False) == "sqlmesh__default.name__3078928823"
    assert snapshot.table_name(is_dev=True, for_read=False) == "sqlmesh__default.name__3078928823"
    assert snapshot.table_name(is_dev=False, for_read=True) == "sqlmesh__default.name__3078928823"
    assert snapshot.table_name(is_dev=True, for_read=True) == "sqlmesh__default.name__3078928823"
    assert snapshot.table_name_for_mapping(is_dev=False) == "sqlmesh__default.name__3078928823"
    assert snapshot.table_name_for_mapping(is_dev=True) == "sqlmesh__default.name__3078928823"

    # Mimic an indirect forward-only change.
    previous_data_version = snapshot.data_version
    assert previous_data_version.physical_schema == "sqlmesh__default"
    snapshot.fingerprint = SnapshotFingerprint(
        data_hash="1", metadata_hash="1", parent_data_hash="2"
    )
    snapshot.previous_versions = (previous_data_version,)
    snapshot.categorize_as(SnapshotChangeCategory.INDIRECT_NON_BREAKING)
    assert snapshot.table_name(is_dev=False, for_read=False) == "sqlmesh__default.name__3078928823"
    assert (
        snapshot.table_name(is_dev=True, for_read=False) == "sqlmesh__default.name__781051917__temp"
    )
    assert snapshot.table_name(is_dev=False, for_read=True) == "sqlmesh__default.name__3078928823"
    assert snapshot.table_name(is_dev=True, for_read=True) == "sqlmesh__default.name__3078928823"
    assert snapshot.table_name_for_mapping(is_dev=False) == "sqlmesh__default.name__3078928823"
    assert snapshot.table_name_for_mapping(is_dev=True) == "sqlmesh__default.name__3078928823"

    # Mimic a direct forward-only change.
    snapshot.fingerprint = SnapshotFingerprint(
        data_hash="2", metadata_hash="1", parent_data_hash="1"
    )
    snapshot.previous_versions = (previous_data_version,)
    snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    assert snapshot.table_name(is_dev=False, for_read=False) == "sqlmesh__default.name__3078928823"
    assert (
        snapshot.table_name(is_dev=True, for_read=False)
        == "sqlmesh__default.name__3049392110__temp"
    )
    assert snapshot.table_name(is_dev=False, for_read=True) == "sqlmesh__default.name__3078928823"
    assert (
        snapshot.table_name(is_dev=True, for_read=True) == "sqlmesh__default.name__3049392110__temp"
    )
    assert snapshot.table_name_for_mapping(is_dev=False) == "sqlmesh__default.name__3078928823"
    assert snapshot.table_name_for_mapping(is_dev=True) == "sqlmesh__default.name__3049392110__temp"

    # Mimic a propmoted forward-only snapshot.
    snapshot.set_unpaused_ts(to_datetime("2022-01-01"))
    assert snapshot.table_name(is_dev=False, for_read=False) == "sqlmesh__default.name__3078928823"
    assert (
        snapshot.table_name(is_dev=True, for_read=False)
        == "sqlmesh__default.name__3049392110__temp"
    )
    assert snapshot.table_name(is_dev=False, for_read=True) == "sqlmesh__default.name__3078928823"
    assert (
        snapshot.table_name(is_dev=True, for_read=True) == "sqlmesh__default.name__3049392110__temp"
    )
    assert snapshot.table_name_for_mapping(is_dev=False) == "sqlmesh__default.name__3078928823"
    assert snapshot.table_name_for_mapping(is_dev=True) == "sqlmesh__default.name__3078928823"
    snapshot.physical_schema_ = "private"
    assert snapshot.table_name_for_mapping(is_dev=True) == "private.name__3078928823"

    fully_qualified_snapshot = make_snapshot(
        SqlModel(name='"my-catalog".db.table', query=parse_one("select 1, ds"))
    )
    fully_qualified_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    assert (
        fully_qualified_snapshot.table_name(is_dev=False, for_read=False)
        == f'"my-catalog".sqlmesh__db.my_catalog__db__table__{fully_qualified_snapshot.version}'
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


def test_categorize_change_seed(make_snapshot, tmp_path):
    config = CategorizerConfig(seed=AutoCategorizationMode.SEMI)
    model_name = "test_db.test_seed_model"
    seed_path = tmp_path / "seed.csv"
    model_kind = SeedKind(path=str(seed_path.absolute()))

    with open(seed_path, "w") as fd:
        fd.write(
            """
col_a,col_b,col_c
1,text_a,1.0
2,text_b,2.0"""
        )

    original_snapshot = make_snapshot(create_seed_model(model_name, model_kind))

    # New column.
    with open(seed_path, "w") as fd:
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
    with open(seed_path, "w") as fd:
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
    with open(seed_path, "w") as fd:
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
    with open(seed_path, "w") as fd:
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
    with open(seed_path, "w") as fd:
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
    with open(seed_path, "w") as fd:
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
    with open(seed_path, "w") as fd:
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
    with open(seed_path, "w") as fd:
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
            start="1 week ago",
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
    assert snapshot.depends_on_past
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


def test_qualified_view_name():
    env = EnvironmentNamingInfo()
    assert (
        QualifiedViewName(catalog="a-b", schema_name="c", table="d").for_environment(env)
        == '"a-b".c.d'
    )


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
    assert sushi_context.snapshots[model_name].node.start is None

    cache: t.Dict[str, datetime] = {}
    earliest_start_date(sushi_context.snapshots.values(), cache)

    # Make sure that the default value for a snapshot with a missing start is not cached.
    assert model_name not in cache
