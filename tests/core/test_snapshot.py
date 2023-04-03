import json
from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse, parse_one, to_column

from sqlmesh.core.config import AutoCategorizationMode, CategorizerConfig
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    Model,
    ModelKind,
    ModelKindName,
    SeedKind,
    SqlModel,
    create_seed_model,
    load_model,
)
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotFingerprint,
    categorize_change,
    fingerprint_from_model,
)
from sqlmesh.utils.date import to_datetime, to_timestamp
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
        kind=IncrementalByTimeRangeKind(time_column="ds"),
        owner="owner",
        dialect="spark",
        cron="1 0 * * *",
        batch_size=30,
        start="2020-01-01",
        query=parse_one("SELECT @EACH([1, 2], x -> x), ds FROM parent.tbl"),
    )


@pytest.fixture
def full_refresh_model():
    return SqlModel(
        name="fr_model",
        kind=ModelKind(name=ModelKindName.FULL),
        owner="owner",
        dialect="spark",
        cron="1 0 * * *",
        batch_size=30,
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
        models={parent_model.name: parent_model, model.name: model},
    )
    snapshot.version = snapshot.fingerprint
    return snapshot


@pytest.fixture
def full_refresh_snapshot(
    full_refresh_model: Model,
    parent_model: Model,
    monkeypatch: MonkeyPatch,
    mocker: MockerFixture,
    make_snapshot,
):
    mock = mocker.Mock()
    mock.return_value = to_datetime("2022-09-23T00:12:53+00:00")
    monkeypatch.setattr("sqlmesh.utils.date.now", mock)
    full_refresh_snapshot = make_snapshot(
        full_refresh_model,
        models={parent_model.name: parent_model, full_refresh_model.name: full_refresh_model},
    )
    full_refresh_snapshot.version = full_refresh_snapshot.fingerprint
    return full_refresh_snapshot


def test_json(snapshot: Snapshot):
    assert json.loads(snapshot.json()) == {
        "created_ts": 1663891973000,
        "ttl": "in 1 week",
        "fingerprint": snapshot.fingerprint,
        "physical_schema": "sqlmesh",
        "intervals": [],
        "dev_intervals": [],
        "model": {
            "audits": [],
            "cron": "1 0 * * *",
            "batch_size": 30,
            "kind": {
                "name": "INCREMENTAL_BY_TIME_RANGE",
                "time_column": {"column": "ds"},
            },
            "start": "2020-01-01",
            "dialect": "spark",
            "name": "name",
            "partitioned_by": [],
            "post": [],
            "pre": [],
            "owner": "owner",
            "query": "SELECT @EACH(ARRAY(1, 2), x -> x), ds FROM parent.tbl",
            "jinja_macros": {
                "global_objs": {},
                "packages": {},
                "root_macros": {},
            },
            "source_type": "sql",
        },
        "audits": [],
        "name": "name",
        "parents": [{"name": "parent.tbl", "identifier": snapshot.parents[0].identifier}],
        "previous_versions": [],
        "indirect_versions": {},
        "updated_ts": 1663891973000,
        "version": snapshot.fingerprint.dict(),
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
        (to_timestamp("2018-12-31"), to_timestamp("2020-01-31")),
    ]

    new_snapshot = make_snapshot(snapshot.model)
    new_snapshot.add_interval("2020-01-29", "2020-02-01")
    new_snapshot.add_interval("2020-02-05", "2020-02-10")
    new_snapshot.merge_intervals(snapshot)
    assert new_snapshot.intervals == [
        (to_timestamp("2018-12-31"), to_timestamp("2020-02-02")),
        (to_timestamp("2020-02-05"), to_timestamp("2020-02-11")),
    ]


def test_add_interval_dev(snapshot: Snapshot):
    snapshot.version = "existing_version"

    snapshot.add_interval("2020-01-01", "2020-01-01")
    assert snapshot.intervals == [(to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))]

    snapshot.add_interval("2020-01-02", "2020-01-02", is_dev=True)
    assert snapshot.intervals == [(to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))]
    assert snapshot.dev_intervals == [(to_timestamp("2020-01-02"), to_timestamp("2020-01-03"))]


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


def test_missing_interval_latest(
    full_refresh_snapshot: Snapshot, monkeypatch: MonkeyPatch, mocker: MockerFixture
):
    mock = mocker.Mock()
    mock.return_value = to_datetime("2020-01-05T00:12:53+00:00")
    monkeypatch.setattr("sqlmesh.core.snapshot.definition.now", mock)
    full_refresh_snapshot.add_interval("2020-01-01", "2020-01-01")
    assert full_refresh_snapshot.missing_intervals("2020-01-01", "2020-01-01", "2020-01-01") == []
    assert full_refresh_snapshot.missing_intervals("2020-01-02", "2020-01-02", "2020-01-02") == [
        (to_timestamp("2020-01-02"), to_timestamp("2020-01-03"))
    ]
    assert full_refresh_snapshot.missing_intervals("2020-01-02", "2020-01-03", "2020-01-03") == [
        (to_timestamp("2020-01-03"), to_timestamp("2020-01-04"))
    ]
    assert full_refresh_snapshot.missing_intervals("2020-01-02", "2020-01-03", "2020-01-04") == [
        (to_timestamp("2020-01-04"), to_timestamp("2020-01-05"))
    ]
    assert full_refresh_snapshot.missing_intervals(
        "2020-01-02", "2020-01-03", "2020-01-03 01:00:00"
    ) == [(to_timestamp("2020-01-02"), to_timestamp("2020-01-03"))]
    assert full_refresh_snapshot.missing_intervals(
        "2020-01-02", "2020-01-03", "2020-01-04 23:59:59"
    ) == [(to_timestamp("2020-01-03"), to_timestamp("2020-01-04"))]
    assert full_refresh_snapshot.missing_intervals("2020-01-02", "2020-01-03") == [
        (to_timestamp("2020-01-04"), to_timestamp("2020-01-05"))
    ]


def test_remove_intervals(snapshot: Snapshot):
    snapshot.add_interval("2020-01-01", "2020-01-01")
    snapshot.remove_interval("2020-01-01", "2020-01-01")
    assert snapshot.intervals == []

    snapshot.add_interval("2020-01-01", "2020-01-01")
    snapshot.add_interval("2020-01-03", "2020-01-03")
    snapshot.remove_interval("2020-01-01", "2020-01-01")
    assert snapshot.intervals == [(to_timestamp("2020-01-03"), to_timestamp("2020-01-04"))]

    snapshot.remove_interval("2020-01-01", "2020-01-05")
    assert snapshot.intervals == []

    snapshot.add_interval("2020-01-01", "2020-01-05")
    snapshot.add_interval("2020-01-07", "2020-01-10")
    snapshot.remove_interval("2020-01-03", "2020-01-04")
    assert snapshot.intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-03")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-06")),
        (to_timestamp("2020-01-07"), to_timestamp("2020-01-11")),
    ]
    snapshot.remove_interval("2020-01-01", "2020-01-01")
    assert snapshot.intervals == [
        (to_timestamp("2020-01-02"), to_timestamp("2020-01-03")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-06")),
        (to_timestamp("2020-01-07"), to_timestamp("2020-01-11")),
    ]
    snapshot.remove_interval("2020-01-10", "2020-01-10")
    assert snapshot.intervals == [
        (to_timestamp("2020-01-02"), to_timestamp("2020-01-03")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-06")),
        (to_timestamp("2020-01-07"), to_timestamp("2020-01-10")),
    ]
    snapshot.remove_interval("2020-01-07", "2020-01-07")
    assert snapshot.intervals == [
        (to_timestamp("2020-01-02"), to_timestamp("2020-01-03")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-06")),
        (to_timestamp("2020-01-08"), to_timestamp("2020-01-10")),
    ]
    snapshot.remove_interval("2020-01-06", "2020-01-21")
    assert snapshot.intervals == [
        (to_timestamp("2020-01-02"), to_timestamp("2020-01-03")),
        (to_timestamp("2020-01-05"), to_timestamp("2020-01-06")),
    ]
    snapshot.remove_interval("2019-01-01", "2022-01-01")
    assert snapshot.intervals == []


each_macro = lambda: "test"


def test_fingerprint(model: Model, parent_model: Model):
    fingerprint = fingerprint_from_model(model, models={})

    original_fingerprint = SnapshotFingerprint(
        data_hash="3042895307",
        metadata_hash="3589467163",
    )

    assert fingerprint == original_fingerprint
    assert fingerprint_from_model(model, physical_schema="x", models={}) != fingerprint

    with_parent_fingerprint = fingerprint_from_model(model, models={"parent.tbl": parent_model})
    assert with_parent_fingerprint != fingerprint
    assert int(with_parent_fingerprint.parent_data_hash) > 0
    assert int(with_parent_fingerprint.parent_metadata_hash) > 0

    assert (
        fingerprint_from_model(
            model,
            models={"parent.tbl": SqlModel(**{**model.dict(), "query": parse_one("select 2, ds")})},
        )
        != with_parent_fingerprint
    )

    model = SqlModel(**{**model.dict(), "query": parse_one("select 1, ds")})
    new_fingerprint = fingerprint_from_model(model, models={})
    assert new_fingerprint != fingerprint

    model = SqlModel(**{**model.dict(), "query": parse_one("select 1, ds -- annotation")})
    assert new_fingerprint != fingerprint_from_model(model, models={})


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
        data_hash="2542007450",
        metadata_hash="2750000337",
    )

    model = load_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))
    actual_fingerprint = fingerprint_from_model(model, models={})
    assert actual_fingerprint == expected_fingerprint

    updated_model = model.copy(
        update={
            "columns_to_types_": {
                "id": exp.DataType.build("int"),
                "name": exp.DataType.build("text"),
            }
        }
    )
    updated_actual_fingerprint = fingerprint_from_model(updated_model, models={})
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
    fingerprint = fingerprint_from_model(model, models={})

    original_fingerprint = SnapshotFingerprint(
        data_hash="2665680291",
        metadata_hash="3589467163",
    )

    fingerprint = fingerprint_from_model(model, models={})
    assert fingerprint == original_fingerprint

    model.jinja_macros.root_macros["test_macro"] = MacroInfo(
        definition="{% macro test_macro() %}b{% endmacro %}", depends_on=[]
    )
    updated_fingerprint = fingerprint_from_model(model, models={})
    assert updated_fingerprint.data_hash != original_fingerprint.data_hash
    assert updated_fingerprint.metadata_hash == original_fingerprint.metadata_hash


def test_fingerprint_builtin_audits(model: Model, parent_model: Model):
    fingerprint = fingerprint_from_model(model, models={})

    model = SqlModel.parse_obj(
        {**model.dict(), "audits": [("unique_values", {"columns": exp.convert([to_column("a")])})]}
    )
    new_fingerprint = fingerprint_from_model(model, models={})
    assert new_fingerprint != fingerprint


def test_stamp(model: Model):
    original_fingerprint = fingerprint_from_model(model, models={})

    stamped_model = SqlModel(**{**model.dict(), "stamp": "test_stamp"})
    stamped_fingerprint = fingerprint_from_model(stamped_model, models={})

    assert original_fingerprint != stamped_fingerprint


def test_table_name(snapshot: Snapshot):
    # Mimic a direct breaking change.
    snapshot.fingerprint = SnapshotFingerprint(
        data_hash="1", metadata_hash="1", parent_data_hash="1"
    )
    snapshot.version = snapshot.fingerprint.to_version()
    snapshot.previous_versions = ()
    assert snapshot.table_name(is_dev=False, for_read=False) == "sqlmesh.name__3078928823"
    assert snapshot.table_name(is_dev=True, for_read=False) == "sqlmesh.name__3078928823"
    assert snapshot.table_name(is_dev=False, for_read=True) == "sqlmesh.name__3078928823"
    assert snapshot.table_name(is_dev=True, for_read=True) == "sqlmesh.name__3078928823"
    assert snapshot.table_name_for_mapping(is_dev=False) == "sqlmesh.name__3078928823"
    assert snapshot.table_name_for_mapping(is_dev=True) == "sqlmesh.name__3078928823"

    # Mimic an indirect non-breaking change.
    previous_data_version = snapshot.data_version
    snapshot.fingerprint = SnapshotFingerprint(
        data_hash="1", metadata_hash="1", parent_data_hash="2"
    )
    snapshot.previous_versions = (previous_data_version,)
    assert snapshot.table_name(is_dev=False, for_read=False) == "sqlmesh.name__3078928823"
    assert snapshot.table_name(is_dev=True, for_read=False) == "sqlmesh.name__781051917__temp"
    assert snapshot.table_name(is_dev=False, for_read=True) == "sqlmesh.name__3078928823"
    assert snapshot.table_name(is_dev=True, for_read=True) == "sqlmesh.name__3078928823"
    assert snapshot.table_name_for_mapping(is_dev=False) == "sqlmesh.name__3078928823"
    assert snapshot.table_name_for_mapping(is_dev=True) == "sqlmesh.name__3078928823"

    # Mimic a direct forward-only change.
    snapshot.fingerprint = SnapshotFingerprint(
        data_hash="2", metadata_hash="1", parent_data_hash="1"
    )
    snapshot.previous_versions = (previous_data_version,)
    assert snapshot.table_name(is_dev=False, for_read=False) == "sqlmesh.name__3078928823"
    assert snapshot.table_name(is_dev=True, for_read=False) == "sqlmesh.name__3049392110__temp"
    assert snapshot.table_name(is_dev=False, for_read=True) == "sqlmesh.name__3078928823"
    assert snapshot.table_name(is_dev=True, for_read=True) == "sqlmesh.name__3049392110__temp"
    assert snapshot.table_name_for_mapping(is_dev=False) == "sqlmesh.name__3078928823"
    assert snapshot.table_name_for_mapping(is_dev=True) == "sqlmesh.name__3049392110__temp"

    # Mimic a propmoted forward-only snapshot.
    snapshot.set_unpaused_ts(to_datetime("2022-01-01"))
    assert snapshot.table_name(is_dev=False, for_read=False) == "sqlmesh.name__3078928823"
    assert snapshot.table_name(is_dev=True, for_read=False) == "sqlmesh.name__3049392110__temp"
    assert snapshot.table_name(is_dev=False, for_read=True) == "sqlmesh.name__3078928823"
    assert snapshot.table_name(is_dev=True, for_read=True) == "sqlmesh.name__3049392110__temp"
    assert snapshot.table_name_for_mapping(is_dev=False) == "sqlmesh.name__3078928823"
    assert snapshot.table_name_for_mapping(is_dev=True) == "sqlmesh.name__3078928823"


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
            new=make_snapshot(SqlModel(name="a", query=parse_one("select 1, fun(a * 2)::INT, ds"))),
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
