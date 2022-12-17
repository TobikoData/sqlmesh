import json

import pytest
from _pytest.monkeypatch import MonkeyPatch
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.macros import macro
from sqlmesh.core.model import Model
from sqlmesh.core.model_kind import IncrementalByTimeRange
from sqlmesh.core.snapshot import Snapshot, fingerprint_from_model
from sqlmesh.utils.date import to_datetime, to_timestamp


@pytest.fixture
def parent_model():
    return Model(
        name="parent.tbl",
        dialect="spark",
        kind=IncrementalByTimeRange(),
        query=parse_one("SELECT 1, ds"),
    )


@pytest.fixture
def model():
    return Model(
        name="name",
        owner="owner",
        dialect="spark",
        kind=IncrementalByTimeRange(),
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


def test_json(snapshot: Snapshot):
    assert json.loads(snapshot.json()) == {
        "created_ts": 1663891973000,
        "ttl": "in 1 week",
        "fingerprint": snapshot.fingerprint,
        "physical_schema": "sqlmesh",
        "intervals": [],
        "model": {
            "audits": {},
            "cron": "1 0 * * *",
            "batch_size": 30,
            "kind": {
                "name": "incremental_by_time_range",
                "time_column": {"column": "ds"},
            },
            "start": "2020-01-01",
            "dialect": "spark",
            "name": "name",
            "owner": "owner",
            "query": "SELECT @EACH(ARRAY(1, 2), x -> x), ds FROM parent.tbl",
        },
        "name": "name",
        "parents": [
            {"name": "parent.tbl", "fingerprint": snapshot.parents[0].fingerprint}
        ],
        "previous_versions": [],
        "indirect_versions": {},
        "updated_ts": 1663891973000,
        "version": snapshot.version,
    }


def test_add_interval(snapshot: Snapshot, make_snapshot):
    with pytest.raises(ValueError):
        snapshot.add_interval("2020-01-02", "2020-01-01")

    snapshot.add_interval("2020-01-01", "2020-01-01")
    assert snapshot.intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-02"))
    ]

    snapshot.add_interval("2020-01-02", "2020-01-02")
    assert snapshot.intervals == [
        (to_timestamp("2020-01-01"), to_timestamp("2020-01-03"))
    ]

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
        (to_timestamp("2019-01-01"), to_timestamp("2020-02-01")),
    ]
    snapshot.add_interval("2018-12-31 23:59:59", "2020-01-31 00:00:01")
    assert snapshot.intervals == [
        (to_timestamp("2018-12-31"), to_timestamp("2020-02-01")),
    ]

    new_snapshot = make_snapshot(snapshot.model)
    new_snapshot.add_interval("2020-01-29", "2020-02-01")
    new_snapshot.add_interval("2020-02-05", "2020-02-10")
    new_snapshot.merge_intervals(snapshot)
    assert new_snapshot.intervals == [
        (to_timestamp("2018-12-31"), to_timestamp("2020-02-02")),
        (to_timestamp("2020-02-05"), to_timestamp("2020-02-11")),
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
    assert (
        snapshot.missing_intervals("2020-01-03 00:00:01", "2020-01-05 00:00:02") == []
    )
    assert snapshot.missing_intervals("2020-01-03 00:00:01", "2020-01-07 00:00:02") == [
        (to_timestamp("2020-01-06"), to_timestamp("2020-01-07")),
    ]


def test_remove_intervals(snapshot: Snapshot):
    snapshot.add_interval("2020-01-01", "2020-01-01")
    snapshot.remove_interval("2020-01-01", "2020-01-01")
    assert snapshot.intervals == []

    snapshot.add_interval("2020-01-01", "2020-01-01")
    snapshot.add_interval("2020-01-03", "2020-01-03")
    snapshot.remove_interval("2020-01-01", "2020-01-01")
    assert snapshot.intervals == [
        (to_timestamp("2020-01-03"), to_timestamp("2020-01-04"))
    ]

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
    macro.get_registry()
    fingerprint = fingerprint_from_model(model, models={})
    original_fingerprint = "3127055399_0"

    assert fingerprint == original_fingerprint
    assert fingerprint_from_model(model, physical_schema="x", models={}) != fingerprint

    parent_fingerprint = fingerprint_from_model(parent_model, models={})
    with_parent_fingerprint = fingerprint_from_model(
        model, models={"parent.tbl": parent_model}
    )
    assert with_parent_fingerprint != fingerprint
    assert int(with_parent_fingerprint.split("_")[-1]) > 0

    assert (
        fingerprint_from_model(
            model,
            models={
                "parent.tbl": Model(
                    **{**model.dict(), "query": parse_one("select 2, ds")}
                )
            },
        )
        != with_parent_fingerprint
    )

    model = Model(**{**model.dict(), "query": parse_one("select 1, ds")})
    new_fingerprint = fingerprint_from_model(model, models={})
    assert new_fingerprint != fingerprint

    model = Model(**{**model.dict(), "query": parse_one("select 1, ds -- annotation")})
    assert new_fingerprint == fingerprint_from_model(model, models={})
