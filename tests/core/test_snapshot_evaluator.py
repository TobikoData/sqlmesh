import typing as t
from unittest.mock import call

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    ModelKind,
    ModelKindName,
    SqlModel,
)
from sqlmesh.core.schema_diff import SchemaDelta
from sqlmesh.core.snapshot import Snapshot, SnapshotEvaluator, SnapshotTableInfo
from sqlmesh.utils.errors import SQLMeshError


@pytest.fixture
def snapshot(duck_conn, make_snapshot) -> Snapshot:
    duck_conn.execute("CREATE VIEW tbl AS SELECT 1 AS a")

    model = SqlModel(
        name="db.model",
        kind=ModelKind(name=ModelKindName.SNAPSHOT),
        query=parse_one("SELECT a::int FROM tbl"),
    )

    snapshot = make_snapshot(model)
    snapshot.version = snapshot.fingerprint
    return snapshot


@pytest.fixture
def date_kwargs() -> t.Dict[str, str]:
    return {
        "start": "2020-01-01",
        "end": "2020-01-01",
        "latest": "2020-01-01",
    }


def test_evaluate(mocker: MockerFixture, make_snapshot):
    transaction_mock = mocker.Mock()
    transaction_mock.__enter__ = mocker.Mock()
    transaction_mock.__exit__ = mocker.Mock()

    adapter_mock = mocker.Mock()
    adapter_mock.transaction.return_value = transaction_mock

    evaluator = SnapshotEvaluator(adapter_mock)

    model = SqlModel(
        name="test_schema.test_model",
        kind=IncrementalByTimeRangeKind(time_column="a"),
        storage_format="parquet",
        query=parse_one(
            "SELECT a::int FROM tbl WHERE ds BETWEEN @start_ds and @end_ds"
        ),
    )

    snapshot = make_snapshot(model, physical_schema="physical_schema")
    snapshot.version = snapshot.fingerprint

    evaluator.create([snapshot], {})
    evaluator.evaluate(
        snapshot,
        "2020-01-01",
        "2020-01-02",
        "2020-01-02",
        snapshots={},
    )

    adapter_mock.create_schema.assert_has_calls(
        [
            call("physical_schema"),
        ]
    )

    adapter_mock.create_table.assert_called_once_with(
        snapshot.table_name(),
        query_or_columns_to_types={"a": exp.DataType.build("int")},
        storage_format="parquet",
        partitioned_by=["a"],
    )


def test_evaluate_paused_forward_only_upstream(mocker: MockerFixture, make_snapshot):
    model = SqlModel(name="test_schema.test_model", query=parse_one("SELECT a, ds"))
    snapshot = make_snapshot(model, physical_schema="physical_schema")
    snapshot.version = snapshot.fingerprint

    parent_snapshot = make_snapshot(
        SqlModel(name="test_parent_model", query=parse_one("SELECT b, ds"))
    )
    parent_snapshot.set_version("test_version")

    evaluator = SnapshotEvaluator(mocker.Mock())
    with pytest.raises(
        SQLMeshError, match=r".*Create and apply a new plan to fix this issue."
    ):
        evaluator.evaluate(
            snapshot,
            "2020-01-01",
            "2020-01-02",
            "2020-01-02",
            snapshots={parent_snapshot.name: parent_snapshot},
        )


def test_promote(mocker: MockerFixture, make_snapshot):
    adapter_mock = mocker.Mock()

    evaluator = SnapshotEvaluator(adapter_mock)

    model = SqlModel(
        name="test_schema.test_model",
        kind=IncrementalByTimeRangeKind(time_column="a"),
        storage_format="parquet",
        query=parse_one("SELECT a FROM tbl WHERE ds BETWEEN @start_ds and @end_ds"),
    )

    evaluator.promote(
        [make_snapshot(model, physical_schema="physical_schema", version="1")],
        "test_env",
    )

    adapter_mock.create_schema.assert_called_once_with("test_schema__test_env")
    adapter_mock.create_view.assert_called_once_with(
        "test_schema__test_env.test_model",
        parse_one("SELECT * FROM physical_schema.test_schema__test_model__1"),
    )


def test_promote_model_info(mocker: MockerFixture):
    adapter_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter")

    evaluator = SnapshotEvaluator(adapter_mock)

    evaluator.promote(
        [
            SnapshotTableInfo(
                physical_schema="physical_schema",
                name="test_schema.test_model",
                fingerprint="1",
                version="1",
                parents=[],
                is_materialized=True,
                is_embedded_kind=False,
            )
        ],
        "test_env",
    )

    adapter_mock.create_schema.assert_called_once_with("test_schema__test_env")
    adapter_mock.create_view.assert_called_once_with(
        "test_schema__test_env.test_model",
        parse_one("SELECT * FROM physical_schema.test_schema__test_model__1"),
    )


def test_migrate(mocker: MockerFixture, make_snapshot):
    adapter_mock = mocker.Mock()

    schema_diff_calculator_mock = mocker.patch(
        "sqlmesh.core.schema_diff.SchemaDiffCalculator.calculate"
    )
    schema_diff_calculator_mock.return_value = [
        SchemaDelta.drop("b", "STRING"),
        SchemaDelta.add("a", "INT"),
    ]

    evaluator = SnapshotEvaluator(adapter_mock)

    model = SqlModel(
        name="test_schema.test_model",
        kind=IncrementalByTimeRangeKind(time_column="a"),
        storage_format="parquet",
        query=parse_one("SELECT a FROM tbl WHERE ds BETWEEN @start_ds and @end_ds"),
    )
    snapshot = make_snapshot(model, physical_schema="physical_schema", version="1")

    evaluator.migrate([snapshot])

    adapter_mock.alter_table.assert_called_once_with(
        snapshot.table_name(),
        {"a": "INT"},
        ["b"],
    )


def test_evaluate_creation_duckdb(
    snapshot: Snapshot,
    duck_conn,
    date_kwargs: t.Dict[str, str],
):
    evaluator = SnapshotEvaluator(create_engine_adapter(lambda: duck_conn, "duckdb"))
    evaluator.create([snapshot], {})
    version = snapshot.version

    def assert_tables_exist() -> None:
        assert duck_conn.execute(
            "SELECT table_schema, table_name, table_type FROM information_schema.tables"
        ).fetchall() == [
            ("sqlmesh", f"db__model__{version}", "BASE TABLE"),
            ("main", "tbl", "VIEW"),
        ]

    # test that a clean run works
    evaluator.evaluate(
        snapshot,
        "2020-01-01",
        "2020-01-01",
        "2020-01-01",
        snapshots={},
    )
    assert_tables_exist()
    assert duck_conn.execute(
        f"SELECT * FROM sqlmesh.db__model__{version}"
    ).fetchall() == [(1,)]

    # test that existing tables work
    evaluator.evaluate(
        snapshot,
        "2020-01-01",
        "2020-01-01",
        "2020-01-01",
        snapshots={},
    )
    assert_tables_exist()
    assert duck_conn.execute(
        f"SELECT * FROM sqlmesh.db__model__{version}"
    ).fetchall() == [
        (1,),
        (1,),
    ]


def test_migrate_duckdb(snapshot: Snapshot, duck_conn, make_snapshot):
    evaluator = SnapshotEvaluator(create_engine_adapter(lambda: duck_conn, "duckdb"))
    evaluator.create([snapshot], {})

    updated_model_dict = snapshot.model.dict()
    updated_model_dict["query"] = "SELECT a AS b FROM tbl"
    updated_model = SqlModel.parse_obj(updated_model_dict)

    new_snapshot = make_snapshot(updated_model)
    new_snapshot.version = snapshot.version

    evaluator.create([new_snapshot], {})
    evaluator.migrate([new_snapshot])

    evaluator.evaluate(
        new_snapshot,
        "2020-01-01",
        "2020-01-01",
        "2020-01-01",
        snapshots={},
    )

    assert duck_conn.execute(
        f"SELECT b FROM sqlmesh.db__model__{snapshot.version}"
    ).fetchall() == [(1,)]
