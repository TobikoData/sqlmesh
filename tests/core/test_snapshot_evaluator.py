import typing as t
from unittest.mock import call

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.model import Model
from sqlmesh.core.model_kind import IncrementalByTimeRange, ModelKind, ModelKindName
from sqlmesh.core.schema_diff import SchemaDelta
from sqlmesh.core.snapshot import Snapshot, SnapshotTableInfo
from sqlmesh.core.snapshot_evaluator import SnapshotEvaluator


@pytest.fixture
def snapshot(duck_conn, make_snapshot) -> Snapshot:
    duck_conn.execute("CREATE VIEW tbl AS SELECT 1 AS a")

    model = Model(
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
    adapter_mock = mocker.Mock()

    evaluator = SnapshotEvaluator(adapter_mock)

    model = Model(
        name="test_schema.test_model",
        kind=IncrementalByTimeRange(time_column="a"),
        storage_format="parquet",
        query=parse_one(
            "SELECT a::int FROM tbl WHERE ds BETWEEN @start_ds and @end_ds"
        ),
    )

    snapshot = make_snapshot(model, physical_schema="physical_schema", version="1")
    evaluator.create([snapshot], {})
    evaluator.evaluate(
        snapshot,
        "2020-01-01",
        "2020-01-02",
        "2020-01-02",
        mapping={},
    )

    assert adapter_mock.create_schema.mock_calls == [
        call("physical_schema"),
    ]

    adapter_mock.create_table.assert_called_once_with(
        "physical_schema.test_schema__test_model__1",
        query_or_columns={"a": exp.DataType.build("int")},
        storage_format="parquet",
        partitioned_by=["a"],
    )


def test_promote(mocker: MockerFixture, make_snapshot):
    adapter_mock = mocker.Mock()

    evaluator = SnapshotEvaluator(adapter_mock)

    model = Model(
        name="test_schema.test_model",
        kind=IncrementalByTimeRange(time_column="a"),
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

    model = Model(
        name="test_schema.test_model",
        kind=IncrementalByTimeRange(time_column="a"),
        storage_format="parquet",
        query=parse_one("SELECT a FROM tbl WHERE ds BETWEEN @start_ds and @end_ds"),
    )
    snapshot = make_snapshot(model, physical_schema="physical_schema", version="1")

    evaluator.migrate([snapshot], {})

    adapter_mock.create_table.assert_called_once_with(
        "physical_schema.test_schema__test_model__1__tmp__2497578715_0",
        query_or_columns=mocker.ANY,
        storage_format=mocker.ANY,
        partitioned_by=mocker.ANY,
    )

    adapter_mock.alter_table.assert_called_once_with(
        snapshot.table_name,
        {"a": "INT"},
        ["b"],
    )

    adapter_mock.drop_table.assert_called_once_with(
        "physical_schema.test_schema__test_model__1__tmp__2497578715_0"
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
        mapping={},
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
        mapping={},
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
    updated_model = Model.parse_obj(updated_model_dict)

    new_snapshot = make_snapshot(updated_model)
    new_snapshot.version = snapshot.version

    evaluator.migrate([new_snapshot], {})

    evaluator.evaluate(
        new_snapshot,
        "2020-01-01",
        "2020-01-01",
        "2020-01-01",
        mapping={},
    )

    assert duck_conn.execute(
        f"SELECT b FROM sqlmesh.db__model__{snapshot.version}"
    ).fetchall() == [(1,)]
