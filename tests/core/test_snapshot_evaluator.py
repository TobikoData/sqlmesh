import typing as t
from datetime import datetime
from unittest.mock import call

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse, parse_one

from sqlmesh.core.context import ExecutionContext
from sqlmesh.core.engine_adapter import EngineAdapter, create_engine_adapter
from sqlmesh.core.hooks import hook
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    ModelKind,
    ModelKindName,
    SqlModel,
    load_model,
)
from sqlmesh.core.model.meta import IntervalUnit
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotEvaluator,
    SnapshotFingerprint,
    SnapshotTableInfo,
)
from sqlmesh.utils.errors import ConfigError, SQLMeshError


@pytest.fixture
def snapshot(duck_conn, make_snapshot) -> Snapshot:
    duck_conn.execute("CREATE VIEW tbl AS SELECT 1 AS a")

    model = SqlModel(
        name="db.model",
        kind=ModelKind(name=ModelKindName.FULL),
        query=parse_one("SELECT a::int FROM tbl"),
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    return snapshot


@pytest.fixture
def date_kwargs() -> t.Dict[str, str]:
    return {
        "start": "2020-01-01",
        "end": "2020-01-01",
        "latest": "2020-01-01",
    }


@pytest.fixture
def adapter_mock(mocker: MockerFixture):
    transaction_mock = mocker.Mock()
    transaction_mock.__enter__ = mocker.Mock()
    transaction_mock.__exit__ = mocker.Mock()

    adapter_mock = mocker.Mock()
    adapter_mock.transaction.return_value = transaction_mock
    return adapter_mock


def test_evaluate(mocker: MockerFixture, adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    payload = {"calls": 0}

    @hook()
    def x(
        context: ExecutionContext,
        start: datetime,
        end: datetime,
        latest: datetime,
        **kwargs,
    ) -> None:
        payload = kwargs["payload"]
        payload["calls"] += 1

        if "y" in kwargs:
            payload["y"] = kwargs["y"]

    model = load_model(
        parse(  # type: ignore
            """
        MODEL (
            name test_schea.test_model,
            kind INCREMENTAL_BY_TIME_RANGE (time_column a),
            storage_format 'parquet',
            pre [x(), 'create table hook_called'],
            post [x(), x(y=['a', 2, TRUE])],
        );

        SELECT @SQL(@REDUCE([100, 200, 300, 400], (x,y) -> x + y));

        SELECT a::int FROM tbl WHERE ds BETWEEN @start_ds and @end_ds
        """
        ),
        hooks=hook.get_registry(),
    )

    snapshot = make_snapshot(model, physical_schema="physical_schema")
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.create([snapshot], {})
    evaluator.evaluate(
        snapshot,
        "2020-01-01",
        "2020-01-02",
        "2020-01-02",
        payload=payload,
        snapshots={},
    )

    assert payload["calls"] == 3
    assert payload["y"] == ["a", 2, True]

    execute_calls = [call(parse_one("select 1000")), call(parse_one("create table hook_called"))]
    adapter_mock.execute.assert_has_calls(execute_calls)

    adapter_mock.create_schema.assert_has_calls(
        [
            call("physical_schema"),
        ]
    )

    adapter_mock.create_table.assert_called_once_with(
        snapshot.table_name(),
        columns_to_types={"a": exp.DataType.build("int")},
        storage_format="parquet",
        partitioned_by=["a"],
        partition_interval_unit=IntervalUnit.DAY,
    )


def test_evaluate_paused_forward_only_upstream(mocker: MockerFixture, make_snapshot):
    model = SqlModel(name="test_schema.test_model", query=parse_one("SELECT a, ds"))
    snapshot = make_snapshot(model, physical_schema="physical_schema")
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    parent_snapshot = make_snapshot(
        SqlModel(name="test_parent_model", query=parse_one("SELECT b, ds"))
    )
    parent_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    parent_snapshot.version = "test_version"

    evaluator = SnapshotEvaluator(mocker.Mock())
    with pytest.raises(SQLMeshError, match=r".*Create and apply a new plan to fix this issue."):
        evaluator.evaluate(
            snapshot,
            "2020-01-01",
            "2020-01-02",
            "2020-01-02",
            snapshots={parent_snapshot.name: parent_snapshot},
        )


def test_promote(mocker: MockerFixture, adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    model = SqlModel(
        name="test_schema.test_model",
        kind=IncrementalByTimeRangeKind(time_column="a"),
        storage_format="parquet",
        query=parse_one("SELECT a FROM tbl WHERE ds BETWEEN @start_ds and @end_ds"),
    )

    snapshot = make_snapshot(model, physical_schema="physical_schema")
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.promote([snapshot], "test_env")

    adapter_mock.create_schema.assert_called_once_with("test_schema__test_env")
    adapter_mock.create_view.assert_called_once_with(
        "test_schema__test_env.test_model",
        parse_one(f"SELECT * FROM physical_schema.test_schema__test_model__{snapshot.version}"),
    )


def test_promote_model_info(mocker: MockerFixture):
    adapter_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter")

    evaluator = SnapshotEvaluator(adapter_mock)

    evaluator.promote(
        [
            SnapshotTableInfo(
                physical_schema="physical_schema",
                name="test_schema.test_model",
                fingerprint=SnapshotFingerprint(data_hash="1", metadata_hash="1"),
                version="1",
                change_category=SnapshotChangeCategory.BREAKING,
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
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    adapter = EngineAdapter(lambda: connection_mock, "")

    current_table = "physical_schema.test_schema__test_model__1"

    def columns(table_name: t.Union[str, exp.Table]) -> t.Dict[str, exp.DataType]:
        if table_name == current_table:
            return {
                "c": exp.DataType.build("int"),
                "b": exp.DataType.build("int"),
            }
        else:
            return {
                "c": exp.DataType.build("int"),
                "a": exp.DataType.build("int"),
            }

    adapter.columns = columns  # type: ignore

    evaluator = SnapshotEvaluator(adapter)

    model = SqlModel(
        name="test_schema.test_model",
        kind=IncrementalByTimeRangeKind(time_column="a"),
        storage_format="parquet",
        query=parse_one("SELECT c, a FROM tbl WHERE ds BETWEEN @start_ds and @end_ds"),
    )
    snapshot = make_snapshot(model, physical_schema="physical_schema", version="1")
    snapshot.change_category = SnapshotChangeCategory.FORWARD_ONLY

    evaluator.migrate([snapshot])

    cursor_mock.execute.assert_has_calls(
        [
            call("""ALTER TABLE physical_schema.test_schema__test_model__1 DROP COLUMN b"""),
            call("""ALTER TABLE physical_schema.test_schema__test_model__1 ADD COLUMN a INT"""),
        ]
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
    assert duck_conn.execute(f"SELECT * FROM sqlmesh.db__model__{version}").fetchall() == [(1,)]

    # test that existing tables work
    evaluator.evaluate(
        snapshot,
        "2020-01-01",
        "2020-01-01",
        "2020-01-01",
        snapshots={},
    )
    assert_tables_exist()
    assert duck_conn.execute(f"SELECT * FROM sqlmesh.db__model__{version}").fetchall() == [(1,)]


def test_migrate_duckdb(snapshot: Snapshot, duck_conn, make_snapshot):
    evaluator = SnapshotEvaluator(create_engine_adapter(lambda: duck_conn, "duckdb"))
    evaluator.create([snapshot], {})

    updated_model_dict = snapshot.model.dict()
    updated_model_dict["query"] = "SELECT a::int, 1 as b FROM tbl"
    updated_model = SqlModel.parse_obj(updated_model_dict)

    new_snapshot = make_snapshot(updated_model)
    new_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
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

    assert duck_conn.execute(f"SELECT b FROM sqlmesh.db__model__{snapshot.version}").fetchall() == [
        (1,)
    ]


def test_audit_unversioned(mocker: MockerFixture, adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    snapshot = make_snapshot(
        SqlModel(
            name="db.model",
            kind=ModelKind(name=ModelKindName.FULL),
            query=parse_one("SELECT a::int FROM tbl"),
        )
    )

    with pytest.raises(
        ConfigError,
        match="Cannot audit 'db.model' because it has not been versioned yet. Apply a plan first.",
    ):
        evaluator.audit(snapshot=snapshot, snapshots={})
