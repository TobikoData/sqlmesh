import typing as t
from unittest.mock import call

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse, parse_one

from sqlmesh.core.engine_adapter import EngineAdapter, create_engine_adapter
from sqlmesh.core.engine_adapter.base import InsertOverwriteStrategy
from sqlmesh.core.macros import macro
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    IncrementalUnmanagedKind,
    ModelKind,
    ModelKindName,
    PythonModel,
    SqlModel,
    TimeColumn,
    ViewKind,
    load_sql_based_model,
)
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotEvaluator,
    SnapshotFingerprint,
    SnapshotTableInfo,
)
from sqlmesh.utils.errors import ConfigError, SQLMeshError
from sqlmesh.utils.metaprogramming import Executable


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
        "execution_time": "2020-01-01",
    }


@pytest.fixture
def adapter_mock(mocker: MockerFixture):
    transaction_mock = mocker.Mock()
    transaction_mock.__enter__ = mocker.Mock()
    transaction_mock.__exit__ = mocker.Mock()

    session_mock = mocker.Mock()
    session_mock.__enter__ = mocker.Mock()
    session_mock.__exit__ = mocker.Mock()

    adapter_mock = mocker.Mock()
    adapter_mock.transaction.return_value = transaction_mock
    adapter_mock.session.return_value = session_mock
    adapter_mock.dialect = "duckdb"
    return adapter_mock


def test_evaluate(mocker: MockerFixture, adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    payload = {"calls": 0}

    @macro()
    def x(evaluator, y=None) -> None:
        if "payload" not in evaluator.locals:
            return

        evaluator.locals["payload"]["calls"] += 1

        if y is not None:
            evaluator.locals["payload"]["y"] = y

    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind INCREMENTAL_BY_TIME_RANGE (time_column a),
                storage_format 'parquet',
            );

            @x();

            @DEF(a, 1);

            CREATE TABLE hook_called;

            SELECT a::int FROM tbl WHERE ds BETWEEN @start_ds and @end_ds;

            @x();

            @DEF(b, 2);

            @x(['a', 2, TRUE]);
            """
        ),
        macros=macro.get_registry(),
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.create([snapshot], {})
    evaluator.evaluate(
        snapshot,
        "2020-01-01",
        "2020-01-02",
        "2020-01-02",
        snapshots={},
        payload=payload,
    )

    assert payload["calls"] == 3
    assert payload["y"] == exp.convert(["a", 2, True])

    execute_calls = [call([parse_one('CREATE TABLE "hook_called"')])]
    adapter_mock.execute.assert_has_calls(execute_calls)

    adapter_mock.create_schema.assert_has_calls(
        [
            call("sqlmesh__test_schema"),
        ]
    )

    adapter_mock.create_table.assert_called_once_with(
        snapshot.table_name(),
        columns_to_types={"a": exp.DataType.build("int")},
        storage_format="parquet",
        partitioned_by=[exp.to_column("a")],
        partition_interval_unit=IntervalUnit.DAY,
        clustered_by=[],
        table_properties={},
    )


def test_evaluate_paused_forward_only_upstream(mocker: MockerFixture, make_snapshot):
    model = SqlModel(name="test_schema.test_model", query=parse_one("SELECT a, ds"))
    snapshot = make_snapshot(model)
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

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.promote([snapshot], "test_env")

    adapter_mock.create_schema.assert_called_once_with("test_schema__test_env")
    adapter_mock.create_view.assert_called_once_with(
        "test_schema__test_env.test_model",
        parse_one(
            f"SELECT * FROM sqlmesh__test_schema.test_schema__test_model__{snapshot.version}"
        ),
    )


def test_evaluate_materialized_view(mocker: MockerFixture, adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind VIEW (
                    materialized true
                )
            );

            SELECT a::int FROM tbl;
            """
        ),
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.evaluate(
        snapshot,
        "2020-01-01",
        "2020-01-02",
        "2020-01-02",
        snapshots={},
    )

    # Evaluation shouldn't take place because the rendered query hasn't changed
    # since the last view creation.
    assert not adapter_mock.create_view.called


def test_evaluate_materialized_view_with_execution_time_macro(
    mocker: MockerFixture, adapter_mock, make_snapshot
):
    evaluator = SnapshotEvaluator(adapter_mock)

    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind VIEW (
                    materialized true
                )
            );

            SELECT a::int FROM tbl WHERE ds < @execution_ds;
            """
        ),
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.evaluate(
        snapshot,
        "2020-01-01",
        "2020-01-02",
        "2020-01-02",
        snapshots={},
    )

    adapter_mock.create_view.assert_called_once_with(
        snapshot.table_name(),
        model.render_query(execution_time="2020-01-02"),
        model.columns_to_types,
        materialized=True,
    )


@pytest.mark.parametrize("insert_overwrite", [False, True])
def test_evaluate_incremental_unmanaged(
    mocker: MockerFixture, make_snapshot, adapter_mock, insert_overwrite
):
    model = SqlModel(
        name="test_schema.test_model",
        query=parse_one("SELECT 1, ds FROM tbl_a"),
        kind=IncrementalUnmanagedKind(insert_overwrite=insert_overwrite),
        partitioned_by=["ds"],
    )
    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator = SnapshotEvaluator(adapter_mock)
    evaluator.evaluate(
        snapshot,
        "2020-01-01",
        "2020-01-02",
        "2020-01-02",
        snapshots={},
    )

    if insert_overwrite:
        adapter_mock.insert_overwrite_by_partition.assert_called_once_with(
            snapshot.table_name(),
            model.render_query(),
            [exp.to_column("ds")],
            columns_to_types=model.columns_to_types,
        )
    else:
        adapter_mock.insert_append.assert_called_once_with(
            snapshot.table_name(),
            model.render_query(),
            columns_to_types=model.columns_to_types,
        )


def test_create_materialized_view(mocker: MockerFixture, adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind VIEW (
                    materialized true
                )
            );

            SELECT a::int FROM tbl;
            """
        ),
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.create([snapshot], {})

    adapter_mock.create_view.assert_called_once_with(
        snapshot.table_name(),
        model.render_query(),
        materialized=True,
    )


def test_promote_model_info(mocker: MockerFixture):
    adapter_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter")
    adapter_mock.dialect = "duckdb"

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
                kind_name=ModelKindName.FULL,
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

    current_table = "sqlmesh__test_schema.test_schema__test_model__1"

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
    snapshot = make_snapshot(model, version="1")
    snapshot.change_category = SnapshotChangeCategory.FORWARD_ONLY

    evaluator.migrate([snapshot], {})

    cursor_mock.execute.assert_has_calls(
        [
            call('ALTER TABLE "sqlmesh__test_schema"."test_schema__test_model__1" DROP COLUMN "b"'),
            call(
                'ALTER TABLE "sqlmesh__test_schema"."test_schema__test_model__1" ADD COLUMN "a" INT'
            ),
        ]
    )


def test_migrate_view(mocker: MockerFixture, make_snapshot):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    adapter = EngineAdapter(lambda: connection_mock, "")

    evaluator = SnapshotEvaluator(adapter)

    model = SqlModel(
        name="test_schema.test_model",
        kind=ViewKind(),
        storage_format="parquet",
        query=parse_one("SELECT c, a FROM tbl"),
    )
    snapshot = make_snapshot(model, version="1")
    snapshot.change_category = SnapshotChangeCategory.FORWARD_ONLY

    evaluator.migrate([snapshot], {})

    cursor_mock.execute.assert_has_calls(
        [
            call(
                'CREATE OR REPLACE VIEW "sqlmesh__test_schema"."test_schema__test_model__1" AS SELECT "c" AS "c", "a" AS "a" FROM "tbl" AS "tbl"'
            )
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
            ("sqlmesh__db", f"db__model__{version}", "BASE TABLE"),
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
    assert duck_conn.execute(f"SELECT * FROM sqlmesh__db.db__model__{version}").fetchall() == [(1,)]

    # test that existing tables work
    evaluator.evaluate(
        snapshot,
        "2020-01-01",
        "2020-01-01",
        "2020-01-01",
        snapshots={},
    )
    assert_tables_exist()
    assert duck_conn.execute(f"SELECT * FROM sqlmesh__db.db__model__{version}").fetchall() == [(1,)]


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
    evaluator.migrate([new_snapshot], {})

    evaluator.evaluate(
        new_snapshot,
        "2020-01-01",
        "2020-01-01",
        "2020-01-01",
        snapshots={},
    )

    assert duck_conn.execute(
        f"SELECT b FROM sqlmesh__db.db__model__{snapshot.version}"
    ).fetchall() == [(1,)]


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


@pytest.mark.parametrize(
    "input_dfs, output_dict",
    [
        (
            """pd.DataFrame({"a": [1, 2, 3], "ds": ["2023-01-01", "2023-01-02", "2023-01-03"]}),
        pd.DataFrame({"a": [4, 5, 6], "ds": ["2023-01-04", "2023-01-05", "2023-01-06"]}),
        pd.DataFrame({"a": [7, 8, 9], "ds": ["2023-01-07", "2023-01-08", "2023-01-09"]})""",
            {
                "a": {
                    0: 1,
                    1: 2,
                    2: 3,
                    3: 4,
                    4: 5,
                    5: 6,
                    6: 7,
                    7: 8,
                    8: 9,
                },
                "ds": {
                    0: "2023-01-01",
                    1: "2023-01-02",
                    2: "2023-01-03",
                    3: "2023-01-04",
                    4: "2023-01-05",
                    5: "2023-01-06",
                    6: "2023-01-07",
                    7: "2023-01-08",
                    8: "2023-01-09",
                },
            },
        ),
        (
            """pd.DataFrame({"a": [1, 2, 3], "ds": ["2023-01-01", "2023-01-02", "2023-01-03"]})""",
            {
                "a": {
                    0: 1,
                    1: 2,
                    2: 3,
                },
                "ds": {
                    0: "2023-01-01",
                    1: "2023-01-02",
                    2: "2023-01-03",
                },
            },
        ),
    ],
)
def test_snapshot_evaluator_yield_pd(adapter_mock, make_snapshot, input_dfs, output_dict):
    adapter_mock.is_pyspark_df.return_value = False
    adapter_mock.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    adapter_mock.try_get_df = lambda x: x
    evaluator = SnapshotEvaluator(adapter_mock)

    snapshot = make_snapshot(
        PythonModel(
            name="db.model",
            entrypoint="python_func",
            kind=IncrementalByTimeRangeKind(time_column=TimeColumn(column="ds", format="%Y-%m-%d")),
            columns={
                "a": "INT",
                "ds": "STRING",
            },
            python_env={
                "python_func": Executable(
                    name="python_func",
                    alias="python_func",
                    path="test_snapshot_evaluator.py",
                    payload=f"""import pandas as pd
def python_func(**kwargs):
    for df in [
        {input_dfs}
    ]:
        yield df""",
                )
            },
        )
    )

    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    evaluator.create([snapshot], {})

    evaluator.evaluate(
        snapshot,
        "2023-01-01",
        "2023-01-09",
        "2023-01-09",
        snapshots={},
    )

    assert adapter_mock.insert_overwrite_by_time_partition.call_args[0][1].to_dict() == output_dict
