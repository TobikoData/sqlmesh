import typing as t
from unittest.mock import call

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse, parse_one, select

from sqlmesh.core.audit import ModelAudit, StandaloneAudit
from sqlmesh.core.dialect import schema_, to_schema
from sqlmesh.core.engine_adapter import EngineAdapter, create_engine_adapter
from sqlmesh.core.engine_adapter.base import MERGE_SOURCE_ALIAS, MERGE_TARGET_ALIAS
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    InsertOverwriteStrategy,
)
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.macros import RuntimeStage, macro
from sqlmesh.core.model import (
    FullKind,
    IncrementalByTimeRangeKind,
    IncrementalUnmanagedKind,
    PythonModel,
    SqlModel,
    TimeColumn,
    ViewKind,
    load_sql_based_model,
)
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotChangeCategory,
    SnapshotEvaluator,
    SnapshotTableCleanupTask,
)
from sqlmesh.utils.date import to_timestamp
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.metaprogramming import Executable


@pytest.fixture
def snapshot(duck_conn, make_snapshot) -> Snapshot:
    duck_conn.execute("CREATE VIEW tbl AS SELECT 1 AS a")

    model = SqlModel(
        name="db.model",
        kind=FullKind(),
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
    adapter_mock.HAS_VIEW_BINDING = False
    adapter_mock.wap_supported.return_value = False
    adapter_mock.get_data_objects.return_value = []
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
        start="2020-01-01",
        end="2020-01-02",
        execution_time="2020-01-02",
        snapshots={},
        payload=payload,
    )

    assert payload["calls"] == 3
    assert payload["y"] == exp.convert(["a", 2, True])

    execute_calls = [call([parse_one('CREATE TABLE "hook_called"')])]
    adapter_mock.execute.assert_has_calls(execute_calls)

    adapter_mock.create_schema.assert_has_calls(
        [
            call(to_schema("sqlmesh__test_schema")),
        ]
    )

    common_kwargs = dict(
        columns_to_types={"a": exp.DataType.build("int")},
        storage_format="parquet",
        partitioned_by=[exp.to_column("a")],
        partition_interval_unit=IntervalUnit.DAY,
        clustered_by=[],
        table_properties={},
        table_description=None,
    )

    adapter_mock.create_table.assert_has_calls(
        [
            call(
                snapshot.table_name(is_deployable=False),
                column_descriptions=None,
                **common_kwargs,
            ),
            call(
                snapshot.table_name(),
                column_descriptions={},
                **common_kwargs,
            ),
        ]
    )


def test_runtime_stages(capsys, mocker, adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    @macro()
    def increment_stage_counter(evaluator) -> None:
        # Hack which allows us to intercept the different runtime stage values
        print(f"RuntimeStage value: {evaluator.locals['runtime_stage']}")

    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind FULL,
            );

            @increment_stage_counter();
            @if(@runtime_stage = 'evaluating', ALTER TABLE test_schema.foo MODIFY COLUMN c SET MASKING POLICY p);

            SELECT 1 AS a, @runtime_stage AS b;
            """
        ),
        macros=macro.get_registry(),
    )

    capsys.readouterr()

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    assert f"RuntimeStage value: {RuntimeStage.LOADING.value}" in capsys.readouterr().out

    evaluator.create([snapshot], {})
    assert f"RuntimeStage value: {RuntimeStage.CREATING.value}" in capsys.readouterr().out

    evaluator.evaluate(
        snapshot, start="2020-01-01", end="2020-01-02", execution_time="2020-01-02", snapshots={}
    )
    assert f"RuntimeStage value: {RuntimeStage.EVALUATING.value}" in capsys.readouterr().out

    empty_call = call([])
    non_empty_calls = [c for c in adapter_mock.execute.mock_calls if c != empty_call]
    assert len(non_empty_calls) == 1
    assert non_empty_calls[0] == call(
        [parse_one("ALTER TABLE test_schema.foo MODIFY COLUMN c SET MASKING POLICY p")]
    )

    assert snapshot.model.render_query().sql() == '''SELECT 1 AS "a", 'loading' AS "b"'''
    assert (
        snapshot.model.render_query(runtime_stage=RuntimeStage.CREATING).sql()
        == '''SELECT 1 AS "a", 'creating' AS "b"'''
    )
    assert (
        snapshot.model.render_query(runtime_stage=RuntimeStage.EVALUATING).sql()
        == '''SELECT 1 AS "a", 'evaluating' AS "b"'''
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

    evaluator.promote([snapshot], EnvironmentNamingInfo(name="test_env"))

    adapter_mock.create_schema.assert_called_once_with(to_schema("test_schema__test_env"))
    adapter_mock.create_view.assert_called_once_with(
        "test_schema__test_env.test_model",
        parse_one(
            f"SELECT * FROM sqlmesh__test_schema.test_schema__test_model__{snapshot.version}"
        ),
        table_description=None,
        column_descriptions=None,
    )


def test_promote_default_catalog(adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    model = SqlModel(
        name="test_schema.test_model",
        kind=IncrementalByTimeRangeKind(time_column="a"),
        storage_format="parquet",
        query=parse_one("SELECT a FROM tbl WHERE ds BETWEEN @start_ds and @end_ds"),
        default_catalog="test_catalog",
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.promote([snapshot], EnvironmentNamingInfo(name="test_env"))

    adapter_mock.create_schema.assert_called_once_with(
        schema_("test_schema__test_env", "test_catalog")
    )
    adapter_mock.create_view.assert_called_once_with(
        "test_catalog.test_schema__test_env.test_model",
        parse_one(
            f"SELECT * FROM test_catalog.sqlmesh__test_schema.test_schema__test_model__{snapshot.version}"
        ),
        table_description=None,
        column_descriptions=None,
    )


def test_promote_forward_only(mocker: MockerFixture, adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    model = SqlModel(
        name="test_schema.test_model",
        kind=IncrementalByTimeRangeKind(time_column="a"),
        query=parse_one("SELECT a FROM tbl WHERE ds BETWEEN @start_ds and @end_ds"),
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    snapshot.version = "test_version"

    evaluator.promote(
        [snapshot],
        EnvironmentNamingInfo(name="test_env"),
        deployability_index=DeployabilityIndex.none_deployable(),
    )

    snapshot.unpaused_ts = to_timestamp("2023-01-01")
    evaluator.promote(
        [snapshot],
        EnvironmentNamingInfo(name="test_env"),
        deployability_index=DeployabilityIndex(indexed_ids=[snapshot.snapshot_id]),
    )

    adapter_mock.create_schema.assert_has_calls(
        [
            call(to_schema("test_schema__test_env")),
            call(to_schema("test_schema__test_env")),
        ]
    )
    adapter_mock.create_view.assert_has_calls(
        [
            call(
                "test_schema__test_env.test_model",
                parse_one(
                    f"SELECT * FROM sqlmesh__test_schema.test_schema__test_model__{snapshot.fingerprint.to_version()}__temp"
                ),
                table_description=None,
                column_descriptions=None,
            ),
            call(
                "test_schema__test_env.test_model",
                parse_one(
                    "SELECT * FROM sqlmesh__test_schema.test_schema__test_model__test_version"
                ),
                table_description=None,
                column_descriptions=None,
            ),
        ]
    )


def test_cleanup(mocker: MockerFixture, adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    def create_and_cleanup(name: str, dev_table_only: bool):
        model = SqlModel(
            name=name,
            kind=IncrementalByTimeRangeKind(time_column="a"),
            storage_format="parquet",
            query=parse_one("SELECT a FROM tbl WHERE ds BETWEEN @start_ds and @end_ds"),
        )

        snapshot = make_snapshot(model)
        snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
        snapshot.version = "test_version"

        evaluator.promote([snapshot], EnvironmentNamingInfo(name="test_env"))
        evaluator.cleanup(
            [SnapshotTableCleanupTask(snapshot=snapshot.table_info, dev_table_only=dev_table_only)]
        )
        return snapshot

    snapshot = create_and_cleanup("catalog.test_schema.test_model", True)
    adapter_mock.drop_table.assert_called_once_with(
        f"catalog.sqlmesh__test_schema.test_schema__test_model__{snapshot.fingerprint.to_version()}__temp"
    )
    adapter_mock.reset_mock()

    snapshot = create_and_cleanup("test_schema.test_model", False)
    adapter_mock.drop_table.assert_has_calls(
        [
            call(
                f"sqlmesh__test_schema.test_schema__test_model__{snapshot.fingerprint.to_version()}__temp"
            ),
            call(f"sqlmesh__test_schema.test_schema__test_model__{snapshot.version}"),
        ]
    )
    adapter_mock.reset_mock()

    snapshot = create_and_cleanup("test_model", False)
    adapter_mock.drop_table.assert_has_calls(
        [
            call(f"sqlmesh__default.test_model__{snapshot.fingerprint.to_version()}__temp"),
            call(f"sqlmesh__default.test_model__{snapshot.version}"),
        ]
    )


@pytest.mark.parametrize("view_exists", [True, False])
def test_evaluate_materialized_view(
    mocker: MockerFixture, adapter_mock, make_snapshot, view_exists: bool
):
    adapter_mock.table_exists.return_value = view_exists
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
    snapshot.add_interval("2023-01-01", "2023-01-01")

    evaluator.evaluate(
        snapshot,
        start="2020-01-01",
        end="2020-01-02",
        execution_time="2020-01-02",
        snapshots={},
    )

    adapter_mock.table_exists.assert_called_once_with(snapshot.table_name())

    if view_exists:
        # Evaluation shouldn't take place because the rendered query hasn't changed
        # since the last view creation.
        assert not adapter_mock.create_view.called
    else:
        # If the view doesn't exist, it should be created even if the rendered query
        # hasn't changed since the last view creation.
        adapter_mock.create_view.assert_called_once_with(
            snapshot.table_name(),
            model.render_query(),
            model.columns_to_types,
            materialized=True,
            table_properties={},
            table_description=None,
            column_descriptions={},
        )


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
        start="2020-01-01",
        end="2020-01-02",
        execution_time="2020-01-02",
        snapshots={},
    )

    adapter_mock.create_view.assert_called_once_with(
        snapshot.table_name(),
        model.render_query(execution_time="2020-01-02"),
        model.columns_to_types,
        materialized=True,
        table_properties={},
        table_description=None,
        column_descriptions={},
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
        start="2020-01-01",
        end="2020-01-02",
        execution_time="2020-01-02",
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


def test_create_tables_exists(mocker: MockerFixture, adapter_mock, make_snapshot):
    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind VIEW
            );

            SELECT a::int FROM tbl;
            """
        ),
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    adapter_mock.get_data_objects.return_value = [
        DataObject(
            name=f"test_schema__test_model__{snapshot.version}__temp",
            schema="sqlmesh__test_schema",
            type=DataObjectType.VIEW,
        ),
        DataObject(
            name=f"test_schema__test_model__{snapshot.version}",
            schema="sqlmesh__test_schema",
            type=DataObjectType.VIEW,
        ),
    ]
    evaluator = SnapshotEvaluator(adapter_mock)

    evaluator.create([snapshot], {})
    adapter_mock.create_view.assert_not_called()
    adapter_mock.get_data_objects.assert_called_once_with(
        schema_("sqlmesh__test_schema"),
        {
            f"test_schema__test_model__{snapshot.version}__temp",
            f"test_schema__test_model__{snapshot.version}",
        },
    )


def test_create_only_dev_table_exists(mocker: MockerFixture, adapter_mock, make_snapshot):
    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind VIEW
            );

            SELECT a::int FROM tbl;
            """
        ),
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    adapter_mock.get_data_objects.return_value = [
        DataObject(
            name=f"test_schema__test_model__{snapshot.version}__temp",
            schema="sqlmesh__test_schema",
            type=DataObjectType.VIEW,
        ),
    ]
    evaluator = SnapshotEvaluator(adapter_mock)

    evaluator.create([snapshot], {})

    common_kwargs = dict(
        materialized=False,
        table_properties={},
        table_description=None,
    )
    rendered_query = model.render_query()
    adapter_mock.create_view.assert_has_calls(
        [
            call(
                snapshot.table_name(False),
                rendered_query,
                column_descriptions=None,
                **common_kwargs,
            ),
            call(snapshot.table_name(), rendered_query, column_descriptions={}, **common_kwargs),
        ]
    )

    adapter_mock.get_data_objects.assert_called_once_with(
        schema_("sqlmesh__test_schema"),
        {
            f"test_schema__test_model__{snapshot.version}__temp",
            f"test_schema__test_model__{snapshot.version}",
        },
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

    common_kwargs = dict(
        materialized=True,
        table_properties={},
        table_description=None,
    )

    adapter_mock.create_view.assert_has_calls(
        [
            call(
                snapshot.table_name(is_deployable=False),
                model.render_query(),
                column_descriptions=None,
                **common_kwargs,
            ),
            call(
                snapshot.table_name(), model.render_query(), column_descriptions={}, **common_kwargs
            ),
        ]
    )


def test_create_view_with_properties(mocker: MockerFixture, adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind VIEW (
                    materialized true
                ),
                table_properties (
                    "key" = 'value'
                )
            );

            SELECT a::int FROM tbl;
            """
        ),
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.create([snapshot], {})

    common_kwargs = dict(
        materialized=True,
        table_properties={
            "key": exp.convert("value"),
        },
        table_description=None,
    )

    adapter_mock.create_view.assert_has_calls(
        [
            call(
                snapshot.table_name(is_deployable=False),
                model.render_query(),
                column_descriptions=None,
                **common_kwargs,
            ),
            call(
                snapshot.table_name(), model.render_query(), column_descriptions={}, **common_kwargs
            ),
        ]
    )


def test_promote_model_info(mocker: MockerFixture, make_snapshot):
    adapter_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter")
    adapter_mock.dialect = "duckdb"

    evaluator = SnapshotEvaluator(adapter_mock)

    model = SqlModel(
        name="test_schema.test_model",
        kind=FullKind(),
        query=parse_one("SELECT a FROM tbl"),
    )

    snapshot = make_snapshot(model, version="1")
    snapshot.physical_schema_ = "physical_schema"
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.promote([snapshot], EnvironmentNamingInfo(name="test_env"))

    adapter_mock.create_schema.assert_called_once_with(to_schema("test_schema__test_env"))
    adapter_mock.create_view.assert_called_once_with(
        "test_schema__test_env.test_model",
        parse_one(f"SELECT * FROM physical_schema.test_schema__test_model__{snapshot.version}"),
        table_description=None,
        column_descriptions=None,
    )


def test_migrate(mocker: MockerFixture, make_snapshot):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    adapter = EngineAdapter(lambda: connection_mock, "")

    current_table = "sqlmesh__test_schema.test_schema__test_model__1"

    def columns(table_name):
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


@pytest.mark.parametrize(
    "change_category",
    [SnapshotChangeCategory.FORWARD_ONLY, SnapshotChangeCategory.INDIRECT_NON_BREAKING],
)
def test_migrate_view(
    mocker: MockerFixture, make_snapshot, change_category: SnapshotChangeCategory
):
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
    snapshot.change_category = change_category

    evaluator.migrate([snapshot], {})

    cursor_mock.execute.assert_has_calls(
        [
            call(
                'CREATE OR REPLACE VIEW "sqlmesh__test_schema"."test_schema__test_model__1" ("c", "a") AS SELECT "c" AS "c", "a" AS "a" FROM "tbl" AS "tbl"'
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
            ("sqlmesh__db", f"db__model__{version}__temp", "BASE TABLE"),
            ("main", "tbl", "VIEW"),
        ]

    # test that a clean run works
    evaluator.evaluate(
        snapshot,
        start="2020-01-01",
        end="2020-01-01",
        execution_time="2020-01-01",
        snapshots={},
    )
    assert_tables_exist()
    assert duck_conn.execute(f"SELECT * FROM sqlmesh__db.db__model__{version}").fetchall() == [(1,)]

    # test that existing tables work
    evaluator.evaluate(
        snapshot,
        start="2020-01-01",
        end="2020-01-01",
        execution_time="2020-01-01",
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
        start="2020-01-01",
        end="2020-01-01",
        execution_time="2020-01-01",
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
            kind=FullKind(),
            query=parse_one("SELECT a::int FROM tbl"),
        )
    )

    with pytest.raises(
        ConfigError,
        match="""Cannot audit '"db"."model"' because it has not been versioned yet. Apply a plan first.""",
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
        start="2023-01-01",
        end="2023-01-09",
        execution_time="2023-01-09",
        snapshots={},
    )

    assert adapter_mock.insert_overwrite_by_time_partition.call_args[0][1].to_dict() == output_dict


def test_create_clone_in_dev(mocker: MockerFixture, adapter_mock, make_snapshot):
    adapter_mock.SUPPORTS_CLONING = True
    evaluator = SnapshotEvaluator(adapter_mock)

    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind INCREMENTAL_BY_TIME_RANGE (
                    time_column ds
                )
            );

            SELECT 1::INT as a, ds::DATE FROM a;
            """
        ),
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
    snapshot.previous_versions = snapshot.all_versions

    evaluator.create([snapshot], {})

    adapter_mock.create_table.assert_called_once_with(
        f"sqlmesh__test_schema.test_schema__test_model__{snapshot.version}__temp__schema_migration_source",
        columns_to_types={"a": exp.DataType.build("int"), "ds": exp.DataType.build("date")},
        storage_format=None,
        partitioned_by=[exp.to_column("ds")],
        partition_interval_unit=IntervalUnit.DAY,
        clustered_by=[],
        table_properties={},
        table_description=None,
        column_descriptions=None,
    )

    adapter_mock.clone_table.assert_called_once_with(
        f"sqlmesh__test_schema.test_schema__test_model__{snapshot.version}__temp",
        f"sqlmesh__test_schema.test_schema__test_model__{snapshot.version}",
        replace=True,
    )

    adapter_mock.alter_table.assert_called_once_with(
        f"sqlmesh__test_schema.test_schema__test_model__{snapshot.version}__temp",
        f"sqlmesh__test_schema.test_schema__test_model__{snapshot.version}__temp__schema_migration_source",
    )

    adapter_mock.drop_table.assert_called_once_with(
        f"sqlmesh__test_schema.test_schema__test_model__{snapshot.version}__temp__schema_migration_source"
    )


def test_forward_only_snapshot_for_added_model(mocker: MockerFixture, adapter_mock, make_snapshot):
    adapter_mock.SUPPORTS_CLONING = False
    evaluator = SnapshotEvaluator(adapter_mock)

    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind INCREMENTAL_BY_TIME_RANGE (
                    time_column ds
                )
            );

            SELECT 1::INT as a, ds::DATE FROM a;
            """
        ),
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)

    evaluator.create([snapshot], {})

    common_create_args = dict(
        columns_to_types={"a": exp.DataType.build("int"), "ds": exp.DataType.build("date")},
        storage_format=None,
        partitioned_by=[exp.to_column("ds")],
        partition_interval_unit=IntervalUnit.DAY,
        clustered_by=[],
        table_properties={},
        table_description=None,
    )
    adapter_mock.create_table.assert_has_calls(
        [
            call(
                f"sqlmesh__test_schema.test_schema__test_model__{snapshot.version}__temp",
                column_descriptions=None,
                **common_create_args,
            ),
        ]
    )


def test_create_scd_type_2_by_time(adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)
    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind SCD_TYPE_2 (
                    unique_key [id, name],
                    time_data_type TIMESTAMPTZ
                )
            );

            SELECT id::int, name::string, updated_at::timestamp FROM tbl;
            """
        )
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.create([snapshot], {})

    common_kwargs = dict(
        columns_to_types={
            "id": exp.DataType.build("INT"),
            "name": exp.DataType.build("STRING"),
            "updated_at": exp.DataType.build("TIMESTAMPTZ"),
            # Make sure that the call includes these extra columns
            "valid_from": exp.DataType.build("TIMESTAMPTZ"),
            "valid_to": exp.DataType.build("TIMESTAMPTZ"),
        },
        storage_format=None,
        partitioned_by=[],
        partition_interval_unit=IntervalUnit.DAY,
        clustered_by=[],
        table_properties={},
        table_description=None,
    )

    adapter_mock.create_table.assert_has_calls(
        [
            call(
                snapshot.table_name(is_deployable=False), column_descriptions=None, **common_kwargs
            ),
            call(snapshot.table_name(), column_descriptions={}, **common_kwargs),
        ]
    )


def test_create_ctas_scd_type_2_by_time(adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)
    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind SCD_TYPE_2 (
                    unique_key id,
                    time_data_type TIMESTAMPTZ,
                    invalidate_hard_deletes false
                )
            );

            SELECT * FROM tbl;
            """
        )
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.create([snapshot], {})

    query = parse_one(
        """SELECT *, CAST(NULL AS TIMESTAMPTZ) AS valid_from, CAST(NULL AS TIMESTAMPTZ) AS valid_to FROM "tbl" AS "tbl" WHERE FALSE LIMIT 0"""
    )

    # Verify that managed columns are included in CTAS with types
    common_kwargs = dict(
        storage_format=None,
        partitioned_by=[],
        partition_interval_unit=IntervalUnit.DAY,
        clustered_by=[],
        table_properties={},
        table_description=None,
    )

    adapter_mock.ctas.assert_has_calls(
        [
            call(
                snapshot.table_name(is_deployable=False),
                query,
                None,
                column_descriptions=None,
                **common_kwargs,
            ),
            call(snapshot.table_name(), query, None, column_descriptions={}, **common_kwargs),
        ]
    )


def test_insert_into_scd_type_2_by_time(adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)
    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind SCD_TYPE_2 (
                    unique_key id,
                )
            );

            SELECT id::int, name::string, updated_at::timestamp FROM tbl;
            """
        )
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.evaluate(
        snapshot,
        start="2020-01-01",
        end="2020-01-02",
        execution_time="2020-01-02",
        snapshots={},
    )

    adapter_mock.scd_type_2_by_time.assert_called_once_with(
        target_table=snapshot.table_name(),
        source_table=model.render_query(),
        columns_to_types={
            "id": exp.DataType.build("INT"),
            "name": exp.DataType.build("STRING"),
            "updated_at": exp.DataType.build("TIMESTAMP"),
            # Make sure that the call includes these extra columns
            "valid_from": exp.DataType.build("TIMESTAMP"),
            "valid_to": exp.DataType.build("TIMESTAMP"),
        },
        unique_key=[exp.to_column("id")],
        valid_from_name="valid_from",
        valid_to_name="valid_to",
        updated_at_name="updated_at",
        start="2020-01-01",
        end="2020-01-02",
        execution_time="2020-01-02",
        invalidate_hard_deletes=False,
        table_description=None,
        column_descriptions={},
        updated_at_as_valid_from=False,
    )


def test_create_scd_type_2_by_column(adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)
    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind SCD_TYPE_2_BY_COLUMN (
                    unique_key id,
                    columns *,
                    invalidate_hard_deletes false,
                )
            );

            SELECT id::int, name::string, FROM tbl;
            """
        )
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.create([snapshot], {})

    common_kwargs = dict(
        columns_to_types={
            "id": exp.DataType.build("INT"),
            "name": exp.DataType.build("STRING"),
            # Make sure that the call includes these extra columns
            "valid_from": exp.DataType.build("TIMESTAMP"),
            "valid_to": exp.DataType.build("TIMESTAMP"),
        },
        storage_format=None,
        partitioned_by=[],
        partition_interval_unit=IntervalUnit.DAY,
        clustered_by=[],
        table_properties={},
        table_description=None,
    )

    adapter_mock.create_table.assert_has_calls(
        [
            call(
                snapshot.table_name(is_deployable=False),
                **{**common_kwargs, "column_descriptions": None},
            ),
            call(snapshot.table_name(), **{**common_kwargs, "column_descriptions": {}}),
        ]
    )


def test_create_ctas_scd_type_2_by_column(adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)
    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind SCD_TYPE_2_BY_COLUMN (
                    unique_key id,
                    columns *
                )
            );

            SELECT * FROM tbl;
            """
        )
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.create([snapshot], {})

    query = parse_one(
        """SELECT *, CAST(NULL AS TIMESTAMP) AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to FROM "tbl" AS "tbl" WHERE FALSE LIMIT 0"""
    )

    # Verify that managed columns are included in CTAS with types
    common_kwargs = dict(
        storage_format=None,
        partitioned_by=[],
        partition_interval_unit=IntervalUnit.DAY,
        clustered_by=[],
        table_properties={},
        table_description=None,
    )

    adapter_mock.ctas.assert_has_calls(
        [
            call(
                snapshot.table_name(is_deployable=False),
                query,
                None,
                **{**common_kwargs, "column_descriptions": None},
            ),
            call(
                snapshot.table_name(), query, None, **{**common_kwargs, "column_descriptions": {}}
            ),
        ]
    )


def test_insert_into_scd_type_2_by_column(adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)
    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind SCD_TYPE_2_BY_COLUMN (
                    unique_key id,
                    columns *
                )
            );

            SELECT id::int, name::string FROM tbl;
            """
        )
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.evaluate(
        snapshot,
        start="2020-01-01",
        end="2020-01-02",
        execution_time="2020-01-02",
        snapshots={},
    )

    adapter_mock.scd_type_2_by_column.assert_called_once_with(
        target_table=snapshot.table_name(),
        source_table=model.render_query(),
        columns_to_types={
            "id": exp.DataType.build("INT"),
            "name": exp.DataType.build("STRING"),
            # Make sure that the call includes these extra columns
            "valid_from": exp.DataType.build("TIMESTAMP"),
            "valid_to": exp.DataType.build("TIMESTAMP"),
        },
        unique_key=[exp.to_column("id")],
        check_columns=exp.Star(),
        valid_from_name="valid_from",
        valid_to_name="valid_to",
        start="2020-01-01",
        end="2020-01-02",
        execution_time="2020-01-02",
        execution_time_as_valid_from=False,
        invalidate_hard_deletes=False,
        table_description=None,
        column_descriptions={},
    )


def test_create_incremental_by_unique_key_updated_at_exp(adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)
    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind INCREMENTAL_BY_UNIQUE_KEY (
                    unique_key [id],
                    when_matched WHEN MATCHED THEN UPDATE SET target.name = source.name, target.updated_at = COALESCE(source.updated_at, target.updated_at)
                )
            );

            SELECT id::int, name::string, updated_at::timestamp FROM tbl;
            """
        )
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator.evaluate(
        snapshot,
        start="2020-01-01",
        end="2020-01-02",
        execution_time="2020-01-02",
        snapshots={},
    )

    adapter_mock.merge.assert_called_once_with(
        snapshot.table_name(),
        model.render_query(),
        columns_to_types={
            "id": exp.DataType.build("INT"),
            "name": exp.DataType.build("STRING"),
            "updated_at": exp.DataType.build("TIMESTAMP"),
        },
        unique_key=[exp.to_column("id")],
        when_matched=exp.When(
            matched=True,
            source=False,
            then=exp.Update(
                expressions=[
                    exp.column("name", MERGE_TARGET_ALIAS).eq(
                        exp.column("name", MERGE_SOURCE_ALIAS)
                    ),
                    exp.column("updated_at", MERGE_TARGET_ALIAS).eq(
                        exp.Coalesce(
                            this=exp.column("updated_at", MERGE_SOURCE_ALIAS),
                            expressions=[exp.column("updated_at", MERGE_TARGET_ALIAS)],
                        )
                    ),
                ],
            ),
        ),
    )


def test_standalone_audit(mocker: MockerFixture, adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    audit = StandaloneAudit(name="test_standalone_audit", query=parse_one("SELECT NULL LIMIT 0"))

    snapshot = make_snapshot(audit)
    snapshot.categorize_as(SnapshotChangeCategory.NON_BREAKING)

    # Create
    evaluator.create([snapshot], {})

    adapter_mock.assert_not_called()
    adapter_mock.transaction.assert_not_called()
    adapter_mock.session.assert_not_called()

    # Evaluate
    payload = {"calls": 0}
    evaluator.evaluate(
        snapshot,
        start="2020-01-01",
        end="2020-01-02",
        execution_time="2020-01-02",
        snapshots={},
        payload=payload,
    )

    adapter_mock.assert_not_called()
    adapter_mock.transaction.assert_not_called()
    adapter_mock.session.assert_not_called()
    assert payload["calls"] == 0

    # Audit
    adapter_mock.fetchone.return_value = (0,)
    evaluator.audit(snapshot=snapshot, snapshots={})

    query = audit.render_query(snapshot)
    adapter_mock.fetchone.assert_called_once_with(
        select("COUNT(*)").from_(query.subquery("audit")), quote_identifiers=True
    )

    # Promote
    adapter_mock.reset_mock()

    evaluator.promote([snapshot], EnvironmentNamingInfo(name="test_env"))

    adapter_mock.assert_not_called()
    adapter_mock.transaction.assert_not_called()
    adapter_mock.session.assert_not_called()

    # Migrate
    evaluator.migrate([snapshot], snapshots={})

    adapter_mock.assert_not_called()
    adapter_mock.transaction.assert_not_called()
    adapter_mock.session.assert_not_called()

    # Demote
    evaluator.demote([snapshot], EnvironmentNamingInfo(name="test_env"))

    adapter_mock.assert_not_called()
    adapter_mock.transaction.assert_not_called()
    adapter_mock.session.assert_not_called()

    # Cleanup
    evaluator.cleanup(
        [SnapshotTableCleanupTask(snapshot=snapshot.table_info, dev_table_only=False)]
    )

    adapter_mock.assert_not_called()
    adapter_mock.transaction.assert_not_called()
    adapter_mock.session.assert_not_called()


def test_audit_wap(adapter_mock, make_snapshot):
    evaluator = SnapshotEvaluator(adapter_mock)

    custom_audit = ModelAudit(
        name="test_audit",
        query="SELECT * FROM test_schema.test_table WHERE 1 = 2",
    )

    model = SqlModel(
        name="test_schema.test_table",
        kind=FullKind(),
        query=parse_one("SELECT a::int FROM tbl"),
        audits=[
            ("not_null", {"columns": exp.to_column("a")}),
            ("test_audit", {}),
        ],
    )
    snapshot = make_snapshot(model, audits={custom_audit.name: custom_audit})
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    wap_id = "test_wap_id"
    expected_table_name = f"spark_catalog.test_schema.test_table.branch_wap_{wap_id}"
    adapter_mock.wap_table_name.return_value = expected_table_name
    adapter_mock.fetchone.return_value = (0,)

    evaluator.audit(snapshot, snapshots={}, wap_id=wap_id)

    call_args = adapter_mock.fetchone.call_args_list
    assert len(call_args) == 2

    not_null_query = call_args[0][0][0]
    assert (
        not_null_query.sql(dialect="spark")
        == "SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM `spark_catalog`.`test_schema`.`test_table`.`branch_wap_test_wap_id` AS `branch_wap_test_wap_id`) AS `_q_0` WHERE `a` IS NULL) AS audit"
    )

    custom_audit_query = call_args[1][0][0]
    assert (
        custom_audit_query.sql(dialect="spark")
        == "SELECT COUNT(*) FROM (SELECT * FROM `spark_catalog`.`test_schema`.`test_table`.`branch_wap_test_wap_id` AS `test_table` /* test_schema.test_table */ WHERE 1 = 2) AS audit"
    )

    adapter_mock.wap_table_name.assert_called_once_with(snapshot.table_name(), wap_id)
    adapter_mock.wap_publish.assert_called_once_with(snapshot.table_name(), wap_id)
