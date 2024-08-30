import pytest
from sqlmesh.core.engine_adapter import ClickhouseEngineAdapter
from sqlmesh.core.model.definition import load_sql_based_model
from sqlmesh.core.model.kind import ModelKindName
from sqlmesh.core.engine_adapter.shared import EngineRunMode
from tests.core.engine_adapter import to_sql_calls
from sqlmesh.core.dialect import parse
from sqlglot import exp
import typing as t
from sqlmesh.core.schema_diff import SchemaDiffer

pytestmark = [pytest.mark.clickhouse, pytest.mark.engine]


@pytest.fixture
def adapter(make_mocked_engine_adapter, mocker) -> ClickhouseEngineAdapter:
    mocker.patch.object(
        ClickhouseEngineAdapter,
        "engine_run_mode",
        new_callable=mocker.PropertyMock(return_value=EngineRunMode.STANDALONE),
    )

    adapter = make_mocked_engine_adapter(ClickhouseEngineAdapter)

    return adapter


def test_create_schema(adapter: ClickhouseEngineAdapter, mocker):
    mocker.patch.object(
        ClickhouseEngineAdapter,
        "cluster",
        new_callable=mocker.PropertyMock(return_value="default"),
    )

    # ON CLUSTER not added because engine_run_mode.is_cluster=False
    adapter.create_schema("foo")

    mocker.patch.object(
        ClickhouseEngineAdapter,
        "engine_run_mode",
        new_callable=mocker.PropertyMock(return_value=EngineRunMode.CLUSTER),
    )
    adapter.create_schema("foo")

    assert to_sql_calls(adapter) == [
        'CREATE DATABASE IF NOT EXISTS "foo"',
        'CREATE DATABASE IF NOT EXISTS "foo" ON CLUSTER "default"',
    ]


def test_drop_schema(adapter: ClickhouseEngineAdapter, mocker):
    mocker.patch.object(
        ClickhouseEngineAdapter,
        "cluster",
        new_callable=mocker.PropertyMock(return_value="default"),
    )

    # ON CLUSTER not added because engine_run_mode.is_cluster=False
    adapter.drop_schema("foo")

    mocker.patch.object(
        ClickhouseEngineAdapter,
        "engine_run_mode",
        new_callable=mocker.PropertyMock(return_value=EngineRunMode.CLUSTER),
    )
    adapter.drop_schema("foo")

    assert to_sql_calls(adapter) == [
        'DROP DATABASE IF EXISTS "foo"',
        'DROP DATABASE IF EXISTS "foo" ON CLUSTER "default"',
    ]


def test_create_table(adapter: ClickhouseEngineAdapter, mocker):
    mocker.patch.object(
        ClickhouseEngineAdapter,
        "cluster",
        new_callable=mocker.PropertyMock(return_value="default"),
    )

    # ON CLUSTER not added because engine_run_mode.is_cluster=False
    adapter.create_table("foo", {"a": exp.DataType.build("Int8", dialect=adapter.dialect)})
    # adapter.create_table_like("target", "source")

    mocker.patch.object(
        ClickhouseEngineAdapter,
        "engine_run_mode",
        new_callable=mocker.PropertyMock(return_value=EngineRunMode.CLUSTER),
    )
    adapter.create_table("foo", {"a": exp.DataType.build("Int8", dialect=adapter.dialect)})
    # adapter.create_table_like("target", "source")

    assert to_sql_calls(adapter) == [
        'CREATE TABLE IF NOT EXISTS "foo" ("a" Int8) ENGINE=MergeTree ORDER BY ()',
        # "CREATE TABLE IF NOT EXISTS target AS source",
        'CREATE TABLE IF NOT EXISTS "foo" ON CLUSTER "default" ("a" Int8) ENGINE=MergeTree ORDER BY ()',
        # "CREATE TABLE IF NOT EXISTS target AS source",
    ]


def test_rename_table(adapter: ClickhouseEngineAdapter, mocker):
    mocker.patch.object(
        ClickhouseEngineAdapter,
        "cluster",
        new_callable=mocker.PropertyMock(return_value="default"),
    )

    # ON CLUSTER not added because engine_run_mode.is_cluster=False
    adapter.rename_table(exp.to_table("foo"), exp.to_table("bar"))

    mocker.patch.object(
        ClickhouseEngineAdapter,
        "engine_run_mode",
        new_callable=mocker.PropertyMock(return_value=EngineRunMode.CLUSTER),
    )
    adapter.rename_table(exp.to_table("foo"), exp.to_table("bar"))

    assert to_sql_calls(adapter) == [
        'RENAME TABLE "foo" TO "bar"',
        'RENAME TABLE "foo" TO "bar" ON CLUSTER "default" ',
    ]


def test_alter_table(
    adapter: ClickhouseEngineAdapter,
    mocker,
):
    adapter.SCHEMA_DIFFER = SchemaDiffer()
    current_table_name = "test_table"
    current_table = {"a": "Int8", "b": "String", "c": "Int8"}
    target_table_name = "target_table"
    target_table = {
        "a": "Int8",
        "b": "String",
        "f": "String",
    }

    def table_columns(table_name: str) -> t.Dict[str, exp.DataType]:
        if table_name == current_table_name:
            return {
                k: exp.DataType.build(v, dialect=adapter.dialect) for k, v in current_table.items()
            }
        else:
            return {
                k: exp.DataType.build(v, dialect=adapter.dialect) for k, v in target_table.items()
            }

    adapter.columns = table_columns  # type: ignore

    # ON CLUSTER not added because engine_run_mode.is_cluster=False
    adapter.alter_table(adapter.get_alter_expressions(current_table_name, target_table_name))

    mocker.patch.object(
        ClickhouseEngineAdapter,
        "cluster",
        new_callable=mocker.PropertyMock(return_value="default"),
    )
    mocker.patch.object(
        ClickhouseEngineAdapter,
        "engine_run_mode",
        new_callable=mocker.PropertyMock(return_value=EngineRunMode.CLUSTER),
    )

    adapter.alter_table(adapter.get_alter_expressions(current_table_name, target_table_name))

    assert to_sql_calls(adapter) == [
        'ALTER TABLE "test_table" DROP COLUMN "c"',
        'ALTER TABLE "test_table" ADD COLUMN "f" String',
        'ALTER TABLE "test_table" ON CLUSTER "default" DROP COLUMN "c"',
        'ALTER TABLE "test_table" ON CLUSTER "default" ADD COLUMN "f" String',
    ]


def test_nullable_datatypes_in_model_kind(adapter: ClickhouseEngineAdapter):
    model = load_sql_based_model(
        parse(
            """
        MODEL (
            name foo,
            kind SCD_TYPE_2_BY_TIME(unique_key id, time_data_type Nullable(DateTime64)),
        );

        select 1;
    """,
            default_dialect="clickhouse",
        )
    )

    assert model.kind.name == ModelKindName.SCD_TYPE_2_BY_TIME
    assert model.kind.time_data_type.sql(dialect="clickhouse") == "Nullable(DateTime64)"


def test_nullable_datatypes_in_model_columns(adapter: ClickhouseEngineAdapter):
    model = load_sql_based_model(
        parse(
            """
        MODEL (
            name foo,
            columns (
                id Int64,
                data Nullable(JSON),
                ts DateTime64,
                other Tuple(UInt16, String)
            )
        );

        select 1, 2, 3, 4;
    """,
            default_dialect="clickhouse",
        )
    )

    rendered_columns_to_types = {
        k: v.sql(dialect="clickhouse") for k, v in model.columns_to_types_or_raise.items()
    }

    assert rendered_columns_to_types["id"] == "Int64"
    assert rendered_columns_to_types["data"] == "Nullable(JSON)"
    assert rendered_columns_to_types["ts"] == "DateTime64"
    assert rendered_columns_to_types["other"] == "Tuple(UInt16, String)"


def test_model_properties(adapter: ClickhouseEngineAdapter):
    def build_properties_sql(storage_format="", order_by="", primary_key=""):
        model = load_sql_based_model(
            parse(
                f"""
        MODEL (
            name foo,
            {storage_format}
            physical_properties (
              {order_by}
              {primary_key}
            ),
        );

        select
            *
        from bar;
    """,
                default_dialect="clickhouse",
            )
        )

        return adapter._build_table_properties_exp(
            storage_format=model.storage_format, table_properties=model.physical_properties
        ).sql("clickhouse")

    # no order by or primary key because table engine is not part of "MergeTree" engine family
    assert (
        build_properties_sql(
            storage_format="storage_format Log,",
            order_by="ORDER_BY = a,",
            primary_key="PRIMARY_KEY = a,",
        )
        == "ENGINE=Log"
    )

    assert (
        build_properties_sql(
            storage_format="storage_format ReplicatedMergeTree,",
            order_by="ORDER_BY = a,",
            primary_key="PRIMARY_KEY = a,",
        )
        == "ENGINE=ReplicatedMergeTree ORDER BY (a) PRIMARY KEY (a)"
    )

    assert (
        build_properties_sql(order_by="ORDER_BY = a,", primary_key="PRIMARY_KEY = a,")
        == "ENGINE=MergeTree ORDER BY (a) PRIMARY KEY (a)"
    )

    assert (
        build_properties_sql(order_by='ORDER_BY = "a",', primary_key='PRIMARY_KEY = "a",')
        == 'ENGINE=MergeTree ORDER BY ("a") PRIMARY KEY ("a")'
    )

    assert (
        build_properties_sql(order_by="ORDER_BY = (a),", primary_key="PRIMARY_KEY = (a)")
        == "ENGINE=MergeTree ORDER BY (a) PRIMARY KEY (a)"
    )

    assert build_properties_sql(order_by="ORDER_BY = a + 1,") == "ENGINE=MergeTree ORDER BY (a + 1)"

    assert (
        build_properties_sql(order_by="ORDER_BY = (a + 1),") == "ENGINE=MergeTree ORDER BY (a + 1)"
    )

    assert (
        build_properties_sql(order_by="ORDER_BY = (a, b + 1),", primary_key="PRIMARY_KEY = (a, b)")
        == "ENGINE=MergeTree ORDER BY (a, b + 1) PRIMARY KEY (a, b)"
    )

    assert (
        build_properties_sql(
            order_by="ORDER_BY = 'timestamp with fill to toStartOfDay(toDateTime64(\\'2024-07-11\\', 3)) step toIntervalDay(1) interpolate(price as price)',"
        )
        == "ENGINE=MergeTree ORDER BY (timestamp WITH FILL TO toStartOfDay(toDateTime64('2024-07-11', 3)) STEP toIntervalDay(1) INTERPOLATE (price AS price))"
    )

    assert (
        build_properties_sql(
            order_by="ORDER_BY = (\"a\", 'timestamp with fill to toStartOfDay(toDateTime64(\\'2024-07-11\\', 3)) step toIntervalDay(1) interpolate(price as price)'),"
        )
        == "ENGINE=MergeTree ORDER BY (\"a\", timestamp WITH FILL TO toStartOfDay(toDateTime64('2024-07-11', 3)) STEP toIntervalDay(1) INTERPOLATE (price AS price))"
    )


def test_create_table_properties(make_mocked_engine_adapter: t.Callable, mocker):
    adapter = make_mocked_engine_adapter(ClickhouseEngineAdapter)

    mocker.patch(
        "sqlmesh.core.engine_adapter.ClickhouseEngineAdapter.fetchone",
        return_value="1",
    )

    columns_to_types = {
        "cola": exp.DataType.build("INT", dialect="clickhouse"),
        "colb": exp.DataType.build("TEXT", dialect="clickhouse"),
        "colc": exp.DataType.build("TEXT", dialect="clickhouse"),
    }
    adapter.create_table(
        "test_table",
        columns_to_types,
        partitioned_by=[exp.to_column("colb")],
        storage_format="ReplicatedMergeTree",
        table_properties={
            "ORDER_BY": [exp.to_column("cola"), exp.to_column("colb")],
            "PRIMARY_KEY": [exp.to_column("cola"), exp.to_column("colb")],
        },
    )

    assert to_sql_calls(adapter) == [
        'CREATE TABLE IF NOT EXISTS "test_table" ("cola" Int32, "colb" String, "colc" String) ENGINE=ReplicatedMergeTree ORDER BY ("cola", "colb") PRIMARY KEY ("cola", "colb") PARTITION BY ("colb")',
    ]
