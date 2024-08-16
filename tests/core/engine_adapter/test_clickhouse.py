import pytest
from sqlmesh.core.engine_adapter import ClickhouseEngineAdapter
from sqlmesh.core.model.definition import load_sql_based_model
from sqlmesh.core.model.kind import ModelKindName
from sqlmesh.core.engine_adapter.shared import EngineRunMode
from tests.core.engine_adapter import to_sql_calls
from sqlmesh.core.dialect import parse

pytestmark = [pytest.mark.clickhouse, pytest.mark.engine]


@pytest.fixture
def adapter(make_mocked_engine_adapter, mocker) -> ClickhouseEngineAdapter:
    mocker.patch.object(
        ClickhouseEngineAdapter, "engine_run_mode", new_callable=mocker.PropertyMock(return_value=EngineRunMode.STANDALONE)
    )
    # mocker.patch.object(
    #     EngineRunMode, "is_cluster", new_callable=mocker.PropertyMock(return_value=False)
    # )
    # mocker.patch.object(
    #     EngineRunMode,
    #     "is_standalone",
    #     new_callable=mocker.PropertyMock(return_value=True),
    # )

    adapter = make_mocked_engine_adapter(ClickhouseEngineAdapter)

    return adapter


def test_create_schema(adapter: ClickhouseEngineAdapter, mocker):
    mocker.patch.object(
        ClickhouseEngineAdapter,
        "default_cluster",
        new_callable=mocker.PropertyMock(return_value="default"),
    )

    # ON CLUSTER not added because is_cluster=False
    adapter.create_schema("foo")

    mocker.patch.object(
        ClickhouseEngineAdapter, "engine_run_mode", new_callable=mocker.PropertyMock(return_value=EngineRunMode.CLUSTER)
    )
    adapter.create_schema("foo")

    assert to_sql_calls(adapter) == [
        'CREATE DATABASE IF NOT EXISTS "foo"',
        'CREATE DATABASE IF NOT EXISTS "foo" ON CLUSTER "default"',
    ]


def test_drop_schema(adapter: ClickhouseEngineAdapter, mocker):
    mocker.patch.object(
        ClickhouseEngineAdapter,
        "default_cluster",
        new_callable=mocker.PropertyMock(return_value="default"),
    )

    adapter.drop_schema("foo")

    mocker.patch.object(
        ClickhouseEngineAdapter, "engine_run_mode", new_callable=mocker.PropertyMock(return_value=EngineRunMode.CLUSTER)
    )
    adapter.drop_schema("foo")

    assert to_sql_calls(adapter) == [
        'DROP DATABASE IF EXISTS "foo"',
        'DROP DATABASE IF EXISTS "foo" ON CLUSTER "default"',
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


def test_nullable_datatypes_in_model_query(adapter: ClickhouseEngineAdapter):
    model = load_sql_based_model(
        parse(
            """
        MODEL (
            name foo
        );

        select
            id::Int64,
            data::Nullable(JSON),
            ts::DateTime64,
            other::Tuple(Uint16, String)
        from bar
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
