import typing as t
import pytest
from unittest.mock import Mock
from pytest_mock import MockerFixture
import pandas as pd

from sqlglot import exp, parse_one
import sqlmesh.core.dialect as d
from sqlmesh.core.engine_adapter import AthenaEngineAdapter
from sqlmesh.core.model import load_sql_based_model
from sqlmesh.core.model.definition import SqlModel
from sqlmesh.utils.errors import SQLMeshError

from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.athena, pytest.mark.engine]


@pytest.fixture
def adapter(make_mocked_engine_adapter: t.Callable) -> AthenaEngineAdapter:
    return make_mocked_engine_adapter(AthenaEngineAdapter)


@pytest.mark.parametrize(
    "config_s3_warehouse_location,table_properties,table,expected_location",
    [
        # No s3_warehouse_location in config
        (None, None, exp.to_table("schema.table"), None),
        (None, {}, exp.to_table("schema.table"), None),
        (
            None,
            {"s3_base_location": exp.Literal.string("s3://some/location/")},
            exp.to_table("schema.table"),
            "s3://some/location/table/",
        ),
        # Location set to bucket
        ("s3://bucket", None, exp.to_table("schema.table"), "s3://bucket/schema/table/"),
        ("s3://bucket", {}, exp.to_table("schema.table"), "s3://bucket/schema/table/"),
        ("s3://bucket", None, exp.to_table("schema.table"), "s3://bucket/schema/table/"),
        (
            "s3://bucket",
            {"s3_base_location": exp.Literal.string("s3://some/location/")},
            exp.to_table("schema.table"),
            "s3://some/location/table/",
        ),
        ("s3://bucket", {}, exp.Table(db=exp.Identifier(this="test")), "s3://bucket/test/"),
        # Location set to bucket with prefix
        (
            "s3://bucket/subpath/",
            None,
            exp.to_table("schema.table"),
            "s3://bucket/subpath/schema/table/",
        ),
        ("s3://bucket/subpath/", None, exp.to_table("table"), "s3://bucket/subpath/table/"),
        (
            "s3://bucket/subpath/",
            None,
            exp.to_table("catalog.schema.table"),
            "s3://bucket/subpath/catalog/schema/table/",
        ),
        (
            "s3://bucket/subpath/",
            None,
            exp.Table(db=exp.Identifier(this="test")),
            "s3://bucket/subpath/test/",
        ),
    ],
)
def test_table_location(
    adapter: AthenaEngineAdapter,
    config_s3_warehouse_location: t.Optional[str],
    table_properties: t.Optional[t.Dict[str, exp.Expression]],
    table: exp.Table,
    expected_location: t.Optional[str],
) -> None:
    adapter.s3_warehouse_location = config_s3_warehouse_location
    if expected_location is None:
        with pytest.raises(SQLMeshError, match=r"Cannot figure out location for table.*"):
            adapter._table_location_or_raise(table_properties, table)
    else:
        location = adapter._table_location_or_raise(
            table_properties, table
        ).this.name  # extract the unquoted location value from the LocationProperty
        assert location == expected_location

    if table_properties is not None:
        # this get consumed by _table_location because we dont want it to end up in a TBLPROPERTIES clause
        assert "s3_base_location" not in table_properties


def test_create_schema(adapter: AthenaEngineAdapter) -> None:
    adapter.create_schema("test")

    adapter.s3_warehouse_location = "s3://base"
    adapter.create_schema("test")

    assert to_sql_calls(adapter) == [
        "CREATE SCHEMA IF NOT EXISTS `test`",
        "CREATE SCHEMA IF NOT EXISTS `test` LOCATION 's3://base/test/'",
    ]


def test_create_table_hive(adapter: AthenaEngineAdapter) -> None:
    expressions = d.parse(
        """
        MODEL (
            name test_table,
            kind FULL,
            partitioned_by (cola, colb),
            storage_format parquet,
            physical_properties (
                s3_base_location = 's3://foo',
                has_encrypted_data = 'true'
            )
        );

        SELECT 1::timestamp AS cola, 2::varchar as colb, 'foo' as colc;
    """
    )
    model: SqlModel = t.cast(SqlModel, load_sql_based_model(expressions))

    adapter.create_table(
        model.name,
        columns_to_types=model.columns_to_types_or_raise,
        table_properties=model.physical_properties,
        partitioned_by=model.partitioned_by,
        storage_format=model.storage_format,
    )

    assert to_sql_calls(adapter) == [
        "CREATE EXTERNAL TABLE IF NOT EXISTS `test_table` (`colc` STRING) PARTITIONED BY (`cola` TIMESTAMP, `colb` STRING) STORED AS PARQUET LOCATION 's3://foo/test_table/' TBLPROPERTIES ('has_encrypted_data'='true')"
    ]


def test_create_table_iceberg(adapter: AthenaEngineAdapter) -> None:
    expressions = d.parse(
        """
        MODEL (
            name test_table,
            kind FULL,
            partitioned_by (colc, bucket(16, cola)),
            table_format iceberg,
            storage_format parquet,
            physical_properties (
                s3_base_location = 's3://foo'
            )
        );

        SELECT 1::timestamp AS cola, 2::varchar as colb, 'foo' as colc;
    """
    )
    model: SqlModel = t.cast(SqlModel, load_sql_based_model(expressions))

    adapter.create_table(
        model.name,
        columns_to_types=model.columns_to_types_or_raise,
        table_properties=model.physical_properties,
        partitioned_by=model.partitioned_by,
        table_format=model.table_format,
        storage_format=model.storage_format,
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`cola` TIMESTAMP, `colb` STRING, `colc` STRING) PARTITIONED BY (`colc`, BUCKET(16, `cola`)) LOCATION 's3://foo/test_table/' TBLPROPERTIES ('table_type'='iceberg', 'format'='parquet')"
    ]


def test_create_table_no_location(adapter: AthenaEngineAdapter) -> None:
    expressions = d.parse(
        """
        MODEL (
            name test_table,
            kind FULL
        );

        SELECT a::int FROM foo;
    """
    )
    model: SqlModel = t.cast(SqlModel, load_sql_based_model(expressions))

    with pytest.raises(SQLMeshError, match=r"Cannot figure out location.*"):
        adapter.create_table(
            model.name,
            columns_to_types=model.columns_to_types_or_raise,
            table_properties=model.physical_properties,
        )

    adapter.s3_warehouse_location = "s3://bucket/prefix"
    adapter.create_table(
        model.name,
        columns_to_types=model.columns_to_types_or_raise,
        table_properties=model.physical_properties,
    )

    assert to_sql_calls(adapter) == [
        "CREATE EXTERNAL TABLE IF NOT EXISTS `test_table` (`a` INT) LOCATION 's3://bucket/prefix/test_table/'",
    ]


def test_ctas_hive(adapter: AthenaEngineAdapter):
    adapter.s3_warehouse_location = "s3://bucket/prefix/"

    adapter.ctas(
        table_name="foo.bar",
        columns_to_types={"a": exp.DataType.build("int")},
        query_or_df=parse_one("select 1", into=exp.Select),
    )

    assert to_sql_calls(adapter) == [
        'CREATE TABLE IF NOT EXISTS "foo"."bar" WITH (external_location=\'s3://bucket/prefix/foo/bar/\') AS SELECT CAST("a" AS INTEGER) AS "a" FROM (SELECT 1) AS "_subquery"'
    ]


def test_ctas_iceberg(adapter: AthenaEngineAdapter):
    adapter.s3_warehouse_location = "s3://bucket/prefix/"

    adapter.ctas(
        table_name="foo.bar",
        columns_to_types={"a": exp.DataType.build("int")},
        query_or_df=parse_one("select 1", into=exp.Select),
        table_format="iceberg",
    )

    assert to_sql_calls(adapter) == [
        'CREATE TABLE IF NOT EXISTS "foo"."bar" WITH (table_type=\'iceberg\', location=\'s3://bucket/prefix/foo/bar/\', is_external=false) AS SELECT CAST("a" AS INTEGER) AS "a" FROM (SELECT 1) AS "_subquery"'
    ]


def test_ctas_iceberg_no_specific_location(adapter: AthenaEngineAdapter):
    with pytest.raises(SQLMeshError, match=r"Cannot figure out location.*"):
        adapter.ctas(
            table_name="foo.bar",
            columns_to_types={"a": exp.DataType.build("int")},
            query_or_df=parse_one("select 1", into=exp.Select),
            table_properties={"table_type": exp.Literal.string("iceberg")},
        )

    assert to_sql_calls(adapter) == []


def test_replace_query(adapter: AthenaEngineAdapter, mocker: MockerFixture):
    mocker.patch(
        "sqlmesh.core.engine_adapter.athena.AthenaEngineAdapter.table_exists", return_value=True
    )
    mocker.patch(
        "sqlmesh.core.engine_adapter.athena.AthenaEngineAdapter._query_table_type",
        return_value="iceberg",
    )

    adapter.replace_query(
        table_name="test",
        query_or_df=parse_one("select 1 as a", into=exp.Select),
        columns_to_types={"a": exp.DataType.build("int")},
        table_properties={},
    )

    assert to_sql_calls(adapter) == [
        'DELETE FROM "test" WHERE TRUE',
        'INSERT INTO "test" ("a") SELECT 1 AS "a"',
    ]

    mocker.patch(
        "sqlmesh.core.engine_adapter.athena.AthenaEngineAdapter.table_exists", return_value=False
    )
    adapter.cursor.execute.reset_mock()

    adapter.s3_warehouse_location = "s3://foo"
    adapter.replace_query(
        table_name="test",
        query_or_df=parse_one("select 1 as a", into=exp.Select),
        columns_to_types={"a": exp.DataType.build("int")},
        table_properties={},
    )

    # gets recreated as a Hive table because table_exists=False and nothing in the properties indicates it should be Iceberg
    assert to_sql_calls(adapter) == [
        'CREATE TABLE IF NOT EXISTS "test" WITH (external_location=\'s3://foo/test/\') AS SELECT CAST("a" AS INTEGER) AS "a" FROM (SELECT 1 AS "a") AS "_subquery"'
    ]


def test_columns(adapter: AthenaEngineAdapter, mocker: MockerFixture):
    mock = mocker.patch(
        "pandas.io.sql.read_sql_query",
        return_value=pd.DataFrame(
            data=[["col1", "int"], ["col2", "varchar"]], columns=["column_name", "data_type"]
        ),
    )

    assert adapter.columns("foo.bar") == {
        "col1": exp.DataType.build("int"),
        "col2": exp.DataType.build("varchar"),
    }

    assert (
        mock.call_args_list[0][0][0]
        == """SELECT "column_name", "data_type" FROM "information_schema"."columns" WHERE "table_schema" = 'foo' AND "table_name" = 'bar' ORDER BY "ordinal_position" NULLS FIRST"""
    )


def test_truncate_table_iceberg(adapter: AthenaEngineAdapter, mocker: MockerFixture):
    mocker.patch.object(
        adapter,
        "_query_table_type",
        return_value="iceberg",
    )
    mocker.patch.multiple(
        adapter, _clear_partition_data=mocker.DEFAULT, _clear_s3_location=mocker.DEFAULT
    )
    adapter._truncate_table(exp.to_table("foo.bar"))

    assert to_sql_calls(adapter) == ['DELETE FROM "foo"."bar" WHERE TRUE']
    t.cast(Mock, adapter._clear_partition_data).assert_not_called()
    t.cast(Mock, adapter._clear_s3_location).assert_not_called()


def test_truncate_table_hive(adapter: AthenaEngineAdapter, mocker: MockerFixture):
    mocker.patch.object(
        adapter,
        "_query_table_type",
        return_value="hive",
    )
    mocker.patch.object(
        adapter,
        "_is_hive_partitioned_table",
        return_value=False,
    )
    mocker.patch.object(adapter, "_query_table_s3_location", return_value="s3://foo/bar")
    mocker.patch.multiple(
        adapter, _clear_partition_data=mocker.DEFAULT, _clear_s3_location=mocker.DEFAULT
    )

    adapter._truncate_table(exp.to_table("foo.bar"))

    assert to_sql_calls(adapter) == []
    t.cast(Mock, adapter._clear_partition_data).assert_not_called()
    t.cast(Mock, adapter._clear_s3_location).assert_called_with("s3://foo/bar")


def test_truncate_table_hive_partitioned(adapter: AthenaEngineAdapter, mocker: MockerFixture):
    mocker.patch.object(
        adapter,
        "_query_table_type",
        return_value="hive",
    )
    mocker.patch.object(
        adapter,
        "_is_hive_partitioned_table",
        return_value=True,
    )
    mocker.patch.object(adapter, "_clear_partition_data")
    mocker.patch.object(adapter, "_clear_s3_location")
    adapter._truncate_table(exp.to_table("foo.bar"))

    assert to_sql_calls(adapter) == []
    t.cast(Mock, adapter._clear_partition_data).assert_called_with(
        exp.to_table("foo.bar"), exp.true()
    )
    t.cast(Mock, adapter._clear_s3_location).assert_not_called()


def test_create_state_table(adapter: AthenaEngineAdapter):
    adapter.s3_warehouse_location = "s3://base"
    adapter.create_state_table("_snapshots", {"name": exp.DataType.build("varchar")})

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `_snapshots` (`name` STRING) LOCATION 's3://base/_snapshots/' TBLPROPERTIES ('table_type'='iceberg')"
    ]
