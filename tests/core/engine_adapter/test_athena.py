import typing as t
import pytest
from pytest_mock import MockerFixture
import pandas as pd

from sqlglot import exp, parse_one
import sqlmesh.core.dialect as d
from sqlmesh.core.engine_adapter import AthenaEngineAdapter
from sqlmesh.core.model import load_sql_based_model
from sqlmesh.core.model.definition import SqlModel

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
        (None, None, exp.Table(db=exp.Identifier(this="test")), None),
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
    location = adapter._table_location(table_properties, table)
    final_location = None

    if location and expected_location:
        final_location = (
            location.this.name
        )  # extract the unquoted location value from the LocationProperty

    assert final_location == expected_location

    if table_properties is not None:
        assert "location" not in table_properties


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
            storage_format parquet,
            physical_properties (
                table_type = 'iceberg',
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
        storage_format=model.storage_format,
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`cola` TIMESTAMP, `colb` STRING, `colc` STRING) PARTITIONED BY (`colc`, BUCKET(16, `cola`)) LOCATION 's3://foo/test_table/' TBLPROPERTIES ('table_type'='iceberg', 'format'='parquet')"
    ]


def test_create_table_inferred_location(adapter: AthenaEngineAdapter) -> None:
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
        "CREATE EXTERNAL TABLE IF NOT EXISTS `test_table` (`a` INT)",
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
        table_properties={"table_type": exp.Literal.string("iceberg")},
    )

    assert to_sql_calls(adapter) == [
        'CREATE TABLE IF NOT EXISTS "foo"."bar" WITH (location=\'s3://bucket/prefix/foo/bar/\', table_type=\'iceberg\', is_external=false) AS SELECT CAST("a" AS INTEGER) AS "a" FROM (SELECT 1) AS "_subquery"'
    ]


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

    adapter.replace_query(
        table_name="test",
        query_or_df=parse_one("select 1 as a", into=exp.Select),
        columns_to_types={"a": exp.DataType.build("int")},
        table_properties={},
    )

    assert to_sql_calls(adapter) == [
        'CREATE TABLE IF NOT EXISTS "test" AS SELECT CAST("a" AS INTEGER) AS "a" FROM (SELECT 1 AS "a") AS "_subquery"'
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


def test_truncate_table(adapter: AthenaEngineAdapter):
    adapter._truncate_table(exp.to_table("foo.bar"))

    assert to_sql_calls(adapter) == ['DELETE FROM "foo"."bar"']


def test_create_state_table(adapter: AthenaEngineAdapter):
    adapter.create_state_table("_snapshots", {"name": exp.DataType.build("varchar")})

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `_snapshots` (`name` STRING) TBLPROPERTIES ('table_type'='iceberg')"
    ]
