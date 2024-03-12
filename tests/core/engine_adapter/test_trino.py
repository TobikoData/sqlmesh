import typing as t

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter import TrinoEngineAdapter
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.trino]


@pytest.fixture
def trino_mocked_engine_adapter(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
) -> TrinoEngineAdapter:
    def mock_catalog_type(catalog_name):
        if "iceberg" in catalog_name:
            return "iceberg"
        return "hive"

    mocker.patch(
        "sqlmesh.core.engine_adapter.trino.TrinoEngineAdapter.get_catalog_type",
        side_effect=mock_catalog_type,
    )

    return make_mocked_engine_adapter(TrinoEngineAdapter)


def test_set_current_catalog(trino_mocked_engine_adapter: TrinoEngineAdapter):
    adapter = trino_mocked_engine_adapter
    adapter.set_current_catalog("test_catalog")

    assert to_sql_calls(adapter) == [
        'USE "test_catalog"."information_schema"',
    ]


def test_get_catalog_type(trino_mocked_engine_adapter: TrinoEngineAdapter, mocker: MockerFixture):
    adapter = trino_mocked_engine_adapter
    mocker.patch(
        "sqlmesh.core.engine_adapter.trino.TrinoEngineAdapter.get_current_catalog",
        return_value="system",
    )

    assert adapter.current_catalog_type == "hive"
    assert adapter.get_catalog_type("foo") == TrinoEngineAdapter.DEFAULT_CATALOG_TYPE
    assert adapter.get_catalog_type("datalake_hive") == "hive"
    assert adapter.get_catalog_type("datalake_iceberg") == "iceberg"

    mocker.patch(
        "sqlmesh.core.engine_adapter.trino.TrinoEngineAdapter.get_current_catalog",
        return_value="system_iceberg",
    )
    assert adapter.current_catalog_type == "iceberg"


def test_partitioned_by_hive(
    trino_mocked_engine_adapter: TrinoEngineAdapter, mocker: MockerFixture
):
    adapter = trino_mocked_engine_adapter

    mocker.patch(
        "sqlmesh.core.engine_adapter.trino.TrinoEngineAdapter.get_current_catalog",
        return_value="datalake_hive",
    )
    assert adapter.get_catalog_type("datalake_hive") == "hive"

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }

    adapter.create_table("test_table", columns_to_types, partitioned_by=[exp.to_column("colb")])

    adapter.ctas("test_table", parse_one("select 1"), partitioned_by=[exp.to_column("colb")])  # type: ignore

    assert to_sql_calls(adapter) == [
        """CREATE TABLE IF NOT EXISTS "test_table" ("cola" INTEGER, "colb" VARCHAR) WITH (PARTITIONED_BY=ARRAY['colb'])""",
        """CREATE TABLE IF NOT EXISTS "test_table" WITH (PARTITIONED_BY=ARRAY['colb']) AS SELECT 1""",
    ]


def test_partitioned_by_iceberg(
    trino_mocked_engine_adapter: TrinoEngineAdapter, mocker: MockerFixture
):
    adapter = trino_mocked_engine_adapter

    mocker.patch(
        "sqlmesh.core.engine_adapter.trino.TrinoEngineAdapter.get_current_catalog",
        return_value="datalake_iceberg",
    )
    assert adapter.get_catalog_type("datalake_iceberg") == "iceberg"

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }

    adapter.create_table("test_table", columns_to_types, partitioned_by=[exp.to_column("colb")])

    adapter.ctas("test_table", parse_one("select 1"), partitioned_by=[exp.to_column("colb")])  # type: ignore

    assert to_sql_calls(adapter) == [
        """CREATE TABLE IF NOT EXISTS "test_table" ("cola" INTEGER, "colb" VARCHAR) WITH (PARTITIONING=ARRAY['colb'])""",
        """CREATE TABLE IF NOT EXISTS "test_table" WITH (PARTITIONING=ARRAY['colb']) AS SELECT 1""",
    ]


def test_partitioned_by_iceberg_transforms(
    trino_mocked_engine_adapter: TrinoEngineAdapter, mocker: MockerFixture
):
    adapter = trino_mocked_engine_adapter

    mocker.patch(
        "sqlmesh.core.engine_adapter.trino.TrinoEngineAdapter.get_current_catalog",
        return_value="datalake_iceberg",
    )

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }

    adapter.create_table(
        "test_table",
        columns_to_types,
        partitioned_by=[exp.to_column("day(cola)"), exp.to_column("truncate(colb, 8)")],
    )

    adapter.ctas(
        "test_table",
        parse_one("select 1"),  # type: ignore
        partitioned_by=[exp.to_column("day(cola)"), exp.to_column("truncate(colb, 8)")],
    )

    assert to_sql_calls(adapter) == [
        """CREATE TABLE IF NOT EXISTS "test_table" ("cola" INTEGER, "colb" VARCHAR) WITH (PARTITIONING=ARRAY['day(cola)', 'truncate(colb, 8)'])""",
        """CREATE TABLE IF NOT EXISTS "test_table" WITH (PARTITIONING=ARRAY['day(cola)', 'truncate(colb, 8)']) AS SELECT 1""",
    ]


def test_partitioned_by_with_multiple_catalogs_same_server(
    trino_mocked_engine_adapter: TrinoEngineAdapter, mocker: MockerFixture
):
    adapter = trino_mocked_engine_adapter

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }

    adapter.create_table(
        "datalake.test_schema.test_table", columns_to_types, partitioned_by=[exp.to_column("colb")]
    )

    adapter.ctas(
        "datalake.test_schema.test_table",
        parse_one("select 1"),  # type: ignore
        partitioned_by=[exp.to_column("colb")],
    )

    adapter.create_table(
        "datalake_iceberg.test_schema.test_table",
        columns_to_types,
        partitioned_by=[exp.to_column("colb")],
    )

    adapter.ctas(
        "datalake_iceberg.test_schema.test_table",
        parse_one("select 1"),  # type: ignore
        partitioned_by=[exp.to_column("colb")],
    )

    assert to_sql_calls(adapter) == [
        """CREATE TABLE IF NOT EXISTS "datalake"."test_schema"."test_table" ("cola" INTEGER, "colb" VARCHAR) WITH (PARTITIONED_BY=ARRAY['colb'])""",
        """CREATE TABLE IF NOT EXISTS "datalake"."test_schema"."test_table" WITH (PARTITIONED_BY=ARRAY['colb']) AS SELECT 1""",
        """CREATE TABLE IF NOT EXISTS "datalake_iceberg"."test_schema"."test_table" ("cola" INTEGER, "colb" VARCHAR) WITH (PARTITIONING=ARRAY['colb'])""",
        """CREATE TABLE IF NOT EXISTS "datalake_iceberg"."test_schema"."test_table" WITH (PARTITIONING=ARRAY['colb']) AS SELECT 1""",
    ]


def test_comments_hive(mocker: MockerFixture, make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(TrinoEngineAdapter)

    current_catalog_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.trino.TrinoEngineAdapter.get_current_catalog"
    )
    current_catalog_mock.return_value = "hive"
    catalog_type_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.trino.TrinoEngineAdapter.get_catalog_type"
    )
    catalog_type_mock.return_value = "hive"

    allowed_table_comment_length = TrinoEngineAdapter.MAX_TABLE_COMMENT_LENGTH
    truncated_table_comment = "a" * allowed_table_comment_length
    long_table_comment = truncated_table_comment + "b"

    allowed_column_comment_length = TrinoEngineAdapter.MAX_COLUMN_COMMENT_LENGTH
    truncated_column_comment = "c" * allowed_column_comment_length
    long_column_comment = truncated_column_comment + "d"

    adapter.create_table(
        "test_table",
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description=long_table_comment,
        column_descriptions={"a": long_column_comment},
    )

    adapter.ctas(
        "test_table",
        parse_one("SELECT a, b FROM source_table"),
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description=long_table_comment,
        column_descriptions={"a": long_column_comment},
    )

    adapter.create_view(
        "test_view",
        parse_one("SELECT a, b FROM source_table"),
        table_description=long_table_comment,
    )

    adapter._create_table_comment(
        "test_table",
        long_table_comment,
    )

    adapter._create_column_comments(
        "test_table",
        {"a": long_column_comment},
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        f"""CREATE TABLE IF NOT EXISTS "test_table" ("a" INTEGER COMMENT '{truncated_column_comment}', "b" INTEGER) COMMENT '{truncated_table_comment}'""",
        f"""CREATE TABLE IF NOT EXISTS "test_table" COMMENT '{truncated_table_comment}' AS SELECT "a", "b" FROM "source_table\"""",
        f"""COMMENT ON COLUMN "test_table"."a" IS '{truncated_column_comment}'""",
        f"""CREATE OR REPLACE VIEW "test_view" AS SELECT "a", "b" FROM "source_table\"""",
        f"""COMMENT ON VIEW "test_view" IS '{truncated_table_comment}'""",
        f"""COMMENT ON TABLE "test_table" IS '{truncated_table_comment}'""",
        f"""COMMENT ON COLUMN "test_table"."a" IS '{truncated_column_comment}'""",
    ]


def test_comments_iceberg(mocker: MockerFixture, make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(TrinoEngineAdapter)

    current_catalog_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.trino.TrinoEngineAdapter.get_current_catalog"
    )
    current_catalog_mock.return_value = "iceberg"
    catalog_type_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.trino.TrinoEngineAdapter.get_catalog_type"
    )
    catalog_type_mock.return_value = "iceberg"

    allowed_table_comment_length = TrinoEngineAdapter.MAX_TABLE_COMMENT_LENGTH
    truncated_table_comment = "a" * allowed_table_comment_length
    long_table_comment = truncated_table_comment + "b"

    allowed_column_comment_length = TrinoEngineAdapter.MAX_COLUMN_COMMENT_LENGTH
    truncated_column_comment = "c" * allowed_column_comment_length
    long_column_comment = truncated_column_comment + "d"

    adapter.create_table(
        "test_table",
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description=long_table_comment,
        column_descriptions={"a": long_column_comment},
    )

    adapter.ctas(
        "test_table",
        parse_one("SELECT a, b FROM source_table"),
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description=long_table_comment,
        column_descriptions={"a": long_column_comment},
    )

    adapter.create_view(
        "test_view",
        parse_one("SELECT a, b FROM source_table"),
        table_description=long_table_comment,
    )

    adapter._create_table_comment(
        "test_table",
        long_table_comment,
    )

    adapter._create_column_comments(
        "test_table",
        {"a": long_column_comment},
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        f"""CREATE TABLE IF NOT EXISTS "test_table" ("a" INTEGER COMMENT '{long_column_comment}', "b" INTEGER) COMMENT '{long_table_comment}'""",
        f"""CREATE TABLE IF NOT EXISTS "test_table" COMMENT '{long_table_comment}' AS SELECT "a", "b" FROM "source_table\"""",
        f"""COMMENT ON COLUMN "test_table"."a" IS '{long_column_comment}'""",
        f"""CREATE OR REPLACE VIEW "test_view" AS SELECT "a", "b" FROM "source_table\"""",
        f"""COMMENT ON VIEW "test_view" IS '{long_table_comment}'""",
        f"""COMMENT ON TABLE "test_table" IS '{long_table_comment}'""",
        f"""COMMENT ON COLUMN "test_table"."a" IS '{long_column_comment}'""",
    ]
