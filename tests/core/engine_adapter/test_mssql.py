# type: ignore
import typing as t
import uuid

import pandas as pd
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter.base import InsertOverwriteStrategy
from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter
from sqlmesh.utils.date import to_ds
from tests.core.engine_adapter import to_sql_calls


def test_columns(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    adapter.cursor.fetchall.return_value = [
        ("decimal_ps", "decimal", None, 5, 4),
        ("decimal", "decimal", None, 18, 0),
        ("numeric_ps", "numeric", None, 5, 4),
        ("numeric", "numeric", None, 18, 0),
        ("float_n", "real", None, 24, None),
        ("float", "float", None, 53, None),
        ("binary_n", "binary", 10, None, None),
        ("binary", "binary", 1, None, None),
        ("var_binary_n", "varbinary", 10, None, None),
        ("var_binary_max", "varbinary", -1, None, None),
        ("var_binary", "varbinary", 1, None, None),
        ("char_n", "char", 10, None, None),
        ("char", "char", 1, None, None),
        ("varchar_n", "varchar", 10, None, None),
        ("varchar_max", "varchar", -1, None, None),
        ("varchar", "varchar", 1, None, None),
        ("nchar_n", "nchar", 10, None, None),
        ("nchar", "nchar", 1, None, None),
        ("nvarchar_n", "nvarchar", 10, None, None),
        ("nvarchar", "nvarchar", 1, None, None),
    ]

    assert adapter.columns("db.table") == {
        "decimal_ps": exp.DataType.build("decimal(5, 4)", dialect=adapter.dialect),
        "decimal": exp.DataType.build("decimal(18, 0)", dialect=adapter.dialect),
        "numeric_ps": exp.DataType.build("numeric(5, 4)", dialect=adapter.dialect),
        "numeric": exp.DataType.build("numeric(18, 0)", dialect=adapter.dialect),
        "float_n": exp.DataType.build("real", dialect=adapter.dialect),
        "float": exp.DataType.build("float(53)", dialect=adapter.dialect),
        "binary_n": exp.DataType.build("binary(10)", dialect=adapter.dialect),
        "binary": exp.DataType.build("binary(1)", dialect=adapter.dialect),
        "var_binary_n": exp.DataType.build("varbinary(10)", dialect=adapter.dialect),
        "var_binary_max": exp.DataType.build("varbinary(max)", dialect=adapter.dialect),
        "var_binary": exp.DataType.build("varbinary(1)", dialect=adapter.dialect),
        "char_n": exp.DataType.build("char(10)", dialect=adapter.dialect),
        "char": exp.DataType.build("char(1)", dialect=adapter.dialect),
        "varchar_n": exp.DataType.build("varchar(10)", dialect=adapter.dialect),
        "varchar_max": exp.DataType.build("varchar(max)", dialect=adapter.dialect),
        "varchar": exp.DataType.build("varchar(1)", dialect=adapter.dialect),
        "nchar_n": exp.DataType.build("nchar(10)", dialect=adapter.dialect),
        "nchar": exp.DataType.build("nchar(1)", dialect=adapter.dialect),
        "nvarchar_n": exp.DataType.build("nvarchar(10)", dialect=adapter.dialect),
        "nvarchar": exp.DataType.build("nvarchar(1)", dialect=adapter.dialect),
    }

    adapter.cursor.execute.assert_called_once_with(
        """SELECT "column_name", "data_type", "character_maximum_length", "numeric_precision", "numeric_scale" FROM "master"."information_schema"."columns" WHERE "table_name" = 'table' AND "table_schema" = 'db';"""
    )


def test_table_exists(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)

    resp = adapter.table_exists("db.table")
    adapter.cursor.execute.assert_called_once_with(
        """SELECT 1 """
        """FROM "master"."information_schema"."tables" """
        """WHERE "table_name" = 'table' AND "table_schema" = 'db';"""
    )
    assert resp
    adapter.cursor.fetchone.return_value = None
    resp = adapter.table_exists("db.table")
    assert not resp


def test_insert_overwrite_by_time_partition_supports_insert_overwrite_pandas(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE

    temp_table_uuid = uuid.uuid4()
    uuid4_mock = mocker.patch("uuid.uuid4")
    uuid4_mock.return_value = temp_table_uuid

    df = pd.DataFrame({"a": [1, 2], "ds": ["2022-01-01", "2022-01-02"]})
    adapter.insert_overwrite_by_time_partition(
        "test_table",
        df,
        start="2022-01-01",
        end="2022-01-02",
        time_formatter=lambda x, _: exp.Literal.string(to_ds(x)),
        time_column="ds",
        columns_to_types={"a": exp.DataType.build("INT"), "ds": exp.DataType.build("STRING")},
    )
    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f'"__temp_test_table_{temp_table_uuid.hex}"',
        [(1, "2022-01-01"), (2, "2022-01-02")],
    )
    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table_{temp_table_uuid.hex}') EXEC('CREATE TABLE "__temp_test_table_{temp_table_uuid.hex}" ("a" INTEGER, "ds" TEXT)');""",
        f"""MERGE INTO "test_table" AS "__MERGE_TARGET__" USING (SELECT "a", "ds" FROM (SELECT "a", "ds" FROM "__temp_test_table_{temp_table_uuid.hex}") AS "_subquery" WHERE "ds" BETWEEN '2022-01-01' AND '2022-01-02') AS "__MERGE_SOURCE__" ON (1 = 0) WHEN NOT MATCHED BY SOURCE AND "ds" BETWEEN '2022-01-01' AND '2022-01-02' THEN DELETE WHEN NOT MATCHED THEN INSERT ("a", "ds") VALUES ("a", "ds");""",
        f'DROP TABLE IF EXISTS "__temp_test_table_{temp_table_uuid.hex}";',
    ]


def test_insert_overwrite_by_time_partition_replace_where_pandas(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.REPLACE_WHERE

    temp_table_uuid = uuid.uuid4()
    uuid4_mock = mocker.patch("uuid.uuid4")
    uuid4_mock.return_value = temp_table_uuid

    df = pd.DataFrame({"a": [1, 2], "ds": ["2022-01-01", "2022-01-02"]})
    adapter.insert_overwrite_by_time_partition(
        "test_table",
        df,
        start="2022-01-01",
        end="2022-01-02",
        time_formatter=lambda x, _: exp.Literal.string(to_ds(x)),
        time_column="ds",
        columns_to_types={"a": exp.DataType.build("INT"), "ds": exp.DataType.build("STRING")},
    )
    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f'"__temp_test_table_{temp_table_uuid.hex}"',
        [(1, "2022-01-01"), (2, "2022-01-02")],
    )

    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table_{temp_table_uuid.hex}') EXEC('CREATE TABLE "__temp_test_table_{temp_table_uuid.hex}" ("a" INTEGER, "ds" TEXT)');""",
        f"""MERGE INTO "test_table" AS "__MERGE_TARGET__" USING (SELECT "a", "ds" FROM (SELECT "a", "ds" FROM "__temp_test_table_{temp_table_uuid.hex}") AS "_subquery" WHERE "ds" BETWEEN '2022-01-01' AND '2022-01-02') AS "__MERGE_SOURCE__" ON (1 = 0) WHEN NOT MATCHED BY SOURCE AND "ds" BETWEEN '2022-01-01' AND '2022-01-02' THEN DELETE WHEN NOT MATCHED THEN INSERT ("a", "ds") VALUES ("a", "ds");""",
        f'DROP TABLE IF EXISTS "__temp_test_table_{temp_table_uuid.hex}";',
    ]


def test_insert_append_pandas(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    temp_table_uuid = uuid.uuid4()
    uuid4_mock = mocker.patch("uuid.uuid4")
    uuid4_mock.return_value = temp_table_uuid

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.insert_append(
        "test_table",
        df,
        columns_to_types={
            "a": exp.DataType.build("INT"),
            "b": exp.DataType.build("INT"),
        },
    )
    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f'"__temp_test_table_{temp_table_uuid.hex}"',
        [(1, 4), (2, 5), (3, 6)],
    )

    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table_{temp_table_uuid.hex}') EXEC('CREATE TABLE "__temp_test_table_{temp_table_uuid.hex}" ("a" INTEGER, "b" INTEGER)');""",
        f'INSERT INTO "test_table" ("a", "b") SELECT "a", "b" FROM "__temp_test_table_{temp_table_uuid.hex}";',
        f'DROP TABLE IF EXISTS "__temp_test_table_{temp_table_uuid.hex}";',
    ]


def test_create_table(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table("test_table", columns_to_types)

    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'test_table') EXEC('CREATE TABLE "test_table" ("cola" INTEGER, "colb" TEXT)');"""
    )


def test_create_table_properties(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table(
        "test_table",
        columns_to_types,
        partitioned_by=[exp.to_column("colb")],
        storage_format="ICEBERG",
    )

    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'test_table') EXEC('CREATE TABLE "test_table" ("cola" INTEGER, "colb" TEXT)');"""
    )


def test_merge_pandas(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    temp_table_uuid = uuid.uuid4()
    uuid4_mock = mocker.patch("uuid.uuid4")
    uuid4_mock.return_value = temp_table_uuid

    df = pd.DataFrame({"id": [1, 2, 3], "ts": [1, 2, 3], "val": [4, 5, 6]})
    adapter.merge(
        target_table="target",
        source_table=df,
        columns_to_types={
            "id": exp.DataType.build("int"),
            "ts": exp.DataType.build("TIMESTAMP"),
            "val": exp.DataType.build("int"),
        },
        unique_key=[exp.to_identifier("id")],
    )
    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f'"__temp_target_{temp_table_uuid.hex}"',
        [(1, 1, 4), (2, 2, 5), (3, 3, 6)],
    )

    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_target_{temp_table_uuid.hex}') EXEC('CREATE TABLE "__temp_target_{temp_table_uuid.hex}" ("id" INTEGER, "ts" DATETIME2, "val" INTEGER)');""",
        f'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT "id", "ts", "val" FROM "__temp_target_{temp_table_uuid.hex}") AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id", "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts", "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val" WHEN NOT MATCHED THEN INSERT ("id", "ts", "val") VALUES ("__MERGE_SOURCE__"."id", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val");',
        f'DROP TABLE IF EXISTS "__temp_target_{temp_table_uuid.hex}";',
    ]

    adapter.cursor.reset_mock()
    adapter._connection_pool.get().reset_mock()
    adapter.merge(
        target_table="target",
        source_table=df,
        columns_to_types={
            "id": exp.DataType.build("int"),
            "ts": exp.DataType.build("TIMESTAMP"),
            "val": exp.DataType.build("int"),
        },
        unique_key=[exp.to_identifier("id"), exp.to_column("ts")],
    )
    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f'"__temp_target_{temp_table_uuid.hex}"',
        [(1, 1, 4), (2, 2, 5), (3, 3, 6)],
    )

    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_target_{temp_table_uuid.hex}') EXEC('CREATE TABLE "__temp_target_{temp_table_uuid.hex}" ("id" INTEGER, "ts" DATETIME2, "val" INTEGER)');""",
        f'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT "id", "ts", "val" FROM "__temp_target_{temp_table_uuid.hex}") AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" AND "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts" WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id", "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts", "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val" WHEN NOT MATCHED THEN INSERT ("id", "ts", "val") VALUES ("__MERGE_SOURCE__"."id", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val");',
        f'DROP TABLE IF EXISTS "__temp_target_{temp_table_uuid.hex}";',
    ]


def test_replace_query(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    assert to_sql_calls(adapter) == [
        """SELECT 1 FROM "master"."information_schema"."tables" WHERE "table_name" = 'test_table';""",
        'TRUNCATE TABLE "test_table"',
        'INSERT INTO "test_table" ("a") SELECT "a" FROM "tbl";',
    ]


def test_replace_query_pandas(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)

    temp_table_uuid = uuid.uuid4()
    uuid4_mock = mocker.patch("uuid.uuid4")
    uuid4_mock.return_value = temp_table_uuid

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query(
        "test_table", df, {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")}
    )

    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f'"__temp_test_table_{temp_table_uuid.hex}"',
        [(1, 4), (2, 5), (3, 6)],
    )

    assert to_sql_calls(adapter) == [
        """SELECT 1 FROM "master"."information_schema"."tables" WHERE "table_name" = 'test_table';""",
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table_{temp_table_uuid.hex}') EXEC('CREATE TABLE "__temp_test_table_{temp_table_uuid.hex}" ("a" INTEGER, "b" INTEGER)');""",
        'TRUNCATE TABLE "test_table"',
        f'INSERT INTO "test_table" ("a", "b") SELECT "a", "b" FROM "__temp_test_table_{temp_table_uuid.hex}";',
        f'DROP TABLE IF EXISTS "__temp_test_table_{temp_table_uuid.hex}";',
    ]


def test_create_table_primary_key(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table("test_table", columns_to_types, primary_key=("cola", "colb"))

    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'test_table') EXEC('CREATE TABLE "test_table" ("cola" INTEGER, "colb" TEXT, PRIMARY KEY ("cola", "colb"))');"""
    )


def test_create_index(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.SUPPORTS_INDEXES = True

    adapter.create_index("test_table", "test_index", ("cola", "colb"))
    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id('test_table') AND name = 'test_index') EXEC('CREATE INDEX "test_index" ON "test_table"("cola", "colb")');"""
    )
