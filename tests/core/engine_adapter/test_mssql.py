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


def test_table_exists(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)

    resp = adapter.table_exists("db.table")
    adapter.cursor.execute.assert_called_once_with(
        """SELECT 1 """
        """FROM "master"."information_schema"."tables" """
        """WHERE "table_name" = 'table' AND "table_schema" = 'db'"""
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
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table_{temp_table_uuid.hex}') EXEC('CREATE TABLE "__temp_test_table_{temp_table_uuid.hex}" ("a" INTEGER, "ds" TEXT)')""",
        f"""MERGE INTO "test_table" AS "__MERGE_TARGET__" USING (SELECT "a", "ds" FROM (SELECT "a", "ds" FROM "__temp_test_table_{temp_table_uuid.hex}") AS "_subquery" WHERE "ds" BETWEEN '2022-01-01' AND '2022-01-02') AS "__MERGE_SOURCE__" ON 1 = 2 WHEN NOT MATCHED BY SOURCE AND "ds" BETWEEN '2022-01-01' AND '2022-01-02' THEN DELETE WHEN NOT MATCHED THEN INSERT ("a", "ds") VALUES ("a", "ds")""",
        f'DROP TABLE IF EXISTS "__temp_test_table_{temp_table_uuid.hex}"',
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
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table_{temp_table_uuid.hex}') EXEC('CREATE TABLE "__temp_test_table_{temp_table_uuid.hex}" ("a" INTEGER, "ds" TEXT)')""",
        f"""MERGE INTO "test_table" AS "__MERGE_TARGET__" USING (SELECT "a", "ds" FROM (SELECT "a", "ds" FROM "__temp_test_table_{temp_table_uuid.hex}") AS "_subquery" WHERE "ds" BETWEEN '2022-01-01' AND '2022-01-02') AS "__MERGE_SOURCE__" ON 1 = 2 WHEN NOT MATCHED BY SOURCE AND "ds" BETWEEN '2022-01-01' AND '2022-01-02' THEN DELETE WHEN NOT MATCHED THEN INSERT ("a", "ds") VALUES ("a", "ds")""",
        f'DROP TABLE IF EXISTS "__temp_test_table_{temp_table_uuid.hex}"',
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
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table_{temp_table_uuid.hex}') EXEC('CREATE TABLE "__temp_test_table_{temp_table_uuid.hex}" ("a" INTEGER, "b" INTEGER)')""",
        f'INSERT INTO "test_table" ("a", "b") SELECT "a", "b" FROM "__temp_test_table_{temp_table_uuid.hex}"',
        f'DROP TABLE IF EXISTS "__temp_test_table_{temp_table_uuid.hex}"',
    ]


def test_create_table(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table("test_table", columns_to_types)

    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'test_table') EXEC('CREATE TABLE "test_table" ("cola" INTEGER, "colb" TEXT)')"""
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
        """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'test_table') EXEC('CREATE TABLE "test_table" ("cola" INTEGER, "colb" TEXT)')"""
    )


def test_merge_pandas(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    temp_table_uuid = uuid.uuid4()
    uuid4_mock = mocker.patch("uuid.uuid4")
    uuid4_mock.return_value = temp_table_uuid

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.merge(
        target_table="target",
        source_table=df,
        columns_to_types={
            "id": exp.DataType.build("int"),
            "ts": exp.DataType.build("TIMESTAMP"),
            "val": exp.DataType.build("int"),
        },
        unique_key=["id"],
    )
    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f'"__temp_target_{temp_table_uuid.hex}"',
        [(1, 4), (2, 5), (3, 6)],
    )

    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_target_{temp_table_uuid.hex}') EXEC('CREATE TABLE "__temp_target_{temp_table_uuid.hex}" ("id" INTEGER, "ts" DATETIME2, "val" INTEGER)')""",
        f'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT "id", "ts", "val" FROM "__temp_target_{temp_table_uuid.hex}") AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id", "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts", "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val" WHEN NOT MATCHED THEN INSERT ("id", "ts", "val") VALUES ("__MERGE_SOURCE__"."id", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")',
        f'DROP TABLE IF EXISTS "__temp_target_{temp_table_uuid.hex}"',
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
        unique_key=["id", "ts"],
    )
    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f'"__temp_target_{temp_table_uuid.hex}"',
        [(1, 4), (2, 5), (3, 6)],
    )

    assert to_sql_calls(adapter) == [
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_target_{temp_table_uuid.hex}') EXEC('CREATE TABLE "__temp_target_{temp_table_uuid.hex}" ("id" INTEGER, "ts" DATETIME2, "val" INTEGER)')""",
        f'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT "id", "ts", "val" FROM "__temp_target_{temp_table_uuid.hex}") AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" AND "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts" WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id", "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts", "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val" WHEN NOT MATCHED THEN INSERT ("id", "ts", "val") VALUES ("__MERGE_SOURCE__"."id", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")',
        f'DROP TABLE IF EXISTS "__temp_target_{temp_table_uuid.hex}"',
    ]


def test_replace_query(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    assert to_sql_calls(adapter) == [
        """SELECT 1 FROM "master"."information_schema"."tables" WHERE "table_name" = 'test_table'""",
        'TRUNCATE "test_table"',
        'INSERT INTO "test_table" ("a") SELECT "a" FROM "tbl"',
    ]


def test_replace_query_pandas(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)

    temp_table_uuid = uuid.uuid4()
    uuid4_mock = mocker.patch("uuid.uuid4")
    uuid4_mock.return_value = temp_table_uuid

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query("test_table", df, {"a": "int", "b": "int"})

    adapter._connection_pool.get().bulk_copy.assert_called_with(
        f'"__temp_test_table_{temp_table_uuid.hex}"',
        [(1, 4), (2, 5), (3, 6)],
    )

    assert to_sql_calls(adapter) == [
        """SELECT 1 FROM "master"."information_schema"."tables" WHERE "table_name" = 'test_table'""",
        f"""IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table_{temp_table_uuid.hex}') EXEC('CREATE TABLE "__temp_test_table_{temp_table_uuid.hex}" ("a" int, "b" int)')""",
        'TRUNCATE "test_table"',
        f'INSERT INTO "test_table" ("a", "b") SELECT "a", "b" FROM "__temp_test_table_{temp_table_uuid.hex}"',
        f'DROP TABLE IF EXISTS "__temp_test_table_{temp_table_uuid.hex}"',
    ]


def test_create_table_primary_key(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table("test_table", columns_to_types, primary_key=("cola", "colb"))

    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'test_table') EXEC('CREATE TABLE "test_table" ("cola" INTEGER, "colb" TEXT, PRIMARY KEY ("cola", "colb"))')"""
    )


def test_create_index(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.SUPPORTS_INDEXES = True

    adapter.create_index("test_table", "test_index", ("cola", "colb"))
    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id('test_table') AND name = 'test_index') EXEC('CREATE INDEX "test_index" ON "test_table"("cola", "colb")')"""
    )
