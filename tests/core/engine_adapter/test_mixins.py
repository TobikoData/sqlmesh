# type: ignore
import typing as t
import uuid
from unittest.mock import call

from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter.mixins import (
    LogicalMergeMixin,
    LogicalReplaceQueryMixin,
)
from tests.core.engine_adapter import to_sql_calls


def test_logical_replace_query_already_exists(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(LogicalReplaceQueryMixin, "postgres")
    adapter.cursor.fetchone.return_value = (1,)

    mocker.patch(
        "sqlmesh.core.engine_adapter.postgres.LogicalReplaceQueryMixin.table_exists",
        return_value=True,
    )
    mocker.patch(
        "sqlmesh.core.engine_adapter.postgres.LogicalReplaceQueryMixin.columns",
        return_value={"col": exp.DataType(this=exp.DataType.Type.INT)},
    )

    adapter.replace_query("db.table", parse_one("SELECT col FROM db.other_table"))

    assert to_sql_calls(adapter) == [
        'TRUNCATE "db"."table"',
        'INSERT INTO "db"."table" ("col") SELECT "col" FROM "db"."other_table"',
    ]


def test_logical_replace_query_does_not_exist(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(LogicalReplaceQueryMixin, "postgres")
    adapter.cursor.fetchone.return_value = (1,)
    mocker.patch(
        "sqlmesh.core.engine_adapter.postgres.LogicalReplaceQueryMixin.table_exists",
        return_value=False,
    )

    adapter.replace_query("db.table", parse_one("SELECT col FROM db.other_table"))
    adapter.cursor.execute.assert_called_once_with(
        'CREATE TABLE "db"."table" AS SELECT "col" FROM "db"."other_table"'
    )


def test_logical_replace_self_reference(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(LogicalReplaceQueryMixin, "postgres")
    adapter.cursor.fetchone.return_value = (1,)
    temp_table_uuid = uuid.uuid4()
    uuid4_mock = mocker.patch("uuid.uuid4")
    uuid4_mock.return_value = temp_table_uuid

    mocker.patch(
        "sqlmesh.core.engine_adapter.postgres.LogicalReplaceQueryMixin.table_exists",
        return_value=True,
    )
    mocker.patch(
        "sqlmesh.core.engine_adapter.postgres.LogicalReplaceQueryMixin.columns",
        return_value={"col": exp.DataType(this=exp.DataType.Type.INT)},
    )

    adapter.replace_query("db.table", parse_one("SELECT col + 1 AS col FROM db.table"))

    assert to_sql_calls(adapter) == [
        f'CREATE SCHEMA IF NOT EXISTS "db"',
        f'CREATE TABLE IF NOT EXISTS "db"."__temp_table_{temp_table_uuid.hex}" AS SELECT "col" FROM "db"."table"',
        'TRUNCATE "db"."table"',
        f'INSERT INTO "db"."table" ("col") SELECT "col" + 1 AS "col" FROM "db"."__temp_table_{temp_table_uuid.hex}"',
        f'DROP TABLE IF EXISTS "db"."__temp_table_{temp_table_uuid.hex}"',
    ]


def test_logical_merge(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(LogicalMergeMixin, "duckdb")
    temp_table_mock = mocker.patch("sqlmesh.core.engine_adapter.base.EngineAdapter._get_temp_table")
    temp_table_mock.return_value = exp.to_table("temporary")

    adapter.merge(
        target_table="target",
        source_table=t.cast(exp.Select, parse_one("SELECT id, ts, val FROM source")),
        columns_to_types={
            "id": exp.DataType(this=exp.DataType.Type.INT),
            "ts": exp.DataType(this=exp.DataType.Type.TIMESTAMP),
            "val": exp.DataType(this=exp.DataType.Type.INT),
        },
        unique_key=["id"],
    )

    adapter.cursor.execute.assert_has_calls(
        [
            call('''CREATE TABLE "temporary" AS SELECT "id", "ts", "val" FROM "source"'''),
            call(
                """DELETE FROM "target" WHERE CONCAT_WS('__SQLMESH_DELIM__', "id") IN (SELECT CONCAT_WS('__SQLMESH_DELIM__', "id") FROM "temporary")"""
            ),
            call(
                """INSERT INTO "target" ("id", "ts", "val") (SELECT DISTINCT ON ("id") "id", "ts", "val" FROM "temporary")"""
            ),
            call('''DROP TABLE IF EXISTS "temporary"'''),
        ]
    )

    adapter.cursor.reset_mock()
    adapter.merge(
        target_table="target",
        source_table=t.cast(exp.Select, parse_one("SELECT id, ts, val FROM source")),
        columns_to_types={
            "id": exp.DataType(this=exp.DataType.Type.INT),
            "ts": exp.DataType(this=exp.DataType.Type.TIMESTAMP),
            "val": exp.DataType(this=exp.DataType.Type.INT),
        },
        unique_key=["id", "ts"],
    )

    adapter.cursor.execute.assert_has_calls(
        [
            call('''CREATE TABLE "temporary" AS SELECT "id", "ts", "val" FROM "source"'''),
            call(
                """DELETE FROM "target" WHERE CONCAT_WS('__SQLMESH_DELIM__', "id", "ts") IN (SELECT CONCAT_WS('__SQLMESH_DELIM__', "id", "ts") FROM "temporary")"""
            ),
            call(
                """INSERT INTO "target" ("id", "ts", "val") (SELECT DISTINCT ON ("id", "ts") "id", "ts", "val" FROM "temporary")"""
            ),
            call('''DROP TABLE IF EXISTS "temporary"'''),
        ]
    )
