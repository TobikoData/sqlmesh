# type: ignore
import typing as t
from unittest.mock import call

from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter.mixins import (
    LogicalMergeMixin,
    LogicalReplaceQueryMixin,
)


def test_logical_replace_query_already_exists(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(LogicalReplaceQueryMixin, "postgres")
    adapter.cursor.fetchone.return_value = (1,)
    mocker.patch(
        "sqlmesh.core.engine_adapter.postgres.LogicalReplaceQueryMixin.table_exists",
        return_value=True,
    )

    adapter.replace_query("db.table", parse_one("SELECT col FROM db.other_table"))
    adapter.cursor.execute.assert_has_calls(
        [
            call('TRUNCATE "db"."table"'),
            call('INSERT INTO "db"."table" SELECT "col" FROM "db"."other_table"'),
        ]
    )


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
