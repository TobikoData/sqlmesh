# type: ignore
import typing as t
from unittest.mock import call

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter.mixins import (
    LogicalMergeMixin,
    NonTransactionalTruncateMixin,
)
from tests.core.engine_adapter import to_sql_calls

pytestmark = pytest.mark.engine


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


def test_non_transaction_truncate_mixin(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture, make_temp_table_name: t.Callable
):
    adapter = make_mocked_engine_adapter(NonTransactionalTruncateMixin, "redshift")
    adapter._truncate_table(table_name="test_table")

    assert to_sql_calls(adapter) == ['TRUNCATE TABLE "test_table"']


def test_non_transaction_truncate_mixin_within_transaction(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture, make_temp_table_name: t.Callable
):
    adapter = make_mocked_engine_adapter(NonTransactionalTruncateMixin, "redshift")
    adapter._connection_pool = mocker.MagicMock()
    adapter._connection_pool.is_transaction_active = True
    adapter._truncate_table(table_name="test_table")

    assert to_sql_calls(adapter) == ['DELETE FROM "test_table"']
