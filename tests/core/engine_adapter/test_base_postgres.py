# type: ignore
import typing as t
from unittest.mock import call

import pytest
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter

pytestmark = [pytest.mark.engine, pytest.mark.postgres, pytest.mark.redshift]


def test_columns(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(BasePostgresEngineAdapter)
    adapter.cursor.fetchall.return_value = [("col", "INT")]

    resp = adapter.columns("db.table")
    adapter.cursor.execute.assert_called_once_with(
        """SELECT "column_name", "data_type" FROM "information_schema"."columns" WHERE "table_name" = 'table' AND "table_schema" = 'db'"""
    )
    assert resp == {"col": exp.DataType.build("INT")}


def test_table_exists(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(BasePostgresEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)

    resp = adapter.table_exists("db.table")
    adapter.cursor.execute.assert_called_once_with(
        """SELECT 1 FROM "information_schema"."tables" WHERE "table_name" = 'table' AND "table_schema" = 'db'"""
    )
    assert resp
    adapter.cursor.fetchone.return_value = None
    resp = adapter.table_exists("db.table")
    assert not resp


def test_create_view(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(BasePostgresEngineAdapter)

    adapter.create_view("db.view", parse_one("SELECT 1"), replace=True)
    adapter.create_view("db.view", parse_one("SELECT 1"), replace=False)

    adapter.cursor.execute.assert_has_calls(
        [
            # 1st call
            call('DROP VIEW IF EXISTS "db"."view" CASCADE'),
            call('CREATE VIEW "db"."view" AS SELECT 1'),
            # 2nd call
            call('CREATE VIEW "db"."view" AS SELECT 1'),
        ]
    )
