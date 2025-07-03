# type: ignore
import typing as t
from unittest.mock import call

import pytest
from sqlglot import parse_one, exp
from sqlmesh.core.engine_adapter.risingwave import RisingwaveEngineAdapter

pytestmark = [pytest.mark.engine, pytest.mark.risingwave]


@pytest.fixture
def adapter(make_mocked_engine_adapter):
    adapter = make_mocked_engine_adapter(RisingwaveEngineAdapter)
    return adapter


def test_columns(adapter: t.Callable):
    adapter.cursor.fetchall.return_value = [
        ("smallint_col", "smallint"),
        ("int_col", "integer"),
        ("bigint_col", "bigint"),
        ("ts_col", "timestamp without time zone"),
        ("tstz_col", "timestamp with time zone"),
        ("int_array_col", "integer[]"),
        ("vchar_col", "character varying"),
        ("struct_col", "struct<nested_col integer>"),
    ]
    resp = adapter.columns("db.table")
    assert resp == {
        "smallint_col": exp.DataType.build(exp.DataType.Type.SMALLINT, nested=False),
        "int_col": exp.DataType.build(exp.DataType.Type.INT, nested=False),
        "bigint_col": exp.DataType.build(exp.DataType.Type.BIGINT, nested=False),
        "ts_col": exp.DataType.build(exp.DataType.Type.TIMESTAMP, nested=False),
        "tstz_col": exp.DataType.build(exp.DataType.Type.TIMESTAMPTZ, nested=False),
        "int_array_col": exp.DataType.build(
            exp.DataType.Type.ARRAY,
            expressions=[exp.DataType.build(exp.DataType.Type.INT, nested=False)],
            nested=True,
        ),
        "vchar_col": exp.DataType.build(exp.DataType.Type.VARCHAR),
        "struct_col": exp.DataType.build(
            exp.DataType.Type.STRUCT,
            expressions=[
                exp.ColumnDef(
                    this=exp.Identifier(this="nested_col", quoted=False),
                    kind=exp.DataType.build(exp.DataType.Type.INT, nested=False),
                )
            ],
            nested=True,
        ),
    }


def test_create_view(adapter: t.Callable):
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


def test_drop_view(adapter: t.Callable):
    adapter.drop_view("db.view")

    adapter.drop_view("db.view", materialized=True)

    adapter.drop_view("db.view", cascade=False)

    adapter.cursor.execute.assert_has_calls(
        [
            # 1st call
            call('DROP VIEW IF EXISTS "db"."view" CASCADE'),
            # 2nd call
            call('DROP MATERIALIZED VIEW IF EXISTS "db"."view" CASCADE'),
            # 3rd call
            call('DROP VIEW IF EXISTS "db"."view"'),
        ]
    )
