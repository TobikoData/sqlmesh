import typing as t

import pytest
from pytest_mock import MockFixture
from pytest_mock.plugin import MockerFixture
from sqlglot import exp
from sqlglot.helper import ensure_list

from sqlmesh.core.engine_adapter import PostgresEngineAdapter
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.postgres]


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        (
            {
                "schema_name": "test_schema",
            },
            'DROP SCHEMA IF EXISTS "test_schema"',
        ),
        (
            {
                "schema_name": "test_schema",
                "ignore_if_not_exists": False,
            },
            'DROP SCHEMA "test_schema"',
        ),
        (
            {
                "schema_name": "test_schema",
                "cascade": True,
            },
            'DROP SCHEMA IF EXISTS "test_schema" CASCADE',
        ),
        (
            {
                "schema_name": "test_schema",
                "cascade": True,
                "ignore_if_not_exists": False,
            },
            'DROP SCHEMA "test_schema" CASCADE',
        ),
    ],
)
def test_drop_schema(kwargs, expected, make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    adapter.drop_schema(**kwargs)

    assert to_sql_calls(adapter) == ensure_list(expected)


def test_drop_schema_with_catalog(
    make_mocked_engine_adapter: t.Callable, mocker: MockFixture, caplog
):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    adapter.get_current_catalog = mocker.MagicMock(return_value="other_catalog")

    adapter.drop_schema("test_catalog.test_schema")
    assert "requires that all catalog operations be against a single catalog" in caplog.text


def test_comments(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(PostgresEngineAdapter)

    adapter.create_table(
        "test_table",
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description="\\",
        column_descriptions={"a": "\\"},
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        'CREATE TABLE IF NOT EXISTS "test_table" ("a" INT, "b" INT)',
        """COMMENT ON TABLE "test_table" IS '\\'""",
        """COMMENT ON COLUMN "test_table"."a" IS '\\'""",
    ]
