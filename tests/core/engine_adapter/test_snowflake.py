import typing as t

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.engine_adapter import SnowflakeEngineAdapter
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.snowflake]


def test_get_temp_table(mocker: MockerFixture, make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(SnowflakeEngineAdapter)

    mocker.patch("sqlmesh.core.engine_adapter.base.random_id", return_value="abcdefgh")

    value = adapter._get_temp_table(
        normalize_model_name("catalog.db.test_table", default_catalog=None, dialect=adapter.dialect)
    )

    assert value.sql(dialect=adapter.dialect) == '"CATALOG"."DB"."__temp_TEST_TABLE_abcdefgh"'


@pytest.mark.parametrize(
    "current_warehouse, current_warehouse_exp, configured_warehouse, configured_warehouse_exp, should_change",
    [
        (
            "test_warehouse",
            '"test_warehouse"',
            exp.to_identifier("test_warehouse", quoted=True),
            '"test_warehouse"',
            False,
        ),
        ("test_warehouse", '"test_warehouse"', "test_warehouse", '"TEST_WAREHOUSE"', True),
        ("TEST_WAREHOUSE", '"TEST_WAREHOUSE"', "test_warehouse", '"TEST_WAREHOUSE"', False),
        ("test warehouse", '"test warehouse"', "test warehouse", '"test warehouse"', False),
        ("test warehouse", '"test warehouse"', "another warehouse", '"another warehouse"', True),
        (
            "test warehouse",
            '"test warehouse"',
            exp.column("another warehouse"),
            '"another warehouse"',
            True,
        ),
        ("test warehouse", '"test warehouse"', "another_warehouse", '"ANOTHER_WAREHOUSE"', True),
        ("TEST_WAREHOUSE", '"TEST_WAREHOUSE"', "another_warehouse", '"ANOTHER_WAREHOUSE"', True),
        ("test_warehouse", '"test_warehouse"', "another_warehouse", '"ANOTHER_WAREHOUSE"', True),
        (
            "test_warehouse",
            '"test_warehouse"',
            exp.column("another_warehouse"),
            '"ANOTHER_WAREHOUSE"',
            True,
        ),
        (
            "test_warehouse",
            '"test_warehouse"',
            exp.to_identifier("another_warehouse"),
            '"ANOTHER_WAREHOUSE"',
            True,
        ),
        (
            "test_warehouse",
            '"test_warehouse"',
            exp.to_identifier("another_warehouse", quoted=True),
            '"another_warehouse"',
            True,
        ),
    ],
)
def test_session(
    mocker: MockerFixture,
    make_mocked_engine_adapter: t.Callable,
    current_warehouse: t.Union[str, exp.Expression],
    current_warehouse_exp: str,
    configured_warehouse: t.Optional[str],
    configured_warehouse_exp: t.Optional[str],
    should_change: bool,
):
    adapter = make_mocked_engine_adapter(SnowflakeEngineAdapter)
    adapter.cursor.fetchone.return_value = (current_warehouse,)

    with adapter.session({"warehouse": configured_warehouse}):
        pass

    expected_calls = []
    if configured_warehouse:
        expected_calls.append("SELECT CURRENT_WAREHOUSE()")
    if should_change:
        expected_calls.extend(
            [
                f"USE WAREHOUSE {configured_warehouse_exp}",
                f"USE WAREHOUSE {current_warehouse_exp}",
            ]
        )

    assert to_sql_calls(adapter) == expected_calls


def test_comments(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(SnowflakeEngineAdapter)

    adapter.create_table(
        "test_table",
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description="table description",
        column_descriptions={"a": "a column description"},
    )

    adapter.ctas(
        "test_table",
        parse_one("SELECT a, b FROM source_table"),
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_description="table description",
        column_descriptions={"a": "a column description"},
    )

    adapter.create_view(
        "test_view",
        parse_one("SELECT a, b FROM source_table"),
        table_description="table description",
        column_descriptions={"a": "a column description"},
    )

    adapter._create_table_comment(
        "test_table",
        "table description",
    )

    adapter._create_column_comments(
        "test_table",
        {"a": "a column description"},
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        """CREATE TABLE IF NOT EXISTS "test_table" ("a" INT COMMENT 'a column description', "b" INT) COMMENT='table description'""",
        """CREATE TABLE IF NOT EXISTS "test_table" ("a" INT COMMENT 'a column description', "b" INT) COMMENT='table description' AS SELECT CAST("a" AS INT) AS "a", CAST("b" AS INT) AS "b" FROM (SELECT "a", "b" FROM "source_table") AS "_subquery\"""",
        """CREATE OR REPLACE VIEW "test_view" COMMENT='table description' AS SELECT "a", "b" FROM "source_table\"""",
        """ALTER VIEW "test_view" ALTER COLUMN "a" COMMENT 'a column description'""",
        """COMMENT ON TABLE "test_table" IS 'table description'""",
        """ALTER TABLE "test_table" ALTER COLUMN "a" COMMENT 'a column description'""",
    ]


def test_df_to_source_queries_use_schema(
    make_mocked_engine_adapter: t.Callable, mocker: MockerFixture
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.snowflake.SnowflakeEngineAdapter.table_exists",
        return_value=False,
    )
    mocker.patch("snowflake.connector.pandas_tools.write_pandas", return_value=None)
    adapter = make_mocked_engine_adapter(SnowflakeEngineAdapter)
    adapter.DEFAULT_BATCH_SIZE = 1

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query(
        "other_db.test_table", df, {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")}
    )
    assert 'USE SCHEMA "other_db"' in to_sql_calls(adapter)

    adapter.replace_query(
        "other_catalog.other_db.test_table",
        df,
        {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
    )
    assert 'USE SCHEMA "other_catalog"."other_db"' in to_sql_calls(adapter)
