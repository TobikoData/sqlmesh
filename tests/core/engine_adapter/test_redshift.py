# type: ignore
import typing as t

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import RedshiftEngineAdapter
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.engine, pytest.mark.redshift]


@pytest.fixture
def adapter(make_mocked_engine_adapter):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    adapter.cursor.fetchall.return_value = []
    return adapter


def test_columns(adapter: t.Callable):
    adapter.cursor.fetchall.return_value = [("col", "INT")]
    resp = adapter.columns("db.table")
    adapter.cursor.execute.assert_called_once_with(
        """SELECT "column_name", "data_type", "character_maximum_length", "numeric_precision", "numeric_scale" FROM "svv_columns" WHERE "table_name" = 'table' AND "table_schema" = 'db'"""
    )
    assert resp == {"col": exp.DataType.build("INT")}


def test_varchar_size_workaround(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)

    columns = {
        "char": exp.DataType.build("CHAR", dialect=adapter.dialect),
        "char1": exp.DataType.build("CHAR(1)", dialect=adapter.dialect),
        "char2": exp.DataType.build("CHAR(2)", dialect=adapter.dialect),
        "varchar": exp.DataType.build("VARCHAR", dialect=adapter.dialect),
        "varchar256": exp.DataType.build("VARCHAR(256)", dialect=adapter.dialect),
        "varchar2": exp.DataType.build("VARCHAR(2)", dialect=adapter.dialect),
    }

    assert adapter._default_precision_to_max(columns) == {
        "char": exp.DataType.build("CHAR", dialect=adapter.dialect),
        "char1": exp.DataType.build("CHAR(max)", dialect=adapter.dialect),
        "char2": exp.DataType.build("CHAR(2)", dialect=adapter.dialect),
        "varchar": exp.DataType.build("VARCHAR", dialect=adapter.dialect),
        "varchar256": exp.DataType.build("VARCHAR(max)", dialect=adapter.dialect),
        "varchar2": exp.DataType.build("VARCHAR(2)", dialect=adapter.dialect),
    }

    mocker.patch(
        "sqlmesh.core.engine_adapter.base.random_id",
        return_value="test_random_id",
    )

    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=True,
    )

    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.columns",
        return_value=columns,
    )

    adapter.ctas(
        table_name="test_schema.test_table",
        query_or_df=parse_one(
            "SELECT char, char1 + 1 AS char1, char2 AS char2, varchar, varchar256, varchar2 FROM (SELECT * FROM table WHERE FALSE LIMIT 0) WHERE d > 0 AND FALSE LIMIT 0"
        ),
        exists=False,
    )

    assert to_sql_calls(adapter) == [
        'CREATE VIEW "__temp_ctas_test_random_id" AS SELECT "char", "char1" + 1 AS "char1", "char2" AS "char2", "varchar", "varchar256", "varchar2" FROM (SELECT * FROM "table")',
        'DROP VIEW IF EXISTS "__temp_ctas_test_random_id" CASCADE',
        'CREATE TABLE "test_schema"."test_table" ("char" CHAR, "char1" CHAR(max), "char2" CHAR(2), "varchar" VARCHAR, "varchar256" VARCHAR(max), "varchar2" VARCHAR(2))',
    ]


def test_create_table_from_query_exists_no_if_not_exists(
    adapter: t.Callable, mocker: MockerFixture
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.base.random_id",
        return_value="test_random_id",
    )

    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=True,
    )

    columns_mock = mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.columns",
        return_value={
            "a": exp.DataType.build("VARCHAR(MAX)", dialect="redshift"),
            "b": exp.DataType.build("VARCHAR(60)", dialect="redshift"),
            "c": exp.DataType.build("VARCHAR(MAX)", dialect="redshift"),
            "d": exp.DataType.build("VARCHAR(MAX)", dialect="redshift"),
            "e": exp.DataType.build("TIMESTAMP", dialect="redshift"),
        },
    )

    adapter.ctas(
        table_name="test_schema.test_table",
        query_or_df=parse_one(
            "SELECT a, b, x + 1 AS c, d AS d, e FROM (SELECT * FROM table WHERE FALSE LIMIT 0) WHERE d > 0 AND FALSE LIMIT 0"
        ),
        exists=False,
    )

    assert to_sql_calls(adapter) == [
        'CREATE VIEW "__temp_ctas_test_random_id" AS SELECT "a", "b", "x" + 1 AS "c", "d" AS "d", "e" FROM (SELECT * FROM "table")',
        'DROP VIEW IF EXISTS "__temp_ctas_test_random_id" CASCADE',
        'CREATE TABLE "test_schema"."test_table" ("a" VARCHAR(MAX), "b" VARCHAR(60), "c" VARCHAR(MAX), "d" VARCHAR(MAX), "e" TIMESTAMP)',
    ]

    columns_mock.assert_called_once_with(exp.table_("__temp_ctas_test_random_id", quoted=True))


def test_create_table_from_query_exists_and_if_not_exists(
    adapter: t.Callable, mocker: MockerFixture
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=True,
    )
    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=True,
    )

    adapter.cursor.execute.assert_not_called()


def test_create_table_from_query_not_exists_if_not_exists(
    adapter: t.Callable, mocker: MockerFixture
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=True,
    )

    adapter.cursor.execute.assert_called_with(
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"'
    )


def test_create_table_from_query_not_exists_no_if_not_exists(
    adapter: t.Callable, mocker: MockerFixture
):
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    adapter.ctas(
        table_name="test_table",
        query_or_df=parse_one("SELECT cola FROM table"),
        exists=False,
    )

    adapter.cursor.execute.assert_called_with(
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"'
    )


def test_values_to_sql(adapter: t.Callable, mocker: MockerFixture):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    result = adapter._values_to_sql(
        values=list(df.itertuples(index=False, name=None)),
        columns_to_types={"a": exp.DataType.build("int"), "b": exp.DataType.build("int")},
        batch_start=0,
        batch_end=2,
    )
    # 3,6 is missing since the batch range excluded it
    assert (
        result.sql(dialect="redshift")
        == "SELECT CAST(a AS INTEGER) AS a, CAST(b AS INTEGER) AS b FROM (SELECT 1 AS a, 4 AS b UNION ALL SELECT 2, 5) AS t"
    )


def test_replace_query_with_query(adapter: t.Callable, mocker: MockerFixture):
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.columns",
        return_value={"cola": exp.DataType(this=exp.DataType.Type.INT)},
    )

    adapter.replace_query(table_name="test_table", query_or_df=parse_one("SELECT cola FROM table"))

    assert to_sql_calls(adapter) == [
        'CREATE TABLE "test_table" AS SELECT "cola" FROM "table"',
    ]


def test_replace_query_with_df_table_exists(adapter: t.Callable, mocker: MockerFixture):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=True,
    )
    call_counter = 0

    def mock_table(*args, **kwargs):
        nonlocal call_counter
        call_counter += 1
        return f"temp_table_{call_counter}"

    mock_temp_table = mocker.MagicMock(side_effect=mock_table)
    mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table", mock_temp_table)

    adapter.replace_query(
        table_name="test_table",
        query_or_df=df,
        columns_to_types={
            "a": exp.DataType.build("int"),
            "b": exp.DataType.build("int"),
        },
    )

    adapter.cursor.begin.assert_called_once()
    adapter.cursor.commit.assert_called_once()

    assert to_sql_calls(adapter) == [
        'CREATE TABLE "temp_table_1" ("a" INTEGER, "b" INTEGER)',
        'INSERT INTO "temp_table_1" ("a", "b") SELECT CAST("a" AS INTEGER) AS "a", CAST("b" AS INTEGER) AS "b" FROM (SELECT 1 AS "a", 4 AS "b" UNION ALL SELECT 2, 5 UNION ALL SELECT 3, 6) AS "t"',
        'ALTER TABLE "test_table" RENAME TO "temp_table_2"',
        'ALTER TABLE "temp_table_1" RENAME TO "test_table"',
        'DROP TABLE IF EXISTS "temp_table_2"',
    ]


def test_replace_query_with_df_table_not_exists(adapter: t.Callable, mocker: MockerFixture):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.table_exists",
        return_value=False,
    )
    adapter.replace_query(
        table_name="test_table",
        query_or_df=df,
        columns_to_types={
            "a": exp.DataType.build("int"),
            "b": exp.DataType.build("int"),
        },
    )

    assert to_sql_calls(adapter) == [
        'CREATE TABLE "test_table" AS SELECT CAST("a" AS INTEGER) AS "a", CAST("b" AS INTEGER) AS "b" FROM (SELECT 1 AS "a", 4 AS "b" UNION ALL SELECT 2, 5 UNION ALL SELECT 3, 6) AS "t"',
    ]


def test_table_exists_db_table(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    connection_mock.cursor.return_value.fetchone.return_value = (1,)

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    assert adapter.table_exists(table_name=exp.to_table("some_db.some_table"))

    cursor_mock.execute.assert_called_once_with(
        """SELECT 1 FROM "information_schema"."tables" WHERE "table_name" = 'some_table' AND "table_schema" = 'some_db'"""
    )


def test_table_exists_table_only(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock
    connection_mock.cursor.return_value.fetchone.return_value = None

    adapter = RedshiftEngineAdapter(lambda: connection_mock)
    assert not adapter.table_exists(table_name=exp.to_table("some_table"))

    cursor_mock.execute.assert_called_once_with(
        """SELECT 1 FROM "information_schema"."tables" WHERE "table_name" = 'some_table'"""
    )


def test_create_view(adapter: t.Callable):
    adapter.create_view(
        view_name="test_view",
        query_or_df=parse_one("SELECT cola FROM table"),
        columns_to_types={
            "a": exp.DataType.build("int"),
            "b": exp.DataType.build("int"),
        },
    )

    assert to_sql_calls(adapter) == [
        'DROP VIEW IF EXISTS "test_view" CASCADE',
        'CREATE VIEW "test_view" ("a", "b") AS SELECT "cola" FROM "table" WITH NO SCHEMA BINDING',
    ]
