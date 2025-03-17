# type: ignore
import typing as t

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from unittest.mock import PropertyMock
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import RedshiftEngineAdapter
from sqlmesh.utils.errors import SQLMeshError
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
        'CREATE VIEW "__temp_ctas_test_random_id" AS SELECT "char", "char1" + 1 AS "char1", "char2" AS "char2", "varchar", "varchar256", "varchar2" FROM (SELECT * FROM "table") WITH NO SCHEMA BINDING',
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
        'CREATE VIEW "__temp_ctas_test_random_id" AS SELECT "a", "b", "x" + 1 AS "c", "d" AS "d", "e" FROM (SELECT * FROM "table") WITH NO SCHEMA BINDING',
        'DROP VIEW IF EXISTS "__temp_ctas_test_random_id" CASCADE',
        'CREATE TABLE "test_schema"."test_table" ("a" VARCHAR(MAX), "b" VARCHAR(60), "c" VARCHAR(MAX), "d" VARCHAR(MAX), "e" TIMESTAMP)',
    ]

    columns_mock.assert_called_once_with(exp.table_("__temp_ctas_test_random_id", quoted=True))


def test_create_table_recursive_cte(adapter: t.Callable, mocker: MockerFixture):
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
            "WITH RECURSIVE cte AS (SELECT * FROM table WHERE FALSE LIMIT 0) SELECT a, b, x + 1 AS c, d AS d, e FROM cte WHERE d > 0 AND FALSE LIMIT 0",
            dialect="redshift",
        ),
        exists=False,
    )

    assert to_sql_calls(adapter) == [
        'CREATE VIEW "__temp_ctas_test_random_id" AS WITH RECURSIVE "cte" AS (SELECT * FROM "table") SELECT "a", "b", "x" + 1 AS "c", "d" AS "d", "e" FROM "cte"',
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


def test_alter_table_drop_column_cascade(adapter: t.Callable):
    current_table_name = "test_table"
    target_table_name = "target_table"

    def table_columns(table_name: str) -> t.Dict[str, exp.DataType]:
        if table_name == current_table_name:
            return {"id": exp.DataType.build("int"), "test_column": exp.DataType.build("int")}
        else:
            return {"id": exp.DataType.build("int")}

    adapter.columns = table_columns

    adapter.alter_table(adapter.get_alter_expressions(current_table_name, target_table_name))
    assert to_sql_calls(adapter) == [
        'ALTER TABLE "test_table" DROP COLUMN "test_column" CASCADE',
    ]


def test_merge(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.enable_merge",
        new_callable=PropertyMock(return_value=True),
    )

    adapter.merge(
        target_table=exp.to_table("target_table_name"),
        source_table=t.cast(exp.Select, parse_one('SELECT "ID", ts, val FROM source')),
        columns_to_types={
            "ID": exp.DataType.build("int"),
            "ts": exp.DataType.build("timestamp"),
            "val": exp.DataType.build("int"),
        },
        unique_key=[exp.to_identifier("ID", quoted=True)],
    )

    # Test additional predicates in the merge_filter
    adapter.merge(
        target_table=exp.to_table("target_table_name"),
        source_table=t.cast(exp.Select, parse_one('SELECT "ID", ts, val FROM source')),
        columns_to_types={
            "ID": exp.DataType.build("int"),
            "ts": exp.DataType.build("timestamp"),
            "val": exp.DataType.build("int"),
        },
        unique_key=[exp.to_identifier("ID", quoted=True)],
        merge_filter=exp.and_(
            exp.and_(exp.column("ID", "__MERGE_SOURCE__") > 0),
            exp.column("ts", "__MERGE_TARGET__") < exp.column("ts", "__MERGE_SOURCE__"),
        ),
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        'MERGE INTO "target_table_name" USING (SELECT "ID", "ts", "val" FROM "source") AS "__MERGE_SOURCE__" ON "target_table_name"."ID" = "__MERGE_SOURCE__"."ID" WHEN MATCHED THEN UPDATE SET "ID" = "__MERGE_SOURCE__"."ID", "ts" = "__MERGE_SOURCE__"."ts", "val" = "__MERGE_SOURCE__"."val" WHEN NOT MATCHED THEN INSERT ("ID", "ts", "val") VALUES ("__MERGE_SOURCE__"."ID", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")',
        'MERGE INTO "target_table_name" USING (SELECT "ID", "ts", "val" FROM "source") AS "__MERGE_SOURCE__" ON ("__MERGE_SOURCE__"."ID" > 0 AND "target_table_name"."ts" < "__MERGE_SOURCE__"."ts") AND "target_table_name"."ID" = "__MERGE_SOURCE__"."ID" WHEN MATCHED THEN UPDATE SET "ID" = "__MERGE_SOURCE__"."ID", "ts" = "__MERGE_SOURCE__"."ts", "val" = "__MERGE_SOURCE__"."val" WHEN NOT MATCHED THEN INSERT ("ID", "ts", "val") VALUES ("__MERGE_SOURCE__"."ID", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")',
    ]


def test_merge_when_matched_error(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.enable_merge",
        new_callable=PropertyMock(return_value=True),
    )

    with pytest.raises(
        SQLMeshError,
        match=r".*Redshift only supports a single WHEN MATCHED and WHEN NOT MATCHED clause*",
    ):
        adapter.merge(
            target_table=exp.to_table("target_table_name"),
            source_table=t.cast(exp.Select, parse_one('SELECT "ID", val FROM source')),
            columns_to_types={
                "ID": exp.DataType.build("int"),
                "val": exp.DataType.build("int"),
            },
            unique_key=[exp.to_identifier("ID", quoted=True)],
            when_matched=exp.Whens(
                expressions=[
                    exp.When(
                        matched=True,
                        condition=exp.column("ID", "__MERGE_SOURCE__").eq(exp.Literal.number(1)),
                        then=exp.Update(
                            expressions=[
                                exp.column("val", "__MERGE_TARGET__").eq(
                                    exp.column("val", "__MERGE_SOURCE__")
                                ),
                            ],
                        ),
                    ),
                    exp.When(
                        matched=True,
                        source=False,
                        then=exp.Update(
                            expressions=[
                                exp.column("val", "__MERGE_TARGET__").eq(
                                    exp.column("val", "__MERGE_SOURCE__")
                                ),
                            ],
                        ),
                    ),
                ]
            ),
        )


def test_merge_logical_filter_error(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)
    mocker.patch(
        "sqlmesh.core.engine_adapter.redshift.RedshiftEngineAdapter.enable_merge",
        new_callable=PropertyMock(return_value=False),
    )

    with pytest.raises(
        SQLMeshError,
        match=r".*This engine does not support MERGE expressions and therefore `merge_filter` is not supported.*",
    ):
        adapter.merge(
            target_table=exp.to_table("target_table_name_2"),
            source_table=t.cast(exp.Select, parse_one('SELECT "ID", ts FROM source')),
            columns_to_types={
                "ID": exp.DataType.build("int"),
                "ts": exp.DataType.build("timestamp"),
            },
            unique_key=[exp.to_identifier("ID", quoted=True)],
            merge_filter=exp.and_(
                exp.and_(exp.column("ID", "__MERGE_SOURCE__") > 0),
                exp.column("ts", "__MERGE_TARGET__") < exp.column("ts", "__MERGE_SOURCE__"),
            ),
        )


def test_merge_logical(
    make_mocked_engine_adapter: t.Callable, make_temp_table_name: t.Callable, mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(RedshiftEngineAdapter)

    temp_table_mock = mocker.patch("sqlmesh.core.engine_adapter.EngineAdapter._get_temp_table")
    table_name = "test"
    temp_table_id = "abcdefgh"
    temp_table_mock.return_value = make_temp_table_name(table_name, temp_table_id)

    adapter.merge(
        target_table=exp.to_table("target"),
        source_table=t.cast(exp.Select, parse_one('SELECT "ID", ts FROM source')),
        columns_to_types={
            "ID": exp.DataType.build("int"),
            "ts": exp.DataType.build("timestamp"),
        },
        unique_key=[exp.to_identifier("ID", quoted=True)],
    )

    sql_calls = to_sql_calls(adapter)
    assert sql_calls == [
        'CREATE TABLE "__temp_test_abcdefgh" AS SELECT CAST("ID" AS INTEGER) AS "ID", CAST("ts" AS TIMESTAMP) AS "ts" FROM (SELECT "ID", "ts" FROM "source") AS "_subquery"',
        'DELETE FROM "target" WHERE "ID" IN (SELECT "ID" FROM "__temp_test_abcdefgh")',
        'INSERT INTO "target" ("ID", "ts") SELECT "ID", "ts" FROM (SELECT "ID" AS "ID", "ts" AS "ts", ROW_NUMBER() OVER (PARTITION BY "ID" ORDER BY "ID") AS _row_number FROM "__temp_test_abcdefgh") AS _t WHERE _row_number = 1',
        'DROP TABLE IF EXISTS "__temp_test_abcdefgh"',
    ]
