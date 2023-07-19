# type: ignore
import typing as t
from unittest.mock import call

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import EngineAdapter, EngineAdapterWithIndexSupport
from sqlmesh.core.engine_adapter.base import InsertOverwriteStrategy
from sqlmesh.core.schema_diff import SchemaDiffer, TableAlterOperation


def test_create_view(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"))
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"), replace=False)

    cursor_mock.execute.assert_has_calls(
        [
            call('CREATE OR REPLACE VIEW "test_view" AS SELECT "a" FROM "tbl"'),
            call('CREATE VIEW "test_view" AS SELECT "a" FROM "tbl"'),
        ]
    )


def test_create_materialized_view(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.SUPPORTS_MATERIALIZED_VIEWS = True
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"), materialized=True)
    adapter.create_view(
        "test_view", parse_one("SELECT a FROM tbl"), replace=False, materialized=True
    )

    cursor_mock.execute.assert_has_calls(
        [
            call('CREATE OR REPLACE MATERIALIZED VIEW "test_view" AS SELECT "a" FROM "tbl"'),
            call('CREATE MATERIALIZED VIEW "test_view" AS SELECT "a" FROM "tbl"'),
        ]
    )


def test_create_schema(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.create_schema("test_schema")
    adapter.create_schema("test_schema", ignore_if_exists=False)

    cursor_mock.execute.assert_has_calls(
        [
            call('CREATE SCHEMA IF NOT EXISTS "test_schema"'),
            call('CREATE SCHEMA "test_schema"'),
        ]
    )


def test_columns(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()

    connection_mock.cursor.return_value = cursor_mock
    cursor_mock.fetchall.return_value = [
        ("id", "int"),
        ("name", "string"),
        ("price", "double"),
        ("ds", "string"),
        ("# Partition Information", ""),
        ("# col_name", "data_type"),
        ("ds", "string"),
    ]

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    assert adapter.columns("test_table") == {
        "id": exp.DataType.build("int"),
        "name": exp.DataType.build("string"),
        "price": exp.DataType.build("double"),
        "ds": exp.DataType.build("string"),
    }

    cursor_mock.execute.assert_called_once_with('DESCRIBE "test_table"')


def test_table_exists(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    assert adapter.table_exists("test_table")
    cursor_mock.execute.assert_called_once_with('DESCRIBE "test_table"')

    cursor_mock = mocker.Mock()
    cursor_mock.execute.side_effect = RuntimeError("error")
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    assert not adapter.table_exists("test_table")
    cursor_mock.execute.assert_called_once_with('DESCRIBE "test_table"')


def test_insert_overwrite_by_time_partition(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter._insert_overwrite_by_condition(
        "test_table",
        parse_one("SELECT a FROM tbl"),
        where=parse_one("b BETWEEN '2022-01-01' and '2022-01-02'"),
        columns_to_types={"a": exp.DataType.build("INT")},
    )

    cursor_mock.begin.assert_called_once()
    cursor_mock.commit.assert_called_once()

    cursor_mock.execute.assert_has_calls(
        [
            call("""DELETE FROM "test_table" WHERE "b" BETWEEN '2022-01-01' AND '2022-01-02'"""),
            call('INSERT INTO "test_table" ("a") SELECT "a" FROM "tbl"'),
        ]
    )


def test_insert_overwrite_by_time_partition_supports_insert_overwrite(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    adapter._insert_overwrite_by_condition(
        "test_table",
        parse_one("SELECT a, b FROM tbl"),
        where=parse_one("b BETWEEN '2022-01-01' and '2022-01-02'"),
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("STRING")},
    )

    cursor_mock.execute.assert_called_once_with(
        """INSERT OVERWRITE TABLE "test_table" ("a", "b") SELECT * FROM (SELECT "a", "b" FROM "tbl") AS "_subquery" WHERE "b" BETWEEN '2022-01-01' AND '2022-01-02'"""
    )


def test_insert_overwrite_by_time_partition_supports_insert_overwrite_pandas(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    df = pd.DataFrame({"a": [1, 2], "ds": ["2022-01-01", "2022-01-02"]})
    adapter._insert_overwrite_by_condition(
        "test_table",
        df,
        where=parse_one("ds BETWEEN '2022-01-01' and '2022-01-02'"),
        columns_to_types={"a": exp.DataType.build("INT"), "ds": exp.DataType.build("STRING")},
    )

    cursor_mock.execute.assert_called_once_with(
        """INSERT OVERWRITE TABLE "test_table" ("a", "ds") SELECT * FROM (SELECT CAST("a" AS INT) AS "a", CAST("ds" AS TEXT) AS "ds" FROM (VALUES (1, '2022-01-01'), (2, '2022-01-02')) AS "test_table"("a", "ds")) AS "_subquery" WHERE "ds" BETWEEN '2022-01-01' AND '2022-01-02'"""
    )


def test_insert_overwrite_by_time_partition_replace_where(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.REPLACE_WHERE
    adapter._insert_overwrite_by_condition(
        "test_table",
        parse_one("SELECT a, b FROM tbl"),
        where=parse_one("b BETWEEN '2022-01-01' and '2022-01-02'"),
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("STRING")},
    )

    cursor_mock.execute.assert_called_once_with(
        """INSERT INTO "test_table" ("a", "b") REPLACE WHERE "b" BETWEEN '2022-01-01' AND '2022-01-02' SELECT * FROM (SELECT "a", "b" FROM "tbl") AS "_subquery" WHERE "b" BETWEEN '2022-01-01' AND '2022-01-02'"""
    )


def test_insert_overwrite_by_time_partition_replace_where_pandas(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.REPLACE_WHERE
    df = pd.DataFrame({"a": [1, 2], "ds": ["2022-01-01", "2022-01-02"]})
    adapter._insert_overwrite_by_condition(
        "test_table",
        df,
        where=parse_one("ds BETWEEN '2022-01-01' and '2022-01-02'"),
        columns_to_types={"a": exp.DataType.build("INT"), "ds": exp.DataType.build("STRING")},
    )

    cursor_mock.execute.assert_called_once_with(
        """INSERT INTO "test_table" ("a", "ds") REPLACE WHERE "ds" BETWEEN '2022-01-01' AND '2022-01-02' SELECT * FROM (SELECT CAST("a" AS INT) AS "a", CAST("ds" AS TEXT) AS "ds" FROM (VALUES (1, '2022-01-01'), (2, '2022-01-02')) AS "test_table"("a", "ds")) AS "_subquery" WHERE "ds" BETWEEN '2022-01-01' AND '2022-01-02'"""
    )


def test_insert_append_query(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.insert_append(
        "test_table",
        parse_one("SELECT a FROM tbl"),
        columns_to_types={"a": exp.DataType.build("INT")},
    )

    cursor_mock.execute.assert_called_once_with(
        'INSERT INTO "test_table" ("a") SELECT "a" FROM "tbl"'
    )


def test_insert_append_pandas(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.insert_append(
        "test_table",
        df,
        columns_to_types={
            "a": exp.DataType.build("INT"),
            "b": exp.DataType.build("INT"),
        },
    )

    cursor_mock.begin.assert_called_once()
    cursor_mock.commit.assert_called_once()

    cursor_mock.execute.assert_has_calls(
        [
            call(
                'INSERT INTO "test_table" ("a", "b") SELECT CAST("a" AS INT) AS "a", CAST("b" AS INT) AS "b" FROM (VALUES (1, 4), (2, 5), (3, 6)) AS "t"("a", "b")',
            ),
        ]
    )


def test_create_table(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.create_table("test_table", columns_to_types)

    cursor_mock.execute.assert_called_once_with(
        'CREATE TABLE IF NOT EXISTS "test_table" ("cola" INT, "colb" TEXT)'
    )


def test_create_table_properties(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.create_table(
        "test_table",
        columns_to_types,
        partitioned_by=[exp.to_column("colb")],
        storage_format="ICEBERG",
    )

    cursor_mock.execute.assert_called_once_with(
        'CREATE TABLE IF NOT EXISTS "test_table" ("cola" INT, "colb" TEXT)'
    )


@pytest.mark.parametrize(
    "schema_differ_config, current_table, target_table, expected_final_structure, expected",
    [
        (
            {
                "support_positional_add": True,
                "support_nested_operations": True,
                "array_element_selector": "element",
            },
            {
                "a": "INT",
                "b": "TEXT",
                "c": "INT",
                "d": "INT",
                "nested": "STRUCT<nested_a INT, nested_c INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_c INT>>",
            },
            {
                "f": "VARCHAR(100)",
                "a": "INT",
                "e": "TEXT",
                "b": "TEXT",
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_c INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_c INT>>",
            },
            {
                "f": "VARCHAR(100)",
                "a": "INT",
                "e": "TEXT",
                "b": "TEXT",
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_c INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_c INT>>",
            },
            [
                'ALTER TABLE "test_table" DROP COLUMN "c"',
                'ALTER TABLE "test_table" DROP COLUMN "d"',
                'ALTER TABLE "test_table" ADD COLUMN "f" VARCHAR(100) FIRST',
                'ALTER TABLE "test_table" ADD COLUMN "e" TEXT AFTER "a"',
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_b" INT AFTER "nested_a"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_b" INT AFTER "array_a"',
            ],
        ),
        (
            {
                "support_nested_operations": True,
                "array_element_selector": "element",
            },
            {
                "a": "INT",
                "b": "TEXT",
                "c": "INT",
                "d": "INT",
                "nested": "STRUCT<nested_a INT, nested_c INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_c INT>>",
            },
            {
                "f": "VARCHAR(100)",
                "a": "INT",
                "e": "TEXT",
                "b": "TEXT",
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_c INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_c INT>>",
            },
            {
                "a": "INT",
                "b": "TEXT",
                "nested": "STRUCT<nested_a INT, nested_c INT, nested_b INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_c INT, array_b INT>>",
                "f": "VARCHAR(100)",
                "e": "TEXT",
            },
            [
                'ALTER TABLE "test_table" DROP COLUMN "c"',
                'ALTER TABLE "test_table" DROP COLUMN "d"',
                'ALTER TABLE "test_table" ADD COLUMN "f" VARCHAR(100)',
                'ALTER TABLE "test_table" ADD COLUMN "e" TEXT',
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_b" INT',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_b" INT',
            ],
        ),
        (
            {
                "array_element_selector": "element",
            },
            {
                "a": "INT",
                "b": "TEXT",
                "c": "INT",
                "d": "INT",
                "nested": "STRUCT<nested_a INT, nested_c INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_c INT>>",
            },
            {
                "f": "VARCHAR(100)",
                "a": "INT",
                "e": "TEXT",
                "b": "TEXT",
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_c INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_c INT>>",
            },
            {
                "a": "INT",
                "b": "TEXT",
                "f": "VARCHAR(100)",
                "e": "TEXT",
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_c INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_c INT>>",
            },
            [
                'ALTER TABLE "test_table" DROP COLUMN "c"',
                'ALTER TABLE "test_table" DROP COLUMN "d"',
                'ALTER TABLE "test_table" ADD COLUMN "f" VARCHAR(100)',
                'ALTER TABLE "test_table" ADD COLUMN "e" TEXT',
                'ALTER TABLE "test_table" DROP COLUMN "nested"',
                'ALTER TABLE "test_table" ADD COLUMN "nested" STRUCT<"nested_a" INT, "nested_b" INT, "nested_c" INT>',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col" ARRAY<STRUCT<"array_a" INT, "array_b" INT, "array_c" INT>>',
            ],
        ),
        (
            {
                "array_element_selector": "element",
            },
            {
                "a": "INT",
                "b": "TEXT",
                "c": "INT",
                "d": "INT",
                "nested": "STRUCT<nested_a INT, nested_c INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_c INT>>",
            },
            {
                "f": "VARCHAR(100)",
                "a": "INT",
                "e": "TEXT",
                "b": "TEXT",
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_c INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_c INT>>",
            },
            {
                "a": "INT",
                "b": "TEXT",
                "f": "VARCHAR(100)",
                "e": "TEXT",
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_c INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_c INT>>",
            },
            [
                'ALTER TABLE "test_table" DROP COLUMN "c"',
                'ALTER TABLE "test_table" DROP COLUMN "d"',
                'ALTER TABLE "test_table" ADD COLUMN "f" VARCHAR(100)',
                'ALTER TABLE "test_table" ADD COLUMN "e" TEXT',
                'ALTER TABLE "test_table" DROP COLUMN "nested"',
                'ALTER TABLE "test_table" ADD COLUMN "nested" STRUCT<"nested_a" INT, "nested_b" INT, "nested_c" INT>',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col" ARRAY<STRUCT<"array_a" INT, "array_b" INT, "array_c" INT>>',
            ],
        ),
        # Test multiple operations on a column with positional and nested features enabled
        (
            {
                "support_positional_add": True,
                "support_nested_operations": True,
                "array_element_selector": "element",
            },
            {
                "nested": """STRUCT<nested_a INT, "nested_c" INT, nested_e INT>""",
                "array_col": """ARRAY<STRUCT<array_a INT, "array_c" INT, array_e INT>>""",
            },
            {
                "nested": """STRUCT<nested_a INT, "nested_b" INT, nested_d INT, nested_e INT>""",
                "array_col": """ARRAY<STRUCT<array_a INT, "array_b" INT, array_d INT, array_e INT>>""",
            },
            {
                "nested": """STRUCT<nested_a INT, "nested_b" INT, nested_d INT, nested_e INT>""",
                "array_col": """ARRAY<STRUCT<array_a INT, "array_b" INT, array_d INT, array_e INT>>""",
            },
            [
                'ALTER TABLE "test_table" DROP COLUMN "nested"."nested_c"',
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_b" INT AFTER "nested_a"',
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_d" INT AFTER "nested_b"',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"."element"."array_c"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_b" INT AFTER "array_a"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_d" INT AFTER "array_b"',
            ],
        ),
        # Test multiple operations on a column with positional and nested features enabled and that when adding
        # last we don't include position since it is not needed
        (
            {
                "support_positional_add": True,
                "support_nested_operations": True,
                "array_element_selector": "element",
            },
            {
                "nested": "STRUCT<nested_a INT, nested_c INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_c INT>>",
            },
            {
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_d INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_d INT>>",
            },
            {
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_d INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_d INT>>",
            },
            [
                'ALTER TABLE "test_table" DROP COLUMN "nested"."nested_c"',
                # Position is not included since we are adding to last so we don't need to specify position
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_b" INT',
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_d" INT',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"."element"."array_c"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_b" INT',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_d" INT',
            ],
        ),
        # Test multiple operations on a column with positional and no nested features enabled
        (
            {
                "support_positional_add": True,
                "array_element_selector": "element",
            },
            {
                "nested": "STRUCT<nested_a INT, nested_c INT, nested_e INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_c INT, array_e INT>>",
                "col_c": "INT",
            },
            {
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_d INT, nested_e INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_d INT, array_e INT>>",
                "col_c": "INT",
            },
            {
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_d INT, nested_e INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_d INT, array_e INT>>",
                "col_c": "INT",
            },
            [
                'ALTER TABLE "test_table" DROP COLUMN "nested"',
                'ALTER TABLE "test_table" ADD COLUMN "nested" STRUCT<"nested_a" INT, "nested_b" INT, "nested_d" INT, "nested_e" INT> FIRST',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col" ARRAY<STRUCT<"array_a" INT, "array_b" INT, "array_d" INT, "array_e" INT>> AFTER "nested"',
            ],
        ),
        # Test multiple operations on a column with no positional and nested features enabled
        (
            {
                "support_nested_operations": True,
                "array_element_selector": "element",
            },
            {
                "nested": "STRUCT<nested_a INT, nested_c INT, nested_e INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_c INT, nested_e INT>>",
                "col_c": "INT",
            },
            {
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_d INT, nested_e INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_d INT, nested_e INT>>",
                "col_c": "INT",
            },
            {
                "nested": "STRUCT<nested_a INT, nested_e INT, nested_b INT, nested_d INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, nested_e INT, array_b INT, array_d INT>>",
                "col_c": "INT",
            },
            [
                'ALTER TABLE "test_table" DROP COLUMN "nested"."nested_c"',
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_b" INT',
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_d" INT',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"."element"."array_c"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_b" INT',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_d" INT',
            ],
        ),
        # Test multiple operations on a column with no positional or nested features enabled
        (
            {
                "array_element_selector": "element",
            },
            {
                "nested": "STRUCT<nested_a INT, nested_c INT, nested_e INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_c INT, array_e INT>>",
                "col_c": "INT",
            },
            {
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_d INT, nested_e INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_d INT, array_e INT>>",
                "col_c": "INT",
            },
            {
                "col_c": "INT",
                "nested": "STRUCT<nested_a INT, nested_b INT, nested_d INT, nested_e INT>",
                "array_col": "ARRAY<STRUCT<array_a INT, array_b INT, array_d INT, array_e INT>>",
            },
            [
                'ALTER TABLE "test_table" DROP COLUMN "nested"',
                'ALTER TABLE "test_table" ADD COLUMN "nested" STRUCT<"nested_a" INT, "nested_b" INT, "nested_d" INT, "nested_e" INT>',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col" ARRAY<STRUCT<"array_a" INT, "array_b" INT, "array_d" INT, "array_e" INT>>',
            ],
        ),
        # Test deeply nested structures
        (
            {
                "support_nested_operations": True,
                "array_element_selector": "element",
            },
            {
                "nested": """STRUCT<nested_1_a STRUCT<"nested_2_a" ARRAY<STRUCT<nested_3_a STRUCT<nested_4_a ARRAY<STRUCT<"nested_5_a" ARRAY<STRUCT<nested_6_a INT>>>>>>>>>""",
            },
            {
                "nested": """STRUCT<nested_1_a STRUCT<"nested_2_a" ARRAY<STRUCT<nested_3_a STRUCT<nested_4_a ARRAY<STRUCT<"nested_5_a" ARRAY<STRUCT<nested_6_a INT, nested_6_b INT>>>>>>>>>""",
            },
            {
                "nested": """STRUCT<nested_1_a STRUCT<"nested_2_a" ARRAY<STRUCT<nested_3_a STRUCT<nested_4_a ARRAY<STRUCT<"nested_5_a" ARRAY<STRUCT<nested_6_a INT, nested_6_b INT>>>>>>>>>""",
            },
            [
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_1_a"."nested_2_a"."element"."nested_3_a"."nested_4_a"."element"."nested_5_a"."element"."nested_6_b" INT',
            ],
        ),
    ],
)
def test_alter_table(
    mocker: MockerFixture,
    schema_differ_config: t.Dict[str, t.Any],
    current_table: t.Dict[str, str],
    target_table: t.Dict[str, str],
    expected_final_structure: t.Dict[str, str],
    expected: t.List[str],
):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")
    adapter.SCHEMA_DIFFER = SchemaDiffer(**schema_differ_config)
    original_from_structs = adapter.SCHEMA_DIFFER._from_structs

    def _from_structs(
        current_struct: exp.DataType, new_struct: exp.DataType
    ) -> t.List[TableAlterOperation]:
        operations = original_from_structs(current_struct, new_struct)
        assert (
            operations[-1].expected_table_struct.sql()
            == SchemaDiffer._dict_to_struct(expected_final_structure).sql()
        )
        return operations

    mocker.patch.object(SchemaDiffer, "_from_structs", side_effect=_from_structs)

    current_table_name = "test_table"
    target_table_name = "target_table"

    def table_columns(table_name: str) -> t.Dict[str, exp.DataType]:
        if table_name == current_table_name:
            return {k: exp.DataType.build(v) for k, v in current_table.items()}
        else:
            return {k: exp.DataType.build(v) for k, v in target_table.items()}

    adapter.columns = table_columns

    adapter.alter_table(
        current_table_name,
        target_table_name,
    )

    cursor_mock.begin.assert_called_once()
    cursor_mock.commit.assert_called_once()
    cursor_mock.execute.assert_has_calls([call(x) for x in expected])


def test_merge(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.merge(
        target_table="target",
        source_table=t.cast(exp.Select, parse_one("SELECT id, ts, val FROM source")),
        columns_to_types={
            "id": exp.DataType.Type.INT,
            "ts": exp.DataType.Type.TIMESTAMP,
            "val": exp.DataType.Type.INT,
        },
        unique_key=["id"],
    )
    cursor_mock.execute.assert_called_once_with(
        'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT "id", "ts", "val" FROM "source") AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" '
        'WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id", "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts", "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val" '
        'WHEN NOT MATCHED THEN INSERT ("id", "ts", "val") VALUES ("__MERGE_SOURCE__"."id", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")'
    )

    cursor_mock.reset_mock()
    adapter.merge(
        target_table="target",
        source_table=parse_one("SELECT id, ts, val FROM source"),
        columns_to_types={
            "id": exp.DataType.Type.INT,
            "ts": exp.DataType.Type.TIMESTAMP,
            "val": exp.DataType.Type.INT,
        },
        unique_key=["id", "ts"],
    )
    cursor_mock.execute.assert_called_once_with(
        'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT "id", "ts", "val" FROM "source") AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" AND "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts" '
        'WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id", "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts", "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val" '
        'WHEN NOT MATCHED THEN INSERT ("id", "ts", "val") VALUES ("__MERGE_SOURCE__"."id", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")'
    )


def test_merge_pandas(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.merge(
        target_table="target",
        source_table=df,
        columns_to_types={
            "id": exp.DataType.Type.INT,
            "ts": exp.DataType.Type.TIMESTAMP,
            "val": exp.DataType.Type.INT,
        },
        unique_key=["id"],
    )
    cursor_mock.execute.assert_called_once_with(
        'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT CAST("id" AS INT) AS "id", CAST("ts" AS TIMESTAMP) AS "ts", CAST("val" AS INT) AS "val" FROM (VALUES (1, 4), (2, 5), (3, 6)) AS "t"("id", "ts", "val")) AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" '
        'WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id", "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts", "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val" '
        'WHEN NOT MATCHED THEN INSERT ("id", "ts", "val") VALUES ("__MERGE_SOURCE__"."id", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")'
    )

    cursor_mock.reset_mock()
    adapter.merge(
        target_table="target",
        source_table=df,
        columns_to_types={
            "id": exp.DataType.Type.INT,
            "ts": exp.DataType.Type.TIMESTAMP,
            "val": exp.DataType.Type.INT,
        },
        unique_key=["id", "ts"],
    )
    cursor_mock.execute.assert_called_once_with(
        'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT CAST("id" AS INT) AS "id", CAST("ts" AS TIMESTAMP) AS "ts", CAST("val" AS INT) AS "val" FROM (VALUES (1, 4), (2, 5), (3, 6)) AS "t"("id", "ts", "val")) AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" AND "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts" '
        'WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id", "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts", "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val" '
        'WHEN NOT MATCHED THEN INSERT ("id", "ts", "val") VALUES ("__MERGE_SOURCE__"."id", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")'
    )


def test_replace_query(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    cursor_mock.execute.assert_called_once_with(
        'CREATE OR REPLACE TABLE "test_table" AS SELECT "a" FROM "tbl"'
    )


def test_replace_query_pandas(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query("test_table", df, {"a": "int", "b": "int"})

    cursor_mock.execute.assert_has_calls(
        [
            call('DROP TABLE IF EXISTS "test_table"'),
            call('CREATE TABLE IF NOT EXISTS "test_table" ("a" int, "b" int)'),
            call(
                'INSERT INTO "test_table" ("a", "b") SELECT CAST("a" AS INT) AS "a", CAST("b" AS INT) AS "b" FROM (VALUES (1, 4), (2, 5), (3, 6)) AS "t"("a", "b")'
            ),
        ]
    )


def test_create_table_like(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.create_table_like("target_table", "source_table")

    cursor_mock.execute.assert_called_once_with(
        'CREATE TABLE IF NOT EXISTS "target_table" LIKE "source_table"'
    )


def test_create_table_primary_key(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }

    adapter = EngineAdapterWithIndexSupport(lambda: connection_mock, "")  # type: ignore
    adapter.create_table("test_table", columns_to_types, primary_key=("cola", "colb"))

    cursor_mock.execute.assert_called_once_with(
        'CREATE TABLE IF NOT EXISTS "test_table" ("cola" INT, "colb" TEXT, PRIMARY KEY ("cola", "colb"))'
    )


def test_create_index(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapterWithIndexSupport(lambda: connection_mock, "")  # type: ignore
    adapter.create_index("test_table", "test_index", ("cola", "colb"))

    cursor_mock.execute.assert_called_once_with(
        'CREATE INDEX IF NOT EXISTS "test_index" ON "test_table" ("cola", "colb")'
    )


def test_rename_table(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = EngineAdapter(lambda: connection_mock, "")  # type: ignore
    adapter.rename_table("old_table", "new_table")

    cursor_mock.execute.assert_called_once_with('ALTER TABLE "old_table" RENAME TO "new_table"')
