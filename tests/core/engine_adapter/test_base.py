# type: ignore
import typing as t
from datetime import datetime
from unittest.mock import call

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import EngineAdapter, EngineAdapterWithIndexSupport
from sqlmesh.core.engine_adapter.base import InsertOverwriteStrategy
from sqlmesh.core.schema_diff import SchemaDiffer, TableAlterOperation
from sqlmesh.utils.date import to_ds
from tests.core.engine_adapter import to_sql_calls


def test_create_view(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"))
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"), replace=False)
    # Test that `table_properties` are ignored for base engine adapter
    adapter.create_view(
        "test_view",
        parse_one("SELECT a FROM tbl"),
        replace=True,
        table_properties={"a": exp.convert(1)},
    )

    assert to_sql_calls(adapter) == [
        'CREATE OR REPLACE VIEW "test_view" AS SELECT "a" FROM "tbl"',
        'CREATE VIEW "test_view" AS SELECT "a" FROM "tbl"',
        'CREATE OR REPLACE VIEW "test_view" AS SELECT "a" FROM "tbl"',
    ]


def test_create_view_pandas(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)
    adapter.create_view("test_view", pd.DataFrame({"a": [1, 2, 3]}), replace=False)
    adapter.create_view(
        "test_view",
        pd.DataFrame({"a": [1, 2, 3]}),
        replace=True,
        table_properties={"a": exp.convert(1)},
    )

    assert to_sql_calls(adapter) == [
        'CREATE VIEW "test_view" ("a") AS SELECT CAST("a" AS BIGINT) AS "a" FROM (VALUES (1), (2), (3)) AS "t"("a")',
        'CREATE OR REPLACE VIEW "test_view" ("a") AS SELECT CAST("a" AS BIGINT) AS "a" FROM (VALUES (1), (2), (3)) AS "t"("a")',
    ]


def test_create_materialized_view(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)
    adapter.SUPPORTS_MATERIALIZED_VIEWS = True
    adapter.create_view(
        "test_view",
        parse_one("SELECT a FROM tbl"),
        materialized=True,
        columns_to_types={"a": exp.DataType.build("INT")},
    )
    adapter.create_view(
        "test_view",
        parse_one("SELECT a FROM tbl"),
        replace=False,
        materialized=True,
        columns_to_types={"a": exp.DataType.build("INT")},
    )

    adapter.cursor.execute.assert_has_calls(
        [
            call('CREATE OR REPLACE MATERIALIZED VIEW "test_view" AS SELECT "a" FROM "tbl"'),
            call('CREATE MATERIALIZED VIEW "test_view" AS SELECT "a" FROM "tbl"'),
        ]
    )

    adapter.SUPPORTS_MATERIALIZED_VIEW_SCHEMA = True
    adapter.create_view(
        "test_view",
        parse_one("SELECT a, b FROM tbl"),
        replace=False,
        materialized=True,
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
    )
    adapter.create_view(
        "test_view", parse_one("SELECT a, b FROM tbl"), replace=False, materialized=True
    )

    adapter.cursor.execute.assert_has_calls(
        [
            call('CREATE MATERIALIZED VIEW "test_view" ("a", "b") AS SELECT "a", "b" FROM "tbl"'),
            call('CREATE MATERIALIZED VIEW "test_view" AS SELECT "a", "b" FROM "tbl"'),
        ]
    )


def test_create_schema(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)
    adapter.create_schema("test_schema")
    adapter.create_schema("test_schema", ignore_if_exists=False)
    adapter.create_schema("test_schema", catalog_name="test_catalog")

    adapter.cursor.execute.assert_has_calls(
        [
            call('CREATE SCHEMA IF NOT EXISTS "test_schema"'),
            call('CREATE SCHEMA "test_schema"'),
            call('CREATE SCHEMA IF NOT EXISTS "test_catalog"."test_schema"'),
        ]
    )


def test_columns(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)
    adapter.cursor.fetchall.return_value = [
        ("id", "int"),
        ("name", "string"),
        ("price", "double"),
        ("ds", "string"),
        ("# Partition Information", ""),
        ("# col_name", "data_type"),
        ("ds", "string"),
    ]
    assert adapter.columns("test_table") == {
        "id": exp.DataType.build("int"),
        "name": exp.DataType.build("string"),
        "price": exp.DataType.build("double"),
        "ds": exp.DataType.build("string"),
    }

    adapter.cursor.execute.assert_called_once_with('DESCRIBE "test_table"')


def test_table_exists(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)
    assert adapter.table_exists("test_table")
    adapter.cursor.execute.assert_called_once_with('DESCRIBE "test_table"')

    adapter.cursor.reset_mock()
    adapter.cursor.execute.side_effect = RuntimeError("error")
    assert not adapter.table_exists("test_table")
    adapter.cursor.execute.assert_called_once_with('DESCRIBE "test_table"')


def test_insert_overwrite_by_time_partition(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    adapter.insert_overwrite_by_time_partition(
        "test_table",
        parse_one("SELECT a, b FROM tbl"),
        start="2022-01-01",
        end="2022-01-02",
        time_column="b",
        time_formatter=lambda x, _: exp.Literal.string(to_ds(x)),
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("STRING")},
    )

    adapter.cursor.begin.assert_called_once()
    adapter.cursor.commit.assert_called_once()

    assert to_sql_calls(adapter) == [
        """DELETE FROM "test_table" WHERE "b" BETWEEN '2022-01-01' AND '2022-01-02'""",
        """INSERT INTO "test_table" ("a", "b") SELECT "a", "b" FROM (SELECT "a", "b" FROM "tbl") AS "_subquery" WHERE "b" BETWEEN '2022-01-01' AND '2022-01-02'""",
    ]


def test_insert_overwrite_by_time_partition_supports_insert_overwrite(
    make_mocked_engine_adapter: t.Callable,
):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE

    adapter.insert_overwrite_by_time_partition(
        "test_table",
        parse_one("SELECT a, b FROM tbl"),
        start="2022-01-01",
        end="2022-01-02",
        time_column="b",
        time_formatter=lambda x, _: exp.Literal.string(to_ds(x)),
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("STRING")},
    )

    adapter.cursor.execute.assert_called_once_with(
        """INSERT OVERWRITE TABLE "test_table" ("a", "b") SELECT "a", "b" FROM (SELECT "a", "b" FROM "tbl") AS "_subquery" WHERE "b" BETWEEN '2022-01-01' AND '2022-01-02'"""
    )


def test_insert_overwrite_by_time_partition_supports_insert_overwrite_pandas(
    make_mocked_engine_adapter: t.Callable,
):
    adapter = make_mocked_engine_adapter(EngineAdapter)
    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    df = pd.DataFrame({"a": [1, 2], "ds": ["2022-01-01", "2022-01-02"]})

    adapter.insert_overwrite_by_time_partition(
        "test_table",
        df,
        start="2022-01-01",
        end="2022-01-02",
        time_column="ds",
        time_formatter=lambda x, _: exp.Literal.string(to_ds(x)),
        columns_to_types={"a": exp.DataType.build("INT"), "ds": exp.DataType.build("STRING")},
    )

    assert to_sql_calls(adapter) == [
        """INSERT OVERWRITE TABLE "test_table" ("a", "ds") SELECT "a", "ds" FROM (SELECT CAST("a" AS INT) AS "a", CAST("ds" AS TEXT) AS "ds" FROM (VALUES (1, '2022-01-01'), (2, '2022-01-02')) AS "t"("a", "ds")) AS "_subquery" WHERE "ds" BETWEEN '2022-01-01' AND '2022-01-02'"""
    ]


def test_insert_overwrite_by_time_partition_replace_where(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)
    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.REPLACE_WHERE

    adapter.insert_overwrite_by_time_partition(
        "test_table",
        parse_one("SELECT a, b FROM tbl"),
        start="2022-01-01",
        end="2022-01-02",
        time_column="b",
        time_formatter=lambda x, _: exp.Literal.string(to_ds(x)),
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("STRING")},
    )

    assert to_sql_calls(adapter) == [
        """INSERT INTO "test_table" REPLACE WHERE "b" BETWEEN '2022-01-01' AND '2022-01-02' SELECT "a", "b" FROM (SELECT "a", "b" FROM "tbl") AS "_subquery" WHERE "b" BETWEEN '2022-01-01' AND '2022-01-02'"""
    ]


def test_insert_overwrite_by_time_partition_replace_where_pandas(
    make_mocked_engine_adapter: t.Callable,
):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.REPLACE_WHERE
    df = pd.DataFrame({"a": [1, 2], "ds": ["2022-01-01", "2022-01-02"]})

    adapter.insert_overwrite_by_time_partition(
        "test_table",
        df,
        start="2022-01-01",
        end="2022-01-02",
        time_column="ds",
        time_formatter=lambda x, _: exp.Literal.string(to_ds(x)),
        columns_to_types={"a": exp.DataType.build("INT"), "ds": exp.DataType.build("STRING")},
    )

    assert to_sql_calls(adapter) == [
        """INSERT INTO "test_table" REPLACE WHERE "ds" BETWEEN '2022-01-01' AND '2022-01-02' SELECT "a", "ds" FROM (SELECT CAST("a" AS INT) AS "a", CAST("ds" AS TEXT) AS "ds" FROM (VALUES (1, '2022-01-01'), (2, '2022-01-02')) AS "t"("a", "ds")) AS "_subquery" WHERE "ds" BETWEEN '2022-01-01' AND '2022-01-02'"""
    ]


def test_insert_append_query(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    adapter.insert_append(
        "test_table",
        parse_one("SELECT a FROM tbl"),
        columns_to_types={"a": exp.DataType.build("INT")},
    )

    assert to_sql_calls(adapter) == [
        'INSERT INTO "test_table" ("a") SELECT "a" FROM "tbl"',
    ]


def test_insert_append_query_select_star(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    adapter.insert_append(
        "test_table",
        parse_one("SELECT 1 AS a, * FROM tbl"),
        columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
    )

    assert to_sql_calls(adapter) == [
        'INSERT INTO "test_table" ("a", "b") SELECT "a", "b" FROM (SELECT 1 AS "a", * FROM "tbl") AS "_ordered_projections"',
    ]


def test_insert_append_pandas(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.insert_append(
        "test_table",
        df,
        columns_to_types={
            "a": exp.DataType.build("INT"),
            "b": exp.DataType.build("INT"),
        },
    )

    assert to_sql_calls(adapter) == [
        'INSERT INTO "test_table" ("a", "b") SELECT CAST("a" AS INT) AS "a", CAST("b" AS INT) AS "b" FROM (VALUES (1, 4), (2, 5), (3, 6)) AS "t"("a", "b")',
    ]


def test_insert_append_pandas_batches(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)
    adapter.DEFAULT_BATCH_SIZE = 1

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.insert_append(
        "test_table",
        df,
        columns_to_types={
            "a": exp.DataType.build("INT"),
            "b": exp.DataType.build("INT"),
        },
    )

    adapter.cursor.begin.assert_called_once()
    adapter.cursor.commit.assert_called_once()

    assert to_sql_calls(adapter) == [
        'INSERT INTO "test_table" ("a", "b") SELECT CAST("a" AS INT) AS "a", CAST("b" AS INT) AS "b" FROM (VALUES (1, 4)) AS "t"("a", "b")',
        'INSERT INTO "test_table" ("a", "b") SELECT CAST("a" AS INT) AS "a", CAST("b" AS INT) AS "b" FROM (VALUES (2, 5)) AS "t"("a", "b")',
        'INSERT INTO "test_table" ("a", "b") SELECT CAST("a" AS INT) AS "a", CAST("b" AS INT) AS "b" FROM (VALUES (3, 6)) AS "t"("a", "b")',
    ]


def test_create_table(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table("test_table", columns_to_types)

    adapter.cursor.execute.assert_called_once_with(
        'CREATE TABLE IF NOT EXISTS "test_table" ("cola" INT, "colb" TEXT)'
    )


def test_create_table_properties(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table(
        "test_table",
        columns_to_types,
        partitioned_by=[exp.to_column("colb")],
        storage_format="ICEBERG",
    )

    adapter.cursor.execute.assert_called_once_with(
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
    make_mocked_engine_adapter: t.Callable,
    mocker: MockerFixture,
    schema_differ_config: t.Dict[str, t.Any],
    current_table: t.Dict[str, str],
    target_table: t.Dict[str, str],
    expected_final_structure: t.Dict[str, str],
    expected: t.List[str],
):
    adapter = make_mocked_engine_adapter(EngineAdapter)

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

    adapter.cursor.begin.assert_called_once()
    adapter.cursor.commit.assert_called_once()
    adapter.cursor.execute.assert_has_calls([call(x) for x in expected])


def test_merge_upsert(make_mocked_engine_adapter: t.Callable, assert_exp_eq):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    adapter.merge(
        target_table="target",
        source_table=t.cast(exp.Select, parse_one('SELECT "ID", ts, val FROM source')),
        columns_to_types={
            "ID": exp.DataType.Type.INT,
            "ts": exp.DataType.Type.TIMESTAMP,
            "val": exp.DataType.Type.INT,
        },
        unique_key=[exp.to_identifier("ID", quoted=True)],
    )

    assert_exp_eq(
        adapter.cursor.execute.call_args[0][0],
        """
MERGE INTO "target" AS "__MERGE_TARGET__" USING (
  SELECT
    "ID",
    "ts",
    "val"
  FROM "source"
) AS "__MERGE_SOURCE__"
  ON "__MERGE_TARGET__"."ID" = "__MERGE_SOURCE__"."ID"
  WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."ID" = "__MERGE_SOURCE__"."ID",
    "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts",
    "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val"
  WHEN NOT MATCHED THEN INSERT ("ID", "ts", "val")
    VALUES ("__MERGE_SOURCE__"."ID", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")
""",
    )

    adapter.cursor.reset_mock()
    adapter.merge(
        target_table="target",
        source_table=parse_one("SELECT id, ts, val FROM source"),
        columns_to_types={
            "id": exp.DataType.Type.INT,
            "ts": exp.DataType.Type.TIMESTAMP,
            "val": exp.DataType.Type.INT,
        },
        unique_key=[exp.column("id"), exp.column("ts")],
    )
    adapter.cursor.execute.assert_called_once_with(
        'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT "id", "ts", "val" FROM "source") AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" AND "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts" '
        'WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id", "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts", "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val" '
        'WHEN NOT MATCHED THEN INSERT ("id", "ts", "val") VALUES ("__MERGE_SOURCE__"."id", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")'
    )


def test_merge_upsert_pandas(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.merge(
        target_table="target",
        source_table=df,
        columns_to_types={
            "id": exp.DataType.Type.INT,
            "ts": exp.DataType.Type.TIMESTAMP,
            "val": exp.DataType.Type.INT,
        },
        unique_key=[exp.to_identifier("id")],
    )
    adapter.cursor.execute.assert_called_once_with(
        'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT CAST("id" AS INT) AS "id", CAST("ts" AS TIMESTAMP) AS "ts", CAST("val" AS INT) AS "val" FROM (VALUES (1, 4), (2, 5), (3, 6)) AS "t"("id", "ts", "val")) AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" '
        'WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id", "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts", "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val" '
        'WHEN NOT MATCHED THEN INSERT ("id", "ts", "val") VALUES ("__MERGE_SOURCE__"."id", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")'
    )

    adapter.cursor.reset_mock()
    adapter.merge(
        target_table="target",
        source_table=df,
        columns_to_types={
            "id": exp.DataType.Type.INT,
            "ts": exp.DataType.Type.TIMESTAMP,
            "val": exp.DataType.Type.INT,
        },
        unique_key=[exp.to_identifier("id"), exp.to_identifier("ts")],
    )
    adapter.cursor.execute.assert_called_once_with(
        'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT CAST("id" AS INT) AS "id", CAST("ts" AS TIMESTAMP) AS "ts", CAST("val" AS INT) AS "val" FROM (VALUES (1, 4), (2, 5), (3, 6)) AS "t"("id", "ts", "val")) AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" AND "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts" '
        'WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id", "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts", "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val" '
        'WHEN NOT MATCHED THEN INSERT ("id", "ts", "val") VALUES ("__MERGE_SOURCE__"."id", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")'
    )


def test_scd_type_2(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    adapter.scd_type_2(
        target_table="target",
        source_table=t.cast(
            exp.Select, parse_one("SELECT id, name, price, test_updated_at FROM source")
        ),
        unique_key=[exp.func("COALESCE", "id", "''")],
        valid_from_name="test_valid_from",
        valid_to_name="test_valid_to",
        updated_at_name="test_updated_at",
        columns_to_types={
            "id": exp.DataType.Type.INT,
            "name": exp.DataType.Type.VARCHAR,
            "price": exp.DataType.Type.DOUBLE,
            "test_updated_at": exp.DataType.Type.TIMESTAMP,
            "test_valid_from": exp.DataType.Type.TIMESTAMP,
            "test_valid_to": exp.DataType.Type.TIMESTAMP,
        },
        execution_time=datetime(2020, 1, 1, 0, 0, 0),
    )

    assert (
        adapter.cursor.execute.call_args[0][0]
        == parse_one(
            """
CREATE OR REPLACE TABLE "target" AS
WITH "source" AS (
  SELECT DISTINCT ON (COALESCE("id", ''))
    TRUE AS "_exists",
    "id",
    "name",
    "price",
    "test_updated_at"
  FROM (
    SELECT
      "id",
      "name",
      "price",
      "test_updated_at"
    FROM "source"
  ) AS "raw_source"
), "static" AS (
  SELECT
    "id",
    "name",
    "price",
    "test_updated_at",
    "test_valid_from",
    "test_valid_to"
  FROM "target"
  WHERE
    NOT "test_valid_to" IS NULL
), "latest" AS (
  SELECT
    "id",
    "name",
    "price",
    "test_updated_at",
    "test_valid_from",
    "test_valid_to"
  FROM "target"
  WHERE
    "test_valid_to" IS NULL
), "deleted" AS (
  SELECT
    "static"."id",
    "static"."name",
    "static"."price",
    "static"."test_updated_at",
    "static"."test_valid_from",
    "static"."test_valid_to"
  FROM "static"
  LEFT JOIN "latest"
    ON COALESCE("static"."id", '') = COALESCE("latest"."id", '')
  WHERE
    "latest"."test_valid_to" IS NULL
), "latest_deleted" AS (
  SELECT
    TRUE AS "_exists",
    COALESCE("id", '') AS "_key0",
    MAX("test_valid_to") AS "test_valid_to"
  FROM "deleted"
  GROUP BY
    COALESCE("id", '')
), "joined" AS (
  SELECT
    "source"."_exists",
    "latest"."id" AS "t_id",
    "latest"."name" AS "t_name",
    "latest"."price" AS "t_price",
    "latest"."test_updated_at" AS "t_test_updated_at",
    "latest"."test_valid_from" AS "t_test_valid_from",
    "latest"."test_valid_to" AS "t_test_valid_to",
    "source"."id" AS "id",
    "source"."name" AS "name",
    "source"."price" AS "price",
    "source"."test_updated_at" AS "test_updated_at"
  FROM "latest"
  LEFT JOIN "source"
    ON COALESCE("latest"."id", '') = COALESCE("source"."id", '')
  UNION
  SELECT
    "source"."_exists",
    "latest"."id" AS "t_id",
    "latest"."name" AS "t_name",
    "latest"."price" AS "t_price",
    "latest"."test_updated_at" AS "t_test_updated_at",
    "latest"."test_valid_from" AS "t_test_valid_from",
    "latest"."test_valid_to" AS "t_test_valid_to",
    "source"."id" AS "id",
    "source"."name" AS "name",
    "source"."price" AS "price",
    "source"."test_updated_at" AS "test_updated_at"
  FROM "latest"
  RIGHT JOIN "source"
    ON COALESCE("latest"."id", '') = COALESCE("source"."id", '')
), "updated_rows" AS (
  SELECT
    COALESCE("joined"."t_id", "joined"."id") AS "id",
    COALESCE("joined"."t_name", "joined"."name") AS "name",
    COALESCE("joined"."t_price", "joined"."price") AS "price",
    COALESCE("joined"."t_test_updated_at", "joined"."test_updated_at") AS "test_updated_at",
    CASE
      WHEN "t_test_valid_from" IS NULL AND NOT "latest_deleted"."_exists" IS NULL
      THEN CASE
        WHEN "latest_deleted"."test_valid_to" > "test_updated_at"
        THEN "latest_deleted"."test_valid_to"
        ELSE "test_updated_at"
      END
      WHEN "t_test_valid_from" IS NULL
      THEN CAST('1970-01-01 00:00:00' AS TIMESTAMP)
      ELSE "t_test_valid_from"
    END AS "test_valid_from",
    CASE
      WHEN "test_updated_at" > "t_test_updated_at"
      THEN "test_updated_at"
      WHEN "joined"."_exists" IS NULL
      THEN CAST('2020-01-01 00:00:00' AS TIMESTAMP)
      ELSE "t_test_valid_to"
    END AS "test_valid_to"
  FROM "joined"
  LEFT JOIN "latest_deleted"
    ON COALESCE("joined"."id", '') = "latest_deleted"."_key0"
), "inserted_rows" AS (
  SELECT
    "id",
    "name",
    "price",
    "test_updated_at",
    "test_updated_at" AS "test_valid_from",
    CAST(NULL AS TIMESTAMP) AS "test_valid_to"
  FROM "joined"
  WHERE
    "test_updated_at" > "t_test_updated_at"
)
SELECT
  *
FROM "static"
UNION ALL
SELECT
  *
FROM "updated_rows"
UNION ALL
SELECT
  *
FROM "inserted_rows"
    """
        ).sql()
    )


def test_merge_scd_type_2_pandas(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    df = pd.DataFrame(
        {
            "id1": [1, 2, 3],
            "id2": [4, 5, 6],
            "name": ["muffins", "chips", "soda"],
            "price": [4.0, 5.0, 6.0],
            "updated_at": ["2020-01-01 10:00:00", "2020-01-02 15:00:00", "2020-01-03 12:00:00"],
        }
    )
    adapter.scd_type_2(
        target_table="target",
        source_table=df,
        unique_key=[exp.column("id1"), exp.column("id2")],
        valid_from_name="test_valid_from",
        valid_to_name="test_valid_to",
        updated_at_name="test_updated_at",
        columns_to_types={
            "id1": exp.DataType.Type.INT,
            "id2": exp.DataType.Type.INT,
            "name": exp.DataType.Type.VARCHAR,
            "price": exp.DataType.Type.DOUBLE,
            "test_updated_at": exp.DataType.Type.TIMESTAMP,
            "test_valid_from": exp.DataType.Type.TIMESTAMP,
            "test_valid_to": exp.DataType.Type.TIMESTAMP,
        },
        execution_time=datetime(2020, 1, 1, 0, 0, 0),
    )

    assert (
        adapter.cursor.execute.call_args[0][0]
        == parse_one(
            """
CREATE OR REPLACE TABLE "target" AS
WITH "source" AS (
  SELECT DISTINCT ON ("id1", "id2")
    TRUE AS "_exists",
    "id1",
    "id2",
    "name",
    "price",
    "test_updated_at"
  FROM (
    SELECT
      CAST("id1" AS INT) AS "id1",
      CAST("id2" AS INT) AS "id2",
      CAST("name" AS VARCHAR) AS "name",
      CAST("price" AS DOUBLE) AS "price",
      CAST("test_updated_at" AS TIMESTAMP) AS "test_updated_at",
      CAST("test_valid_from" AS TIMESTAMP) AS "test_valid_from",
      CAST("test_valid_to" AS TIMESTAMP) AS "test_valid_to"
    FROM (VALUES
      (1, 4, 'muffins', 4.0, '2020-01-01 10:00:00'),
      (2, 5, 'chips', 5.0, '2020-01-02 15:00:00'),
      (3, 6, 'soda', 6.0, '2020-01-03 12:00:00')) AS "t"("id1", "id2", "name", "price", "test_updated_at", "test_valid_from", "test_valid_to")
  ) AS "raw_source"
), "static" AS (
  SELECT
    "id1",
    "id2",
    "name",
    "price",
    "test_updated_at",
    "test_valid_from",
    "test_valid_to"
  FROM "target"
  WHERE
    NOT "test_valid_to" IS NULL
), "latest" AS (
  SELECT
    "id1",
    "id2",
    "name",
    "price",
    "test_updated_at",
    "test_valid_from",
    "test_valid_to"
  FROM "target"
  WHERE
    "test_valid_to" IS NULL
), "deleted" AS (
  SELECT
    "static"."id1",
    "static"."id2",
    "static"."name",
    "static"."price",
    "static"."test_updated_at",
    "static"."test_valid_from",
    "static"."test_valid_to"
  FROM "static"
  LEFT JOIN "latest"
    ON "static"."id1" = "latest"."id1" AND "static"."id2" = "latest"."id2"
  WHERE
    "latest"."test_valid_to" IS NULL
), "latest_deleted" AS (
  SELECT
    TRUE AS "_exists",
    "id1" AS "_key0",
    "id2" AS "_key1",
    MAX("test_valid_to") AS "test_valid_to"
  FROM "deleted"
  GROUP BY
    "id1",
    "id2"
), "joined" AS (
  SELECT
    "source"."_exists",
    "latest"."id1" AS "t_id1",
    "latest"."id2" AS "t_id2",
    "latest"."name" AS "t_name",
    "latest"."price" AS "t_price",
    "latest"."test_updated_at" AS "t_test_updated_at",
    "latest"."test_valid_from" AS "t_test_valid_from",
    "latest"."test_valid_to" AS "t_test_valid_to",
    "source"."id1" AS "id1",
    "source"."id2" AS "id2",
    "source"."name" AS "name",
    "source"."price" AS "price",
    "source"."test_updated_at" AS "test_updated_at"
  FROM "latest"
  LEFT JOIN "source"
    ON "latest"."id1" = "source"."id1" AND "latest"."id2" = "source"."id2"
  UNION
  SELECT
    "source"."_exists",
    "latest"."id1" AS "t_id1",
    "latest"."id2" AS "t_id2",
    "latest"."name" AS "t_name",
    "latest"."price" AS "t_price",
    "latest"."test_updated_at" AS "t_test_updated_at",
    "latest"."test_valid_from" AS "t_test_valid_from",
    "latest"."test_valid_to" AS "t_test_valid_to",
    "source"."id1" AS "id1",
    "source"."id2" AS "id2",
    "source"."name" AS "name",
    "source"."price" AS "price",
    "source"."test_updated_at" AS "test_updated_at"
  FROM "latest"
  RIGHT JOIN "source"
    ON "latest"."id1" = "source"."id1" AND "latest"."id2" = "source"."id2"
), "updated_rows" AS (
  SELECT
    COALESCE("joined"."t_id1", "joined"."id1") AS "id1",
    COALESCE("joined"."t_id2", "joined"."id2") AS "id2",
    COALESCE("joined"."t_name", "joined"."name") AS "name",
    COALESCE("joined"."t_price", "joined"."price") AS "price",
    COALESCE("joined"."t_test_updated_at", "joined"."test_updated_at") AS "test_updated_at",
    CASE
      WHEN "t_test_valid_from" IS NULL AND NOT "latest_deleted"."_exists" IS NULL
      THEN CASE
        WHEN "latest_deleted"."test_valid_to" > "test_updated_at"
        THEN "latest_deleted"."test_valid_to"
        ELSE "test_updated_at"
      END
      WHEN "t_test_valid_from" IS NULL
      THEN CAST('1970-01-01 00:00:00' AS TIMESTAMP)
      ELSE "t_test_valid_from"
    END AS "test_valid_from",
    CASE
      WHEN "test_updated_at" > "t_test_updated_at"
      THEN "test_updated_at"
      WHEN "joined"."_exists" IS NULL
      THEN CAST('2020-01-01 00:00:00' AS TIMESTAMP)
      ELSE "t_test_valid_to"
    END AS "test_valid_to"
  FROM "joined"
  LEFT JOIN "latest_deleted"
    ON "joined"."id1" = "latest_deleted"."_key0"
    AND "joined"."id2" = "latest_deleted"."_key1"
), "inserted_rows" AS (
  SELECT
    "id1",
    "id2",
    "name",
    "price",
    "test_updated_at",
    "test_updated_at" AS "test_valid_from",
    CAST(NULL AS TIMESTAMP) AS "test_valid_to"
  FROM "joined"
  WHERE
    "test_updated_at" > "t_test_updated_at"
)
SELECT
  *
FROM "static"
UNION ALL
SELECT
  *
FROM "updated_rows"
UNION ALL
SELECT
  *
FROM "inserted_rows"
"""
        ).sql()
    )


def test_replace_query(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    # TODO: Shouldn't we enforce that `a` is casted to an int?
    assert to_sql_calls(adapter) == [
        'CREATE OR REPLACE TABLE "test_table" AS SELECT "a" FROM "tbl"'
    ]


def test_replace_query_pandas(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)
    adapter.DEFAULT_BATCH_SIZE = 1

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query("test_table", df, {"a": "int", "b": "int"})

    assert to_sql_calls(adapter) == [
        'CREATE OR REPLACE TABLE "test_table" AS SELECT CAST("a" AS INT) AS "a", CAST("b" AS INT) AS "b" FROM (VALUES (1, 4)) AS "t"("a", "b")',
        'INSERT INTO "test_table" ("a", "b") SELECT CAST("a" AS INT) AS "a", CAST("b" AS INT) AS "b" FROM (VALUES (2, 5)) AS "t"("a", "b")',
        'INSERT INTO "test_table" ("a", "b") SELECT CAST("a" AS INT) AS "a", CAST("b" AS INT) AS "b" FROM (VALUES (3, 6)) AS "t"("a", "b")',
    ]


def test_create_table_like(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    adapter.create_table_like("target_table", "source_table")
    adapter.cursor.execute.assert_called_once_with(
        'CREATE TABLE IF NOT EXISTS "target_table" LIKE "source_table"'
    )


def test_create_table_primary_key(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapterWithIndexSupport)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table("test_table", columns_to_types, primary_key=("cola", "colb"))

    adapter.cursor.execute.assert_called_once_with(
        'CREATE TABLE IF NOT EXISTS "test_table" ("cola" INT, "colb" TEXT, PRIMARY KEY ("cola", "colb"))'
    )


def test_create_index(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapterWithIndexSupport)
    adapter.SUPPORTS_INDEXES = True

    adapter.create_index("test_table", "test_index", ("cola", "colb"))
    adapter.cursor.execute.assert_called_once_with(
        'CREATE INDEX IF NOT EXISTS "test_index" ON "test_table"("cola", "colb")'
    )


def test_rename_table(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    adapter.rename_table("old_table", "new_table")
    adapter.cursor.execute.assert_called_once_with('ALTER TABLE "old_table" RENAME TO "new_table"')


def test_clone_table(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter, dialect="bigquery")

    with pytest.raises(NotImplementedError):
        adapter.clone_table("target_table", "source_table")

    adapter.SUPPORTS_CLONING = True

    adapter.clone_table("target_table", "source_table")

    adapter.cursor.execute.assert_called_once_with(
        "CREATE TABLE `target_table` CLONE `source_table`"
    )


def test_ctas(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    adapter.ctas("new_table", parse_one("SELECT * FROM old_table"))

    assert to_sql_calls(adapter) == [
        'CREATE TABLE IF NOT EXISTS "new_table" AS SELECT * FROM "old_table"'
    ]


def test_ctas_pandas(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    # Inferring the types from the dataframe
    adapter.ctas("new_table", df)

    assert to_sql_calls(adapter) == [
        'CREATE TABLE IF NOT EXISTS "new_table" AS SELECT CAST("a" AS BIGINT) AS "a", CAST("b" AS BIGINT) AS "b" FROM (VALUES (1, 4), (2, 5), (3, 6)) AS "t"("a", "b")'
    ]


def test_drop_view(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(EngineAdapter)

    adapter.drop_view("test_view")
    adapter.drop_view("test_view", materialized=True)

    assert to_sql_calls(adapter) == [
        'DROP VIEW IF EXISTS "test_view"',
        'DROP MATERIALIZED VIEW IF EXISTS "test_view"',
    ]
