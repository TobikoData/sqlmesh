# type: ignore
import typing as t
from unittest.mock import call

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter.base import InsertOverwriteStrategy
from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter
from sqlmesh.core.schema_diff import SchemaDiffer, TableAlterOperation


def test_table_exists(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)

    resp = adapter.table_exists("db.table")
    adapter.cursor.execute.assert_called_once_with(
        """SELECT 1 """
        """FROM "master"."information_schema"."tables" """
        """WHERE "table_name" = 'table' AND "table_schema" = 'db'"""
    )
    assert resp
    adapter.cursor.fetchone.return_value = None
    resp = adapter.table_exists("db.table")
    assert not resp


def test_insert_overwrite_by_time_partition_supports_insert_overwrite_pandas(
    make_mocked_engine_adapter: t.Callable,
):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INSERT_OVERWRITE
    df = pd.DataFrame({"a": [1, 2], "ds": ["2022-01-01", "2022-01-02"]})
    adapter._insert_overwrite_by_condition(
        "test_table",
        df,
        where=parse_one("ds BETWEEN '2022-01-01' and '2022-01-02'"),
        columns_to_types={"a": exp.DataType.build("INT"), "ds": exp.DataType.build("STRING")},
    )

    # Check for the important parts of these queries
    expected_queries: t.List[t.List[str]] = [
        [
            "IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table",
            """EXEC('CREATE TABLE "__temp_test_table""",
            """("a" BIGINT, "ds" TEXT)""",
        ],
        [
            'INSERT INTO "__temp_test_table_',
            """" ("a", "ds") SELECT CAST("a" AS BIGINT) AS "a", CAST("ds" AS TEXT) AS "ds" FROM (VALUES (1, '2022-01-01'), (2, '2022-01-02')) AS "t"("a", "ds")""",
        ],
        [
            'MERGE INTO "test_table" AS "__MERGE_TARGET__" USING (SELECT * FROM (SELECT "a", "ds" FROM "__temp_test_table_',
            """") AS "_subquery" WHERE "ds" BETWEEN '2022-01-01' AND '2022-01-02') AS "__MERGE_SOURCE__" ON 1 = 2 WHEN NOT MATCHED BY SOURCE AND "ds" BETWEEN '2022-01-01' AND '2022-01-02' THEN DELETE WHEN NOT MATCHED THEN INSERT ("a", "ds") VALUES ("a", "ds")""",
        ],
        ['DROP TABLE IF EXISTS "__temp_test_table_'],
    ]
    # call can be Tuple[Tuple, Dict] or Tuple[str, Tuple, Dict], per docs
    # we're after the nested tuple, which contains call args
    # https://docs.python.org/3.7/library/unittest.mock.html#calls-as-tuples
    call_list: t.List[str] = [
        c[0][0] if isinstance(c[0], tuple) else c[1][0]
        for c in adapter.cursor.execute.call_args_list
    ]
    test_results: t.List[bool] = [
        # any call can match one of the expected queries
        any(
            [  # all pieces of the query should be found in the call
                all([q in c for q in e]) for e in expected_queries
            ]
        )
        for c in call_list
    ]

    assert all(test_results)


def test_insert_overwrite_by_time_partition_replace_where_pandas(
    make_mocked_engine_adapter: t.Callable,
):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    adapter.INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.REPLACE_WHERE
    df = pd.DataFrame({"a": [1, 2], "ds": ["2022-01-01", "2022-01-02"]})
    adapter._insert_overwrite_by_condition(
        "test_table",
        df,
        where=parse_one("CAST(CAST(ds AS VARCHAR) AS DATE) BETWEEN '2022-01-01' and '2022-01-02'"),
        columns_to_types={"a": exp.DataType.build("INT"), "ds": exp.DataType.build("TEXT")},
    )

    # Check for the important parts of these queries
    expected_queries: t.List[t.List[str]] = [
        [
            "IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = '__temp_test_table",
            """EXEC('CREATE TABLE "__temp_test_table""",
            """("a" BIGINT, "ds" TEXT)""",
        ],
        [
            'INSERT INTO "__temp_test_table_',
            """" ("a", "ds") SELECT CAST("a" AS BIGINT) AS "a", CAST("ds" AS TEXT) AS "ds" FROM (VALUES (1, '2022-01-01'), (2, '2022-01-02')) AS "t"("a", "ds")""",
        ],
        [
            'MERGE INTO "test_table" AS "__MERGE_TARGET__" USING (SELECT * FROM (SELECT "a", "ds" FROM "__temp_test_table_',
            """") AS "_subquery" WHERE CAST(CAST("ds" AS VARCHAR) AS DATE) BETWEEN '2022-01-01' AND '2022-01-02') AS "__MERGE_SOURCE__" ON 1 = 2 WHEN NOT MATCHED BY SOURCE AND CAST(CAST("ds" AS VARCHAR) AS DATE) BETWEEN '2022-01-01' AND '2022-01-02' THEN DELETE WHEN NOT MATCHED THEN INSERT ("a", "ds") VALUES ("a", "ds")""",
        ],
        ['DROP TABLE IF EXISTS "__temp_test_table_'],
    ]
    # call can be Tuple[Tuple, Dict] or Tuple[str, Tuple, Dict], per docs
    # we're after the nested tuple, which contains call args
    # https://docs.python.org/3.7/library/unittest.mock.html#calls-as-tuples
    call_list: t.List[str] = [
        c[0][0] if isinstance(c[0], tuple) else c[1][0]
        for c in adapter.cursor.execute.call_args_list
    ]
    test_results: t.List[bool] = [
        # any call can match one of the expected queries
        any(
            [  # all pieces of the query should be found in the call
                all([q in c for q in e]) for e in expected_queries
            ]
        )
        for c in call_list
    ]

    assert all(test_results)


def test_insert_append_pandas(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

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

    adapter.cursor.execute.assert_has_calls(
        [
            call(
                'INSERT INTO "test_table" ("a", "b") SELECT CAST("a" AS INTEGER) AS "a", CAST("b" AS INTEGER) AS "b" FROM (VALUES (1, 4), (2, 5), (3, 6)) AS "t"("a", "b")',
            ),
        ]
    )


def test_create_table(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table("test_table", columns_to_types)

    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'test_table') EXEC('CREATE TABLE "test_table" ("cola" INTEGER, "colb" TEXT)')"""
    )


def test_create_table_properties(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

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
        """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'test_table') EXEC('CREATE TABLE "test_table" ("cola" INTEGER, "colb" TEXT)')"""
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
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_b" INTEGER AFTER "nested_a"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_b" INTEGER AFTER "array_a"',
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
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_b" INTEGER',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_b" INTEGER',
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
                'ALTER TABLE "test_table" ADD COLUMN "nested" STRUCT<"nested_a" INTEGER, "nested_b" INTEGER, "nested_c" INTEGER>',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col" ARRAY<STRUCT<"array_a" INTEGER, "array_b" INTEGER, "array_c" INTEGER>>',
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
                'ALTER TABLE "test_table" ADD COLUMN "nested" STRUCT<"nested_a" INTEGER, "nested_b" INTEGER, "nested_c" INTEGER>',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col" ARRAY<STRUCT<"array_a" INTEGER, "array_b" INTEGER, "array_c" INTEGER>>',
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
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_b" INTEGER AFTER "nested_a"',
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_d" INTEGER AFTER "nested_b"',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"."element"."array_c"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_b" INTEGER AFTER "array_a"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_d" INTEGER AFTER "array_b"',
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
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_b" INTEGER',
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_d" INTEGER',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"."element"."array_c"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_b" INTEGER',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_d" INTEGER',
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
                'ALTER TABLE "test_table" ADD COLUMN "nested" STRUCT<"nested_a" INTEGER, "nested_b" INTEGER, "nested_d" INTEGER, "nested_e" INTEGER> FIRST',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col" ARRAY<STRUCT<"array_a" INTEGER, "array_b" INTEGER, "array_d" INTEGER, "array_e" INTEGER>> AFTER "nested"',
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
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_b" INTEGER',
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_d" INTEGER',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"."element"."array_c"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_b" INTEGER',
                'ALTER TABLE "test_table" ADD COLUMN "array_col"."element"."array_d" INTEGER',
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
                'ALTER TABLE "test_table" ADD COLUMN "nested" STRUCT<"nested_a" INTEGER, "nested_b" INTEGER, "nested_d" INTEGER, "nested_e" INTEGER>',
                'ALTER TABLE "test_table" DROP COLUMN "array_col"',
                'ALTER TABLE "test_table" ADD COLUMN "array_col" ARRAY<STRUCT<"array_a" INTEGER, "array_b" INTEGER, "array_d" INTEGER, "array_e" INTEGER>>',
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
                'ALTER TABLE "test_table" ADD COLUMN "nested"."nested_1_a"."nested_2_a"."element"."nested_3_a"."nested_4_a"."element"."nested_5_a"."element"."nested_6_b" INTEGER',
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
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

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


def test_merge_pandas(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

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
    adapter.cursor.execute.assert_called_once_with(
        'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT CAST("id" AS INTEGER) AS "id", CAST("ts" AS DATETIME2) AS "ts", CAST("val" AS INTEGER) AS "val" FROM (VALUES (1, 4), (2, 5), (3, 6)) AS "t"("id", "ts", "val")) AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" '
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
        unique_key=["id", "ts"],
    )
    adapter.cursor.execute.assert_called_once_with(
        'MERGE INTO "target" AS "__MERGE_TARGET__" USING (SELECT CAST("id" AS INTEGER) AS "id", CAST("ts" AS DATETIME2) AS "ts", CAST("val" AS INTEGER) AS "val" FROM (VALUES (1, 4), (2, 5), (3, 6)) AS "t"("id", "ts", "val")) AS "__MERGE_SOURCE__" ON "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id" AND "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts" '
        'WHEN MATCHED THEN UPDATE SET "__MERGE_TARGET__"."id" = "__MERGE_SOURCE__"."id", "__MERGE_TARGET__"."ts" = "__MERGE_SOURCE__"."ts", "__MERGE_TARGET__"."val" = "__MERGE_SOURCE__"."val" '
        'WHEN NOT MATCHED THEN INSERT ("id", "ts", "val") VALUES ("__MERGE_SOURCE__"."id", "__MERGE_SOURCE__"."ts", "__MERGE_SOURCE__"."val")'
    )


def test_replace_query(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    adapter.cursor.execute.assert_has_calls(
        [
            call(
                """SELECT 1 """
                """FROM "master"."information_schema"."tables" """
                """WHERE "table_name" = 'test_table'"""
            ),
            call('TRUNCATE "test_table"'),
            call('INSERT INTO "test_table" ("a") SELECT "a" FROM "tbl"'),
        ]
    )


def test_replace_query_pandas(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.cursor.fetchone.return_value = (1,)

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query("test_table", df, {"a": "int", "b": "int"})

    adapter.cursor.execute.assert_has_calls(
        [
            call(
                """SELECT 1 FROM "master"."information_schema"."tables" WHERE "table_name" = 'test_table'"""
            ),
            call('TRUNCATE "test_table"'),
            call(
                'INSERT INTO "test_table" ("a", "b") '
                "SELECT "
                'CAST("a" AS INTEGER) AS "a", '
                'CAST("b" AS INTEGER) AS "b" '
                'FROM (VALUES (1, 4), (2, 5), (3, 6)) AS "t"("a", "b")'
            ),
        ]
    )


def test_create_table_primary_key(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }
    adapter.create_table("test_table", columns_to_types, primary_key=("cola", "colb"))

    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'test_table') EXEC('CREATE TABLE "test_table" ("cola" INTEGER, "colb" TEXT, PRIMARY KEY ("cola", "colb"))')"""
    )


def test_create_index(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(MSSQLEngineAdapter)
    adapter.SUPPORTS_INDEXES = True

    adapter.create_index("test_table", "test_index", ("cola", "colb"))
    adapter.cursor.execute.assert_called_once_with(
        """IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = object_id('test_table') AND name = 'test_index') EXEC('CREATE INDEX "test_index" ON "test_table"("cola", "colb")')"""
    )
