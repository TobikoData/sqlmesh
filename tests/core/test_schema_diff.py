import typing as t
from unittest.mock import call

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp

from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.schema_diff import (
    ColumnPosition,
    ParentColumns,
    SchemaDelta,
    struct_diff,
    table_diff,
)


def test_schema_diff_calculate(mocker: MockerFixture):
    apply_to_table_name = "apply_to_table"
    schema_from_table_name = "schema_from_table"

    def table_columns(table_name: str) -> t.Dict[str, exp.DataType]:
        if table_name == apply_to_table_name:
            return {
                "id": exp.DataType.build("INT"),
                "name": exp.DataType.build("STRING"),
                "price": exp.DataType.build("DOUBLE"),
                "ds": exp.DataType.build("STRING"),
            }
        else:
            return {
                "name": exp.DataType.build("INT"),
                "id": exp.DataType.build("INT"),
                "ds": exp.DataType.build("STRING"),
                "new_column": exp.DataType.build("DOUBLE"),
            }

    engine_adapter_mock = mocker.Mock()
    engine_adapter_mock.columns.side_effect = table_columns

    assert table_diff(apply_to_table_name, schema_from_table_name, engine_adapter_mock) == [
        SchemaDelta.drop(
            "price",
            "STRUCT<id: INT, name: TEXT, ds: TEXT>",
            "DOUBLE",
        ),
        SchemaDelta.add(
            "new_column",
            "DOUBLE",
            "STRUCT<id: INT, name: TEXT, ds: TEXT, new_column: DOUBLE>",
            ColumnPosition.create_last("ds"),
        ),
        SchemaDelta.alter_type(
            "name",
            "INT",
            "STRING",
            "STRUCT<id: INT, name: INT, ds: TEXT, new_column: DOUBLE>",
            ColumnPosition.create_middle("id"),
        ),
    ]

    engine_adapter_mock.columns.assert_has_calls(
        [call(apply_to_table_name), call(schema_from_table_name)]
    )


def test_schema_diff_calculate_type_transitions(mocker: MockerFixture):
    apply_to_table_name = "apply_to_table"
    schema_from_table_name = "schema_from_table"

    def table_columns(table_name: str) -> t.Dict[str, exp.DataType]:
        if table_name == apply_to_table_name:
            return {
                "id": exp.DataType.build("INT"),
                "ds": exp.DataType.build("STRING"),
            }
        else:
            return {
                "id": exp.DataType.build("BIGINT"),
                "ds": exp.DataType.build("INT"),
            }

    engine_adapter_mock = mocker.Mock()
    engine_adapter_mock.columns.side_effect = table_columns

    assert table_diff(apply_to_table_name, schema_from_table_name, engine_adapter_mock) == [
        SchemaDelta.alter_type(
            "id", "BIGINT", "INT", "STRUCT<id: BIGINT, ds: STRING>", ColumnPosition.create_first()
        ),
        SchemaDelta.alter_type(
            "ds", "INT", "STRING", "STRUCT<id: BIGINT, ds: INT>", ColumnPosition.create_last("id")
        ),
    ]

    engine_adapter_mock.columns.assert_has_calls(
        [call(apply_to_table_name), call(schema_from_table_name)]
    )


def test_schema_diff_struct_add_column(mocker: MockerFixture):
    apply_to_table_name = "apply_to_table"
    schema_from_table_name = "schema_from_table"

    def table_columns(table_name: str) -> t.Dict[str, exp.DataType]:
        if table_name == apply_to_table_name:
            return {
                "complex": exp.DataType.build("STRUCT<id: INT, name: STRING>"),
                "ds": exp.DataType.build("STRING"),
            }
        else:
            return {
                "complex": exp.DataType.build("STRUCT<id: INT, new_column: DOUBLE, name: STRING>"),
                "ds": exp.DataType.build("INT"),
            }

    engine_adapter_mock = mocker.Mock()
    engine_adapter_mock.columns.side_effect = table_columns

    assert table_diff(apply_to_table_name, schema_from_table_name, engine_adapter_mock) == [
        SchemaDelta.add(
            "complex.new_column",
            "DOUBLE",
            "STRUCT<complex: STRUCT<id: INT, new_column: DOUBLE, name: STRING>, ds: STRING>",
            ColumnPosition.create_middle("id"),
            parents=ParentColumns.create(("complex", "STRUCT")),
        ),
        SchemaDelta.alter_type(
            "ds",
            "INT",
            "STRING",
            "STRUCT<complex: STRUCT<id: INT, new_column: DOUBLE, name: STRING>, ds: INT>",
            ColumnPosition.create_last("complex"),
        ),
    ]

    engine_adapter_mock.columns.assert_has_calls(
        [call(apply_to_table_name), call(schema_from_table_name)]
    )


@pytest.mark.parametrize(
    "current_struct, new_struct, expected_diff",
    [
        # ###########
        # # Add Tests
        # ###########
        # No diff
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, name STRING, age INT>",
            [],
        ),
        # Add root level column at the end
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, name STRING, age INT, address STRING>",
            [
                SchemaDelta.add(
                    "address",
                    "STRING",
                    "STRUCT<id INT, name STRING, age INT, address STRING>",
                    position=ColumnPosition.create_last(after="age"),
                )
            ],
        ),
        # Add root level column at the beginning
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<address STRING, id INT, name STRING, age INT>",
            [
                SchemaDelta.add(
                    "address",
                    "STRING",
                    expected_table_struct="STRUCT<address STRING, id INT, name STRING, age INT>",
                    position=ColumnPosition.create_first(),
                )
            ],
        ),
        # Add root level column in the middle
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, address STRING, name STRING, age INT>",
            [
                SchemaDelta.add(
                    "address",
                    "STRING",
                    expected_table_struct="STRUCT<id INT, address STRING, name STRING, age INT>",
                    position=ColumnPosition.create_middle(after="id"),
                )
            ],
        ),
        # Add columns at the beginning, middle, and end
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<address STRING, id INT, address2 STRING, name STRING, age INT, address3 STRING>",
            [
                SchemaDelta.add(
                    "address",
                    "STRING",
                    expected_table_struct="STRUCT<address STRING, id INT, name STRING, age INT>",
                    position=ColumnPosition.create_first(),
                ),
                SchemaDelta.add(
                    "address2",
                    "STRING",
                    expected_table_struct="STRUCT<address STRING, id INT, address2 STRING, name STRING, age INT>",
                    position=ColumnPosition.create_middle(after="id"),
                ),
                SchemaDelta.add(
                    "address3",
                    "STRING",
                    expected_table_struct="STRUCT<address STRING, id INT, address2 STRING, name STRING, age INT, address3 STRING>",
                    position=ColumnPosition.create_last(after="age"),
                ),
            ],
        ),
        # Add two columns next to each other at the start
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<address STRING, address2 STRING, id INT, name STRING, age INT>",
            [
                SchemaDelta.add(
                    "address",
                    "STRING",
                    expected_table_struct="STRUCT<address STRING, id INT, name STRING, age INT>",
                    position=ColumnPosition.create_first(),
                ),
                SchemaDelta.add(
                    "address2",
                    "STRING",
                    expected_table_struct="STRUCT<address STRING, address2 STRING, id INT, name STRING, age INT>",
                    position=ColumnPosition.create_middle(after="address"),
                ),
            ],
        ),
        ############
        # Drop Tests
        ############
        # Drop root level column at the start
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<name STRING, age INT>",
            [
                SchemaDelta.drop(
                    "id",
                    "STRUCT<name STRING, age INT>",
                    "INT",
                )
            ],
        ),
        # Drop root level column in the middle
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, age INT>",
            [
                SchemaDelta.drop(
                    "name",
                    "STRUCT<id INT, age INT>",
                    "STRING",
                )
            ],
        ),
        # Drop root level column at the end
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, name STRING>",
            [
                SchemaDelta.drop(
                    "age",
                    "STRUCT<id INT, name STRING>",
                    "INT",
                )
            ],
        ),
        # Drop root level column at start, middle, and end
        (
            "STRUCT<id INT, name STRING, middle STRING, address STRING, age INT>",
            "STRUCT<name STRING, address STRING>",
            [
                SchemaDelta.drop(
                    "id",
                    "STRUCT<name STRING, middle STRING, address STRING, age INT>",
                    "INT",
                ),
                SchemaDelta.drop(
                    "middle",
                    "STRUCT<name STRING, address STRING, age INT>",
                    "STRING",
                ),
                SchemaDelta.drop(
                    "age",
                    "STRUCT<name STRING, address STRING>",
                    "INT",
                ),
            ],
        ),
        # Drop two columns next to each other at the start
        (
            "STRUCT<address STRING, address2 STRING, id INT, name STRING, age INT>",
            "STRUCT<id INT, name STRING, age INT>",
            [
                SchemaDelta.drop(
                    "address",
                    "STRUCT<address2 STRING, id INT, name STRING, age INT>",
                    "STRING",
                ),
                SchemaDelta.drop(
                    "address2",
                    "STRUCT<id INT, name STRING, age INT>",
                    "STRING",
                ),
            ],
        ),
        #############
        # Move Tests
        #############
        # Move root level column at the start
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<name STRING, id INT, age INT>",
            [],
        ),
        # Move root level column in the middle
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, age INT, name STRING>",
            [],
        ),
        # Move root level column at the end
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<age INT, id INT, name STRING>",
            [],
        ),
        ###################
        # Type Change Tests
        ###################
        # Change root level column type that is allowed
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id STRING, name STRING, age INT>",
            [
                SchemaDelta.alter_type(
                    "id",
                    "STRING",
                    current_type="INT",
                    expected_table_struct="STRUCT<id STRING, name STRING, age INT>",
                    position=ColumnPosition.create_first(),
                )
            ],
        ),
        ############
        # Mix Tests
        ############
        # Add, drop, and change type
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id STRING, age INT, address STRING>",
            [
                SchemaDelta.drop(
                    "name",
                    "STRUCT<id INT, age INT>",
                    "STRING",
                ),
                SchemaDelta.add(
                    "address",
                    "STRING",
                    expected_table_struct="STRUCT<id INT, age INT, address STRING>",
                    position=ColumnPosition.create_last(after="age"),
                ),
                SchemaDelta.alter_type(
                    "id",
                    "STRING",
                    current_type="INT",
                    expected_table_struct="STRUCT<id STRING, age INT, address STRING>",
                    position=ColumnPosition.create_first(),
                ),
            ],
        ),
        ##############
        # Struct Tests
        ##############
        # Add a column to the start of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_d INT, col_a INT, col_b INT, col_c INT>>",
            [
                SchemaDelta.add(
                    "info.col_d",
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_d INT, col_a INT, col_b INT, col_c INT>>",
                    position=ColumnPosition.create_first(),
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
            ],
        ),
        # Add a column to the end of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT, col_d INT>>",
            [
                SchemaDelta.add(
                    "info.col_d",
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT, col_d INT>>",
                    position=ColumnPosition.create_last(after="col_c"),
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
            ],
        ),
        # Add a column to the middle of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_d INT, col_b INT, col_c INT>>",
            [
                SchemaDelta.add(
                    "info.col_d",
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, col_d INT, col_b INT, col_c INT>>",
                    position=ColumnPosition.create_middle(after="col_a"),
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
            ],
        ),
        # Add two columns at the start of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_d INT, col_e INT, col_a INT, col_b INT, col_c INT>>",
            [
                SchemaDelta.add(
                    "info.col_d",
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_d INT, col_a INT, col_b INT, col_c INT>>",
                    position=ColumnPosition.create_first(),
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
                SchemaDelta.add(
                    "info.col_e",
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_d INT, col_e INT, col_a INT, col_b INT, col_c INT>>",
                    position=ColumnPosition.create_middle(after="col_d"),
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
            ],
        ),
        # Remove a column from the start of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_b INT, col_c INT>>",
            [
                SchemaDelta.drop(
                    "info.col_a",
                    "STRUCT<id INT, info STRUCT<col_b INT, col_c INT>>",
                    "INT",
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
            ],
        ),
        # Remove a column from the end of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT>>",
            [
                SchemaDelta.drop(
                    "info.col_c",
                    "STRUCT<id INT, info STRUCT<col_a INT, col_b INT>>",
                    "INT",
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
            ],
        ),
        # Remove a column from the middle of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_c INT>>",
            [
                SchemaDelta.drop(
                    "info.col_b",
                    "STRUCT<id INT, info STRUCT<col_a INT, col_c INT>>",
                    "INT",
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
            ],
        ),
        # Remove two columns from the start of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_c INT>>",
            [
                SchemaDelta.drop(
                    "info.col_a",
                    "STRUCT<id INT, info STRUCT<col_b INT, col_c INT>>",
                    "INT",
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
                SchemaDelta.drop(
                    "info.col_b",
                    "STRUCT<id INT, info STRUCT<col_c INT>>",
                    "INT",
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
            ],
        ),
        # Change a column type in a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c TEXT>>",
            [
                SchemaDelta.alter_type(
                    "info.col_c",
                    "TEXT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c TEXT>>",
                    position=ColumnPosition.create_last(after="col_b"),
                    parents=ParentColumns.create(("info", "STRUCT")),
                    current_type=exp.DataType.build("INT"),
                ),
            ],
        ),
        # Add, remove and change a column in a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_d INT, col_b INT, col_e INT, col_c TEXT>>",
            [
                SchemaDelta.drop(
                    "info.col_a",
                    "STRUCT<id INT, info STRUCT<col_b INT, col_c INT>>",
                    "INT",
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
                SchemaDelta.add(
                    "info.col_d",
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_d INT, col_b INT, col_c INT>>",
                    position=ColumnPosition.create_first(),
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
                SchemaDelta.add(
                    "info.col_e",
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_d INT, col_b INT, col_e INT, col_c INT>>",
                    parents=ParentColumns.create(("info", "STRUCT")),
                    position=ColumnPosition.create_middle(after="col_b"),
                ),
                SchemaDelta.alter_type(
                    "info.col_c",
                    "TEXT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_d INT, col_b INT, col_e INT, col_c TEXT>>",
                    position=ColumnPosition.create_last(after="col_e"),
                    parents=ParentColumns.create(("info", "STRUCT")),
                    current_type=exp.DataType.build("INT"),
                ),
            ],
        ),
        # Add and remove from outer and nested struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, nested_info STRUCT<nest_col_a INT, nest_col_b INT>>>",
            "STRUCT<id INT, info STRUCT<col_a INT, nested_info STRUCT<nest_col_a INT, col_c INT>, col_c INT>>",
            [
                SchemaDelta.drop(
                    "info.col_b",
                    "STRUCT<id INT, info STRUCT<col_a INT, nested_info STRUCT<nest_col_a INT, nest_col_b INT>>>",
                    "INT",
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
                SchemaDelta.add(
                    "info.col_c",
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, nested_info STRUCT<nest_col_a INT, nest_col_b INT>, col_c INT>>",
                    position=ColumnPosition.create_last("nested_info"),
                    parents=ParentColumns.create(("info", "STRUCT")),
                ),
                SchemaDelta.drop(
                    "info.nested_info.nest_col_b",
                    "STRUCT<id INT, info STRUCT<col_a INT, nested_info STRUCT<nest_col_a INT>, col_c INT>>",
                    "INT",
                    parents=ParentColumns.create(
                        ("info", "STRUCT"),
                        ("nested_info", "STRUCT"),
                    ),
                ),
                SchemaDelta.add(
                    "info.nested_info.col_c",
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, nested_info STRUCT<nest_col_a INT, col_c INT>, col_c INT>>",
                    position=ColumnPosition.create_last("nest_col_a"),
                    parents=ParentColumns.create(("info", "STRUCT"), ("nested_info", "STRUCT")),
                ),
            ],
        ),
        #####################
        # Array Struct Tests
        #####################
        # Add column to array of structs
        (
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>>",
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_d INT, col_c INT>>>",
            [
                SchemaDelta.add(
                    "infos.col_d",
                    "INT",
                    expected_table_struct="STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_d INT, col_c INT>>>",
                    position=ColumnPosition.create_middle("col_b"),
                    parents=ParentColumns.create(("infos", "ARRAY")),
                ),
            ],
        ),
        # Remove column from array of structs
        (
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>>",
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_c INT>>>",
            [
                SchemaDelta.drop(
                    "infos.col_b",
                    "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_c INT>>>",
                    "INT",
                    parents=ParentColumns.create(("infos", "ARRAY")),
                ),
            ],
        ),
        # Alter column type in array of structs
        (
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>>",
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c TEXT>>>",
            [
                SchemaDelta.alter_type(
                    "infos.col_c",
                    "TEXT",
                    expected_table_struct="STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c TEXT>>>",
                    position=ColumnPosition.create_last("col_b"),
                    parents=ParentColumns.create(("infos", "ARRAY")),
                    current_type="INT",
                ),
            ],
        ),
        # Add an array of primitives
        (
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>>",
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>, values ARRAY<INT>>",
            [
                SchemaDelta.add(
                    "values",
                    "ARRAY<INT>",
                    expected_table_struct="STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>, values ARRAY<INT>>",
                    position=ColumnPosition.create_last("infos"),
                ),
            ],
        ),
        # Precision VARCHAR is a no-op with no changes
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR(120)>",
            [],
        ),
        # Change the precision bits of a VARCHAR
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR(100)>",
            [
                SchemaDelta.alter_type(
                    "address",
                    "VARCHAR(100)",
                    expected_table_struct="STRUCT<id INT, address VARCHAR(100)>",
                    position=ColumnPosition.create_last("id"),
                    current_type=exp.DataType.build("VARCHAR(120)"),
                ),
            ],
        ),
    ],
)
def test_struct_diff(
    current_struct,
    new_struct,
    expected_diff: t.List[SchemaDelta],
):
    assert (
        struct_diff(
            exp.DataType.build(current_struct),
            exp.DataType.build(new_struct),
        )
        == expected_diff
    )


def test_schema_diff_calculate_duckdb(duck_conn):
    engine_adapter = create_engine_adapter(lambda: duck_conn, "duckdb")

    engine_adapter.create_table(
        "apply_to_table",
        {
            "id": exp.DataType.build("int"),
            "name": exp.DataType.build("text"),
            "price": exp.DataType.build("double"),
            "ds": exp.DataType.build("text"),
        },
    )

    engine_adapter.create_table(
        "schema_from_table",
        {
            "name": exp.DataType.build("int"),
            "id": exp.DataType.build("int"),
            "ds": exp.DataType.build("text"),
            "new_column": exp.DataType.build("double"),
        },
    )

    assert table_diff("apply_to_table", "schema_from_table", engine_adapter) == [
        SchemaDelta.drop("price", "STRUCT<id INT, name VARCHAR, ds VARCHAR>", "DOUBLE"),
        SchemaDelta.add(
            "new_column",
            "DOUBLE",
            expected_table_struct="STRUCT<id INT, name VARCHAR, ds VARCHAR, new_column DOUBLE>",
            position=ColumnPosition.create_last("ds"),
        ),
        SchemaDelta.alter_type(
            "name",
            "INTEGER",
            "VARCHAR",
            expected_table_struct="STRUCT<id INT, name INTEGER, ds VARCHAR, new_column DOUBLE>",
            position=ColumnPosition.create_middle(after="id"),
        ),
    ]
