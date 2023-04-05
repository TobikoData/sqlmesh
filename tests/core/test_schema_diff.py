import typing as t
from unittest.mock import call

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.schema_diff import (
    ColumnPosition,
    Columns,
    SchemaDelta,
    SchemaDeltaOp,
    struct_diff,
    table_diff,
)


def test_schema_diff_calculate(mocker: MockerFixture):
    apply_to_table_name = "apply_to_table"
    schema_from_table_name = "schema_from_table"

    def table_columns(table_name: str) -> t.Dict[str, str]:
        if table_name == apply_to_table_name:
            return {
                "id": "INT",
                "name": "STRING",
                "price": "DOUBLE",
                "ds": "STRING",
            }
        else:
            return {
                "name": "INT",
                "id": "INT",
                "ds": "STRING",
                "new_column": "DOUBLE",
            }

    engine_adapter_mock = mocker.Mock()
    engine_adapter_mock.columns.side_effect = table_columns

    assert table_diff(apply_to_table_name, schema_from_table_name, engine_adapter_mock) == [
        SchemaDelta.drop("price", "DOUBLE"),
        SchemaDelta.add(
            "new_column",
            "DOUBLE",
            ColumnPosition.create_last(Columns(columns=[("ds", exp.DataType.build("STRING"))])),
        ),
        SchemaDelta.alter_type(
            "name", "INT", ColumnPosition.create_middle(after=Columns.create("id", "INT"))
        ),
    ]

    engine_adapter_mock.columns.assert_has_calls(
        [call(apply_to_table_name), call(schema_from_table_name)]
    )


def test_schema_diff_calculate_type_transitions(mocker: MockerFixture):
    apply_to_table_name = "apply_to_table"
    schema_from_table_name = "schema_from_table"

    def table_columns(table_name: str) -> t.Dict[str, str]:
        if table_name == apply_to_table_name:
            return {
                "id": "INT",
                "ds": "STRING",
            }
        else:
            return {
                "id": "BIGINT",
                "ds": "INT",
            }

    engine_adapter_mock = mocker.Mock()
    engine_adapter_mock.columns.side_effect = table_columns

    assert table_diff(apply_to_table_name, schema_from_table_name, engine_adapter_mock) == [
        SchemaDelta.alter_type("id", "BIGINT", ColumnPosition.create_first()),
        SchemaDelta.alter_type(
            "ds", "INT", ColumnPosition.create_last(Columns.create("id", "BIGINT"))
        ),
    ]

    engine_adapter_mock.columns.assert_has_calls(
        [call(apply_to_table_name), call(schema_from_table_name)]
    )


def test_schema_diff_struct_add_column(mocker: MockerFixture):
    apply_to_table_name = "apply_to_table"
    schema_from_table_name = "schema_from_table"

    def table_columns(table_name: str) -> t.Dict[str, str]:
        if table_name == apply_to_table_name:
            return {
                "complex": "STRUCT<id: INT, name: STRING>",
                "ds": "STRING",
            }
        else:
            return {
                "complex": "STRUCT<id: INT, new_column: DOUBLE, name: STRING>",
                "ds": "INT",
            }

    engine_adapter_mock = mocker.Mock()
    engine_adapter_mock.columns.side_effect = table_columns

    assert table_diff(apply_to_table_name, schema_from_table_name, engine_adapter_mock) == [
        SchemaDelta.add(
            "complex.new_column",
            "DOUBLE",
            ColumnPosition.create_middle(
                Columns(
                    columns=[
                        ("complex", exp.DataType.build("STRUCT")),
                        ("id", exp.DataType.build("INT")),
                    ]
                )
            ),
        ),
        SchemaDelta.alter_type(
            "ds", "INT", ColumnPosition.create_last(Columns.create("complex", "STRUCT"))
        ),
    ]

    engine_adapter_mock.columns.assert_has_calls(
        [call(apply_to_table_name), call(schema_from_table_name)]
    )


@pytest.mark.parametrize(
    "current_struct, new_struct, expected_diff",
    [
        ###########
        # Add Tests
        ###########
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
                SchemaDelta(
                    column_name="address",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=True,
                        after=Columns.create("age", "INT"),
                    ),
                )
            ],
        ),
        # Add root level column at the beginning
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<address STRING, id INT, name STRING, age INT>",
            [
                SchemaDelta(
                    column_name="address",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=True,
                        is_last=False,
                    ),
                )
            ],
        ),
        # Add root level column in the middle
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, address STRING, name STRING, age INT>",
            [
                SchemaDelta(
                    column_name="address",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=False,
                        after=Columns.create("id", "INT"),
                    ),
                )
            ],
        ),
        # Add columns at the beginning, middle, and end
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<address STRING, id INT, address2 STRING, name STRING, age INT, address3 STRING>",
            [
                SchemaDelta(
                    column_name="address",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=True,
                        is_last=False,
                    ),
                ),
                SchemaDelta(
                    column_name="address2",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=False,
                        after=Columns.create("id", "INT"),
                    ),
                ),
                SchemaDelta(
                    column_name="address3",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=True,
                        after=Columns.create("age", "INT"),
                    ),
                ),
            ],
        ),
        # Add two columns next to each other at the start
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<address STRING, address2 STRING, id INT, name STRING, age INT>",
            [
                SchemaDelta(
                    column_name="address",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=True,
                        is_last=False,
                    ),
                ),
                SchemaDelta(
                    column_name="address2",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=False,
                        after=Columns.create("address", "STRING"),
                    ),
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
                SchemaDelta(
                    column_name="id",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                )
            ],
        ),
        # Drop root level column in the middle
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, age INT>",
            [
                SchemaDelta(
                    column_name="name",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.DROP,
                )
            ],
        ),
        # Drop root level column at the end
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, name STRING>",
            [
                SchemaDelta(
                    column_name="age",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                )
            ],
        ),
        # Drop root level column at start, middle, and end
        (
            "STRUCT<id INT, name STRING, middle STRING, address STRING, age INT>",
            "STRUCT<name STRING, address STRING>",
            [
                SchemaDelta(
                    column_name="id",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                ),
                SchemaDelta(
                    column_name="middle",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.DROP,
                ),
                SchemaDelta(
                    column_name="age",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                ),
            ],
        ),
        # Drop two columns next to each other at the start
        (
            "STRUCT<address STRING, address2 STRING, id INT, name STRING, age INT>",
            "STRUCT<id INT, name STRING, age INT>",
            [
                SchemaDelta(
                    column_name="address",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.DROP,
                ),
                SchemaDelta(
                    column_name="address2",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.DROP,
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
                SchemaDelta(
                    column_name="id",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.ALTER_TYPE,
                    add_position=ColumnPosition.create_first(),
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
                SchemaDelta(
                    column_name="name",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.DROP,
                ),
                SchemaDelta(
                    column_name="address",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=True,
                        after=Columns.create("age", "INT"),
                    ),
                ),
                SchemaDelta(
                    column_name="id",
                    column_type=exp.DataType.build("STRING"),
                    op=SchemaDeltaOp.ALTER_TYPE,
                    add_position=ColumnPosition.create_first(),
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
                SchemaDelta(
                    column_name="info.col_d",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=True,
                        is_last=False,
                        after=None,
                    ),
                ),
            ],
        ),
        # Add a column to the end of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT, col_d INT>>",
            [
                SchemaDelta(
                    column_name="info.col_d",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=True,
                        after=Columns(
                            columns=[
                                ("info", exp.DataType.build("STRUCT")),
                                ("col_c", exp.DataType.build("INT")),
                            ]
                        ),
                    ),
                ),
            ],
        ),
        # Add a column to the middle of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_d INT, col_b INT, col_c INT>>",
            [
                SchemaDelta(
                    column_name="info.col_d",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=False,
                        after=Columns(
                            columns=[
                                ("info", exp.DataType.build("STRUCT")),
                                ("col_a", exp.DataType.build("INT")),
                            ]
                        ),
                    ),
                ),
            ],
        ),
        # Add two columns at the start of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_d INT, col_e INT, col_a INT, col_b INT, col_c INT>>",
            [
                SchemaDelta(
                    column_name="info.col_d",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=True,
                        is_last=False,
                        after=None,
                    ),
                ),
                SchemaDelta(
                    column_name="info.col_e",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=False,
                        after=Columns(
                            columns=[
                                ("info", exp.DataType.build("STRUCT")),
                                ("col_d", exp.DataType.build("INT")),
                            ]
                        ),
                    ),
                ),
            ],
        ),
        # Remove a column from the start of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_b INT, col_c INT>>",
            [
                SchemaDelta(
                    column_name="info.col_a",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                ),
            ],
        ),
        # Remove a column from the end of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT>>",
            [
                SchemaDelta(
                    column_name="info.col_c",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                ),
            ],
        ),
        # Remove a column from the middle of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_c INT>>",
            [
                SchemaDelta(
                    column_name="info.col_b",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                ),
            ],
        ),
        # Remove two columns from the start of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_c INT>>",
            [
                SchemaDelta(
                    column_name="info.col_a",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                ),
                SchemaDelta(
                    column_name="info.col_b",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                ),
            ],
        ),
        # Change a column type in a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c TEXT>>",
            [
                SchemaDelta(
                    column_name="info.col_c",
                    column_type=exp.DataType.build("TEXT"),
                    op=SchemaDeltaOp.ALTER_TYPE,
                    add_position=ColumnPosition.create_last(
                        after=Columns(
                            columns=[
                                ("info", exp.DataType.build("STRUCT")),
                                ("col_b", exp.DataType.build("INT")),
                            ]
                        )
                    ),
                ),
            ],
        ),
        # Add, remove and change a column in a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_d INT, col_b INT, col_e INT, col_c TEXT>>",
            [
                SchemaDelta(
                    column_name="info.col_a",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                ),
                SchemaDelta(
                    column_name="info.col_d",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=True,
                        is_last=False,
                        after=None,
                    ),
                ),
                SchemaDelta(
                    column_name="info.col_e",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=False,
                        after=Columns(
                            columns=[
                                ("info", exp.DataType.build("STRUCT")),
                                ("col_b", exp.DataType.build("INT")),
                            ]
                        ),
                    ),
                ),
                SchemaDelta(
                    column_name="info.col_c",
                    column_type=exp.DataType.build("TEXT"),
                    op=SchemaDeltaOp.ALTER_TYPE,
                    add_position=ColumnPosition.create_last(
                        after=Columns(
                            columns=[
                                ("info", exp.DataType.build("STRUCT")),
                                ("col_e", exp.DataType.build("INT")),
                            ]
                        )
                    ),
                ),
            ],
        ),
        # Add and remove from outer and nested struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, nested_info STRUCT<nest_col_a INT, nest_col_b INT>>>",
            "STRUCT<id INT, info STRUCT<col_a INT, nested_info STRUCT<nest_col_a INT, col_c INT>, col_c INT>>",
            [
                SchemaDelta(
                    column_name="info.col_b",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                ),
                SchemaDelta(
                    column_name="info.col_c",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=True,
                        after=Columns(
                            columns=[
                                ("info", exp.DataType.build("STRUCT")),
                                ("nested_info", exp.DataType.build("STRUCT")),
                            ]
                        ),
                    ),
                ),
                SchemaDelta(
                    column_name="info.nested_info.nest_col_b",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                ),
                SchemaDelta(
                    column_name="info.nested_info.col_c",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=True,
                        after=Columns(
                            columns=[
                                ("info", exp.DataType.build("STRUCT")),
                                ("nested_info", exp.DataType.build("STRUCT")),
                                ("nest_col_a", exp.DataType.build("INT")),
                            ]
                        ),
                    ),
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
                SchemaDelta(
                    column_name="infos.col_d",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=False,
                        after=Columns(
                            columns=[
                                ("infos", exp.DataType.build("ARRAY")),
                                ("col_b", exp.DataType.build("INT")),
                            ]
                        ),
                    ),
                ),
            ],
        ),
        # Remove column from array of structs
        (
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>>",
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_c INT>>>",
            [
                SchemaDelta(
                    column_name="infos.col_b",
                    column_type=exp.DataType.build("INT"),
                    op=SchemaDeltaOp.DROP,
                ),
            ],
        ),
        # Alter column type in array of structs
        (
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>>",
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c TEXT>>>",
            [
                SchemaDelta(
                    column_name="infos.col_c",
                    column_type=exp.DataType.build("TEXT"),
                    op=SchemaDeltaOp.ALTER_TYPE,
                    add_position=ColumnPosition.create_last(
                        after=Columns(
                            columns=[
                                ("infos", exp.DataType.build("ARRAY")),
                                ("col_b", exp.DataType.build("INT")),
                            ]
                        )
                    ),
                ),
            ],
        ),
        # Add an array of primitives
        (
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>>",
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>, values ARRAY<INT>>",
            [
                SchemaDelta(
                    column_name="values",
                    column_type=exp.DataType.build("ARRAY<INT>"),
                    op=SchemaDeltaOp.ADD,
                    add_position=ColumnPosition(
                        is_first=False,
                        is_last=True,
                        after=Columns.create("infos", "ARRAY"),
                    ),
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
            parse_one(current_struct) if isinstance(current_struct, str) else current_struct,
            parse_one(new_struct) if isinstance(new_struct, str) else new_struct,
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
        SchemaDelta.drop("price", "DOUBLE"),
        SchemaDelta.add(
            "new_column",
            "DOUBLE",
            position=ColumnPosition.create_last(after=Columns.create("ds", "VARCHAR")),
        ),
        SchemaDelta.alter_type(
            "name", "INTEGER", ColumnPosition.create_middle(after=Columns.create("id", "INTEGER"))
        ),
    ]
