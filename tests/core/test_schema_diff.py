import typing as t

import pytest
from sqlglot import exp

from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.schema_diff import (
    SchemaDiffer,
    TableAlterColumn,
    TableAlterColumnPosition,
    TableAlterOperation,
)


def test_schema_diff_calculate():
    alter_expressions = SchemaDiffer(
        **{
            "support_positional_add": False,
            "support_nested_operations": False,
            "array_element_selector": "",
            "compatible_types": {
                exp.DataType.build("STRING"): {exp.DataType.build("INT")},
            },
        }
    ).compare_columns(
        "apply_to_table",
        {
            "id": exp.DataType.build("INT"),
            "name": exp.DataType.build("STRING"),
            "price": exp.DataType.build("DOUBLE"),
            "ds": exp.DataType.build("STRING"),
        },
        {
            "name": exp.DataType.build("INT"),
            "id": exp.DataType.build("INT"),
            "ds": exp.DataType.build("STRING"),
            "new_column": exp.DataType.build("DOUBLE"),
        },
    )

    assert [x.sql() for x in alter_expressions] == [
        """ALTER TABLE apply_to_table DROP COLUMN price""",
        """ALTER TABLE apply_to_table ADD COLUMN new_column DOUBLE""",
        """ALTER TABLE apply_to_table ALTER COLUMN name TYPE INT""",
    ]


def test_schema_diff_calculate_type_transitions():
    alter_expressions = SchemaDiffer(
        **{
            "support_positional_add": False,
            "support_nested_operations": False,
            "array_element_selector": "",
            "compatible_types": {
                exp.DataType.build("STRING"): {exp.DataType.build("INT")},
            },
        }
    ).compare_columns(
        "apply_to_table",
        {
            "id": exp.DataType.build("INT"),
            "ds": exp.DataType.build("STRING"),
        },
        {
            "id": exp.DataType.build("BIGINT"),
            "ds": exp.DataType.build("INT"),
        },
    )

    assert [x.sql() for x in alter_expressions] == [
        """ALTER TABLE apply_to_table DROP COLUMN id""",
        """ALTER TABLE apply_to_table ADD COLUMN id BIGINT""",
        """ALTER TABLE apply_to_table ALTER COLUMN ds TYPE INT""",
    ]


@pytest.mark.parametrize(
    "current_struct, new_struct, expected_diff, config",
    [
        # ###########
        # # Add Tests
        # ###########
        # No diff
        ("STRUCT<id INT, name STRING, age INT>", "STRUCT<id INT, name STRING, age INT>", [], {}),
        # Add root level column at the end
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, name STRING, age INT, address STRING>",
            [
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "STRING",
                    "STRUCT<id INT, name STRING, age INT, address STRING>",
                )
            ],
            {},
        ),
        # Add root level column at the beginning
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<address STRING, id INT, name STRING, age INT>",
            [
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "STRING",
                    expected_table_struct="STRUCT<address STRING, id INT, name STRING, age INT>",
                    position=TableAlterColumnPosition.first(),
                )
            ],
            dict(support_positional_add=True),
        ),
        # Add root level column in the middle
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, address STRING, name STRING, age INT>",
            [
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "STRING",
                    expected_table_struct="STRUCT<id INT, address STRING, name STRING, age INT>",
                    position=TableAlterColumnPosition.middle(after="id"),
                )
            ],
            dict(support_positional_add=True),
        ),
        # Add columns at the beginning, middle, and end
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<address STRING, id INT, address2 STRING, name STRING, age INT, address3 STRING>",
            [
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "STRING",
                    expected_table_struct="STRUCT<address STRING, id INT, name STRING, age INT>",
                    position=TableAlterColumnPosition.first(),
                ),
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address2"),
                    "STRING",
                    expected_table_struct="STRUCT<address STRING, id INT, address2 STRING, name STRING, age INT>",
                    position=TableAlterColumnPosition.middle(after="id"),
                ),
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address3"),
                    "STRING",
                    expected_table_struct="STRUCT<address STRING, id INT, address2 STRING, name STRING, age INT, address3 STRING>",
                    position=TableAlterColumnPosition.last(after="age"),
                ),
            ],
            dict(support_positional_add=True),
        ),
        # Add two columns next to each other at the start
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<address STRING, address2 STRING, id INT, name STRING, age INT>",
            [
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "STRING",
                    expected_table_struct="STRUCT<address STRING, id INT, name STRING, age INT>",
                    position=TableAlterColumnPosition.first(),
                ),
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address2"),
                    "STRING",
                    expected_table_struct="STRUCT<address STRING, address2 STRING, id INT, name STRING, age INT>",
                    position=TableAlterColumnPosition.middle(after="address"),
                ),
            ],
            dict(support_positional_add=True),
        ),
        ############
        # Drop Tests
        ############
        # Drop root level column at the start
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<name STRING, age INT>",
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("id"),
                    "STRUCT<name STRING, age INT>",
                    "INT",
                )
            ],
            {},
        ),
        # Drop root level column in the middle
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, age INT>",
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("name"),
                    "STRUCT<id INT, age INT>",
                    "STRING",
                )
            ],
            {},
        ),
        # Drop root level column at the end
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id INT, name STRING>",
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("age"),
                    "STRUCT<id INT, name STRING>",
                    "INT",
                )
            ],
            {},
        ),
        # Drop root level column at start, middle, and end
        (
            "STRUCT<id INT, name STRING, middle STRING, address STRING, age INT>",
            "STRUCT<name STRING, address STRING>",
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("id"),
                    "STRUCT<name STRING, middle STRING, address STRING, age INT>",
                    "INT",
                ),
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("middle"),
                    "STRUCT<name STRING, address STRING, age INT>",
                    "STRING",
                ),
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("age"),
                    "STRUCT<name STRING, address STRING>",
                    "INT",
                ),
            ],
            {},
        ),
        # Drop two columns next to each other at the start
        (
            "STRUCT<address STRING, address2 STRING, id INT, name STRING, age INT>",
            "STRUCT<id INT, name STRING, age INT>",
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("address"),
                    "STRUCT<address2 STRING, id INT, name STRING, age INT>",
                    "STRING",
                ),
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("address2"),
                    "STRUCT<id INT, name STRING, age INT>",
                    "STRING",
                ),
            ],
            {},
        ),
        #############
        # Move Tests
        #############
        # Move root level column at the start
        ("STRUCT<id INT, name STRING, age INT>", "STRUCT<name STRING, id INT, age INT>", [], {}),
        # Move root level column in the middle
        ("STRUCT<id INT, name STRING, age INT>", "STRUCT<id INT, age INT, name STRING>", [], {}),
        # Move root level column at the end
        ("STRUCT<id INT, name STRING, age INT>", "STRUCT<age INT, id INT, name STRING>", [], {}),
        # ###################
        # # Type Change Tests
        # ###################
        # # Change root level column type that is allowed
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id STRING, name STRING, age INT>",
            [
                TableAlterOperation.alter_type(
                    TableAlterColumn.primitive("id"),
                    "STRING",
                    current_type="INT",
                    expected_table_struct="STRUCT<id STRING, name STRING, age INT>",
                )
            ],
            dict(
                compatible_types={
                    exp.DataType.build("INT"): {exp.DataType.build("STRING")},
                }
            ),
        ),
        # ############
        # # Mix Tests
        # ############
        # # Add, drop, and change type
        (
            "STRUCT<id INT, name STRING, age INT>",
            "STRUCT<id STRING, age INT, address STRING>",
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("name"),
                    "STRUCT<id INT, age INT>",
                    "STRING",
                ),
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "STRING",
                    expected_table_struct="STRUCT<id INT, age INT, address STRING>",
                ),
                TableAlterOperation.alter_type(
                    TableAlterColumn.primitive("id"),
                    "STRING",
                    current_type="INT",
                    expected_table_struct="STRUCT<id STRING, age INT, address STRING>",
                ),
            ],
            dict(
                compatible_types={
                    exp.DataType.build("INT"): {exp.DataType.build("STRING")},
                }
            ),
        ),
        # ##############
        # # Struct Tests
        # ##############
        # Add a column to the start of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_d INT, col_a INT, col_b INT, col_c INT>>",
            [
                TableAlterOperation.add(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_d"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_d INT, col_a INT, col_b INT, col_c INT>>",
                    position=TableAlterColumnPosition.first(),
                ),
            ],
            dict(support_positional_add=True, support_nested_operations=True),
        ),
        # Add a column to the end of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT, col_d INT>>",
            [
                TableAlterOperation.add(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_d"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT, col_d INT>>",
                    position=TableAlterColumnPosition.last(after="col_c"),
                ),
            ],
            dict(support_positional_add=True, support_nested_operations=True),
        ),
        # Add a column to the middle of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_d INT, col_b INT, col_c INT>>",
            [
                TableAlterOperation.add(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_d"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, col_d INT, col_b INT, col_c INT>>",
                    position=TableAlterColumnPosition.middle(after="col_a"),
                ),
            ],
            dict(support_positional_add=True, support_nested_operations=True),
        ),
        # Add two columns at the start of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_d INT, col_e INT, col_a INT, col_b INT, col_c INT>>",
            [
                TableAlterOperation.add(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_d"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_d INT, col_a INT, col_b INT, col_c INT>>",
                    position=TableAlterColumnPosition.first(),
                ),
                TableAlterOperation.add(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_e"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_d INT, col_e INT, col_a INT, col_b INT, col_c INT>>",
                    position=TableAlterColumnPosition.middle(after="col_d"),
                ),
            ],
            dict(support_positional_add=True, support_nested_operations=True),
        ),
        # Remove a column from the start of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_b INT, col_c INT>>",
            [
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_a"),
                    ],
                    "STRUCT<id INT, info STRUCT<col_b INT, col_c INT>>",
                    "INT",
                ),
            ],
            dict(support_positional_add=True, support_nested_operations=True),
        ),
        # Remove a column from the end of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT>>",
            [
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_c"),
                    ],
                    "STRUCT<id INT, info STRUCT<col_a INT, col_b INT>>",
                    "INT",
                ),
            ],
            dict(support_positional_add=True, support_nested_operations=True),
        ),
        # Remove a column from the middle of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_c INT>>",
            [
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_b"),
                    ],
                    "STRUCT<id INT, info STRUCT<col_a INT, col_c INT>>",
                    "INT",
                ),
            ],
            dict(support_positional_add=True, support_nested_operations=True),
        ),
        # Remove two columns from the start of a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_c INT>>",
            [
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_a"),
                    ],
                    "STRUCT<id INT, info STRUCT<col_b INT, col_c INT>>",
                    "INT",
                ),
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_b"),
                    ],
                    "STRUCT<id INT, info STRUCT<col_c INT>>",
                    "INT",
                ),
            ],
            dict(support_positional_add=True, support_nested_operations=True),
        ),
        # Change a column type in a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c TEXT>>",
            [
                TableAlterOperation.alter_type(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_c"),
                    ],
                    "TEXT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c TEXT>>",
                    position=TableAlterColumnPosition.last(after="col_b"),
                    current_type=exp.DataType.build("INT"),
                ),
            ],
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                compatible_types={
                    exp.DataType.build("INT"): {exp.DataType.build("TEXT")},
                },
            ),
        ),
        # Add, remove and change a column in a struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_d INT, col_b INT, col_e INT, col_c TEXT>>",
            [
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_a"),
                    ],
                    "STRUCT<id INT, info STRUCT<col_b INT, col_c INT>>",
                    "INT",
                ),
                TableAlterOperation.add(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_d"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_d INT, col_b INT, col_c INT>>",
                    position=TableAlterColumnPosition.first(),
                ),
                TableAlterOperation.add(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_e"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_d INT, col_b INT, col_e INT, col_c INT>>",
                    position=TableAlterColumnPosition.middle(after="col_b"),
                ),
                TableAlterOperation.alter_type(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_c"),
                    ],
                    "TEXT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_d INT, col_b INT, col_e INT, col_c TEXT>>",
                    position=TableAlterColumnPosition.last(after="col_e"),
                    current_type=exp.DataType.build("INT"),
                ),
            ],
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                compatible_types={
                    exp.DataType.build("INT"): {exp.DataType.build("TEXT")},
                },
            ),
        ),
        # Add and remove from outer and nested struct
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, nested_info STRUCT<nest_col_a INT, nest_col_b INT>>>",
            "STRUCT<id INT, info STRUCT<col_a INT, nested_info STRUCT<nest_col_a INT, nest_col_c INT>, col_c INT>>",
            [
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_b"),
                    ],
                    "STRUCT<id INT, info STRUCT<col_a INT, nested_info STRUCT<nest_col_a INT, nest_col_b INT>>>",
                    "INT",
                ),
                TableAlterOperation.add(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_c"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, nested_info STRUCT<nest_col_a INT, nest_col_b INT>, col_c INT>>",
                    position=TableAlterColumnPosition.last("nested_info"),
                ),
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.struct("nested_info"),
                        TableAlterColumn.primitive("nest_col_b"),
                    ],
                    "STRUCT<id INT, info STRUCT<col_a INT, nested_info STRUCT<nest_col_a INT>, col_c INT>>",
                    "INT",
                ),
                TableAlterOperation.add(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.struct("nested_info"),
                        TableAlterColumn.primitive("nest_col_c"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, nested_info STRUCT<nest_col_a INT, nest_col_c INT>, col_c INT>>",
                    position=TableAlterColumnPosition.last("nest_col_a"),
                ),
            ],
            dict(support_positional_add=True, support_nested_operations=True),
        ),
        # #####################
        # # Array Struct Tests
        # #####################
        # Add column to array of structs
        (
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>>",
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_d INT, col_c INT>>>",
            [
                TableAlterOperation.add(
                    [
                        TableAlterColumn.array_of_struct("infos"),
                        TableAlterColumn.primitive("col_d"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_d INT, col_c INT>>>",
                    position=TableAlterColumnPosition.middle("col_b"),
                ),
            ],
            dict(support_positional_add=True, support_nested_operations=True),
        ),
        # Remove column from array of structs
        (
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>>",
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_c INT>>>",
            [
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.array_of_struct("infos"),
                        TableAlterColumn.primitive("col_b"),
                    ],
                    "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_c INT>>>",
                    "INT",
                ),
            ],
            dict(support_positional_add=True, support_nested_operations=True),
        ),
        # Alter column type in array of structs
        (
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>>",
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c TEXT>>>",
            [
                TableAlterOperation.alter_type(
                    [
                        TableAlterColumn.array_of_struct("infos"),
                        TableAlterColumn.primitive("col_c"),
                    ],
                    "TEXT",
                    expected_table_struct="STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c TEXT>>>",
                    position=TableAlterColumnPosition.last("col_b"),
                    current_type="INT",
                ),
            ],
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                compatible_types={
                    exp.DataType.build("INT"): {exp.DataType.build("TEXT")},
                },
            ),
        ),
        # Add an array of primitives
        (
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>>",
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>, values ARRAY<INT>>",
            [
                TableAlterOperation.add(
                    [
                        TableAlterColumn.array_of_primitive("values"),
                    ],
                    "ARRAY<INT>",
                    expected_table_struct="STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>, values ARRAY<INT>>",
                    position=TableAlterColumnPosition.last("infos"),
                ),
            ],
            dict(support_positional_add=True, support_nested_operations=True),
        ),
        # Precision VARCHAR is a no-op with no changes
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR(120)>",
            [],
            {},
        ),
        # Change the precision bits of a VARCHAR
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR(100)>",
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("address"),
                    "STRUCT<id INT>",
                    "VARCHAR(120)",
                ),
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR(100)",
                    expected_table_struct="STRUCT<id INT, address VARCHAR(100)>",
                    position=TableAlterColumnPosition.last("id"),
                ),
            ],
            dict(
                support_positional_add=True,
                support_nested_operations=True,
            ),
        ),
    ],
)
def test_struct_diff(
    current_struct,
    new_struct,
    expected_diff: t.List[TableAlterOperation],
    config: t.Dict[str, t.Any],
):
    config = {
        **config,
    }
    resolver = SchemaDiffer(**config)
    operations = resolver._from_structs(
        exp.DataType.build(current_struct), exp.DataType.build(new_struct)
    )
    assert operations == expected_diff


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

    engine_adapter.alter_table("apply_to_table", "schema_from_table")
    assert engine_adapter.columns("apply_to_table") == {
        "id": exp.DataType.build("int"),
        "ds": exp.DataType.build("varchar"),
        "new_column": exp.DataType.build("double"),
        "name": exp.DataType.build("int"),
    }


def test_schema_diff_alter_op_column():
    nested = TableAlterOperation.add(
        [
            TableAlterColumn.array_of_struct("nested"),
            TableAlterColumn.primitive("col_a"),
        ],
        "INT",
        expected_table_struct="STRUCT<id INT, nested ARRAY<STRUCT<col_a INT>>>",
        position=TableAlterColumnPosition.last("id"),
    )
    assert nested.column("").sql() == "nested.col_a"
    nested_complete_column = TableAlterOperation.add(
        [
            TableAlterColumn.array_of_struct("nested_1", quoted=True),
            TableAlterColumn.struct("nested_2"),
            TableAlterColumn.array_of_struct("nested_3"),
            TableAlterColumn.primitive("col_a", quoted=True),
        ],
        "INT",
        expected_table_struct="""STRUCT<id INT, "nested_1" ARRAY<STRUCT<nested_2 STRUCT<nested_3 ARRAY<STRUCT<"col_a" INT>>>>>>""",
        position=TableAlterColumnPosition.last("id"),
    )
    assert nested_complete_column.column("").sql() == '"nested_1".nested_2.nested_3."col_a"'
    nested_one_more_complete_column = TableAlterOperation.add(
        [
            TableAlterColumn.array_of_struct("nested_1", quoted=True),
            TableAlterColumn.struct("nested_2"),
            TableAlterColumn.array_of_struct("nested_3"),
            TableAlterColumn.struct("nested_4"),
            TableAlterColumn.primitive("col_a", quoted=True),
        ],
        "INT",
        expected_table_struct="""STRUCT<id INT, "nested_1" ARRAY<STRUCT<nested_2 STRUCT<nested_3 ARRAY<STRUCT<nested_4 STRUCT<"col_a" INT>>>>>>>""",
        position=TableAlterColumnPosition.last("id"),
    )
    assert (
        nested_one_more_complete_column.column("").sql()
        == '"nested_1".nested_2.nested_3.nested_4."col_a"'
    )
    super_nested = TableAlterOperation.add(
        [
            TableAlterColumn.array_of_struct("nested_1", quoted=True),
            TableAlterColumn.struct("nested_2"),
            TableAlterColumn.array_of_struct("nested_3"),
            TableAlterColumn.struct("nested_4"),
            TableAlterColumn.struct("nested_5"),
            TableAlterColumn.struct("nested_6", quoted=True),
            TableAlterColumn.struct("nested_7"),
            TableAlterColumn.array_of_struct("nested_8"),
            TableAlterColumn.primitive("col_a", quoted=True),
        ],
        "INT",
        expected_table_struct="""STRUCT<id INT, "nested_1" ARRAY<STRUCT<nested_2 STRUCT<nested_3 ARRAY<STRUCT<nested_4 STRUCT<nested_5 STRUCT<"nested_6" STRUCT<nested_7 STRUCT<nested_8 ARRAY<STRUCT<"col_a" INT>>>>>>>>>>>>""",
        position=TableAlterColumnPosition.last("id"),
    )
    assert (
        super_nested.column("element").sql()
        == '"nested_1".element.nested_2.nested_3.element.nested_4.nested_5."nested_6".nested_7.nested_8.element."col_a"'
    )
