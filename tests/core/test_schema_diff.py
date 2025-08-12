import typing as t

import pytest
from sqlglot import exp

from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.schema_diff import (
    SchemaDiffer,
    TableAlterColumn,
    TableAlterColumnPosition,
    TableAlterOperation,
    get_schema_differ,
    has_additive_changes,
    filter_additive_changes,
    has_drop_alteration,
    filter_destructive_changes,
    get_additive_changes,
)


def is_only_additive_changes(alter_expressions: t.List[exp.Alter]) -> bool:
    """
    Check if all changes in the list of alter expressions are additive.

    Args:
        alter_expressions: List of ALTER TABLE expressions

    Returns:
        True if all changes are additive (or no changes), False if there are any non-additive changes
    """
    if not alter_expressions:
        return True  # No changes at all - this should be considered "only additive"

    return len(get_additive_changes(alter_expressions)) == len(alter_expressions)


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
        """ALTER TABLE apply_to_table ALTER COLUMN name SET DATA TYPE INT""",
    ]


def test_schema_diff_drop_cascade():
    alter_expressions = SchemaDiffer(
        **{
            "support_positional_add": False,
            "support_nested_operations": False,
            "array_element_selector": "",
            "drop_cascade": True,
        }
    ).compare_columns(
        "apply_to_table",
        {
            "id": exp.DataType.build("INT"),
            "name": exp.DataType.build("STRING"),
            "price": exp.DataType.build("DOUBLE"),
        },
        {
            "id": exp.DataType.build("INT"),
            "name": exp.DataType.build("STRING"),
        },
    )

    assert [x.sql() for x in alter_expressions] == [
        """ALTER TABLE apply_to_table DROP COLUMN price CASCADE"""
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
        """ALTER TABLE apply_to_table ALTER COLUMN ds SET DATA TYPE INT""",
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
        # Add columns in different levels of nesting of structs
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT, col_d INT>, txt TEXT>",
            [
                TableAlterOperation.add(
                    [
                        TableAlterColumn.primitive("txt"),
                    ],
                    "TEXT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>, txt TEXT>",
                ),
                TableAlterOperation.add(
                    [
                        TableAlterColumn.struct("info"),
                        TableAlterColumn.primitive("col_d"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT, col_d INT>, txt TEXT>",
                ),
            ],
            dict(support_positional_add=False, support_nested_operations=True),
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
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                support_nested_drop=True,
            ),
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
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                support_nested_drop=True,
            ),
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
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                support_nested_drop=True,
            ),
        ),
        # Remove a column from a struct where nested drop is not supported
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT>>",
            [
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.struct("info"),
                    ],
                    expected_table_struct="STRUCT<id INT>",
                    column_type="STRUCT<col_a INT, col_b INT, col_c INT>",
                ),
                TableAlterOperation.add(
                    [
                        TableAlterColumn.struct("info"),
                    ],
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, col_b INT>>",
                    column_type="STRUCT<col_a INT, col_b INT>",
                    is_part_of_destructive_change=True,
                ),
            ],
            dict(
                support_nested_operations=True,
                support_nested_drop=False,
            ),
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
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                support_nested_drop=True,
            ),
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
                support_nested_drop=True,
                compatible_types={
                    exp.DataType.build("INT"): {exp.DataType.build("TEXT")},
                },
            ),
        ),
        # Add, remove and change a column from a struct where nested drop is not supported
        (
            "STRUCT<id INT, info STRUCT<col_a INT, col_b INT, col_c INT>>",
            "STRUCT<id INT, info STRUCT<col_a INT, col_b TEXT, col_d INT>>",
            [
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.struct("info"),
                    ],
                    expected_table_struct="STRUCT<id INT>",
                    column_type="STRUCT<col_a INT, col_b INT, col_c INT>",
                ),
                TableAlterOperation.add(
                    [
                        TableAlterColumn.struct("info"),
                    ],
                    expected_table_struct="STRUCT<id INT, info STRUCT<col_a INT, col_b TEXT, col_d INT>>",
                    column_type="STRUCT<col_a INT, col_b TEXT, col_d INT>",
                    is_part_of_destructive_change=True,
                ),
            ],
            dict(
                support_nested_operations=True,
                support_nested_drop=False,
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
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                support_nested_drop=True,
            ),
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
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                support_nested_drop=True,
            ),
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
        # Add columns to struct of array within different nesting levels
        (
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>>",
            "STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT, col_d INT >>, col_e INT>",
            [
                TableAlterOperation.add(
                    [
                        TableAlterColumn.primitive("col_e"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT, col_c INT>>, col_e INT>",
                ),
                TableAlterOperation.add(
                    [
                        TableAlterColumn.array_of_struct("infos"),
                        TableAlterColumn.primitive("col_d"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, infos ARRAY<STRUCT<col_a INT, col_b INT,  col_c INT, col_d INT>>, col_e INT>",
                ),
            ],
            dict(support_positional_add=False, support_nested_operations=True),
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
        # untyped array to support Snowflake
        (
            "STRUCT<id INT, ids ARRAY>",
            "STRUCT<id INT, ids ARRAY>",
            [],
            {},
        ),
        # Primitive to untyped array
        (
            "STRUCT<id INT, ids INT>",
            "STRUCT<id INT, ids ARRAY>",
            [
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.primitive("ids"),
                    ],
                    "STRUCT<id INT>",
                    "INT",
                ),
                TableAlterOperation.add(
                    [
                        TableAlterColumn.primitive("ids"),
                    ],
                    "ARRAY",
                    expected_table_struct="STRUCT<id INT, ids ARRAY>",
                    is_part_of_destructive_change=True,
                ),
            ],
            {},
        ),
        # untyped array to primitive
        (
            "STRUCT<id INT, ids ARRAY>",
            "STRUCT<id INT, ids INT>",
            [
                TableAlterOperation.drop(
                    [
                        TableAlterColumn.array_of_primitive("ids"),
                    ],
                    "STRUCT<id INT>",
                    "ARRAY",
                ),
                TableAlterOperation.add(
                    [
                        TableAlterColumn.array_of_primitive("ids"),
                    ],
                    "INT",
                    expected_table_struct="STRUCT<id INT, ids INT>",
                    is_part_of_destructive_change=True,
                ),
            ],
            {},
        ),
        # ############
        # Precision Tests
        # ############
        # Identical precision is a no-op with no changes
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR(120)>",
            [],
            {},
        ),
        # Increase the precision of a type is ALTER
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR(121)>",
            [
                TableAlterOperation.alter_type(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR(121)",
                    current_type="VARCHAR(120)",
                    expected_table_struct="STRUCT<id INT, address VARCHAR(121)>",
                )
            ],
            {},
        ),
        # Increase the precision of a type is ALTER when the type is supported
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR(121)>",
            [
                TableAlterOperation.alter_type(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR(121)",
                    current_type="VARCHAR(120)",
                    expected_table_struct="STRUCT<id INT, address VARCHAR(121)>",
                )
            ],
            dict(
                precision_increase_allowed_types={exp.DataType.build("VARCHAR").this},
            ),
        ),
        # Increase the precision of a type is DROP/ADD when the type is not supported
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR(121)>",
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("address"),
                    "STRUCT<id INT>",
                    "VARCHAR(120)",
                ),
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR(121)",
                    expected_table_struct="STRUCT<id INT, address VARCHAR(121)>",
                    is_part_of_destructive_change=True,
                ),
            ],
            dict(
                precision_increase_allowed_types={exp.DataType.build("DECIMAL").this},
            ),
        ),
        # Decrease the precision of a type is DROP/ADD
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
                    is_part_of_destructive_change=True,
                ),
            ],
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                support_nested_drop=True,
            ),
        ),
        # Type with precision to same type with no precision and no default is DROP/ADD
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR>",
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("address"),
                    "STRUCT<id INT>",
                    "VARCHAR(120)",
                ),
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR",
                    expected_table_struct="STRUCT<id INT, address VARCHAR>",
                    position=TableAlterColumnPosition.last("id"),
                    is_part_of_destructive_change=True,
                ),
            ],
            dict(
                support_positional_add=True,
            ),
        ),
        # Type with no precision and no default to same type with precision is DROP/ADD
        (
            "STRUCT<id INT, address VARCHAR>",
            "STRUCT<id INT, address VARCHAR(120)>",
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("address"),
                    "STRUCT<id INT>",
                    "VARCHAR",
                ),
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR(120)",
                    expected_table_struct="STRUCT<id INT, address VARCHAR(120)>",
                    position=TableAlterColumnPosition.last("id"),
                    is_part_of_destructive_change=True,
                ),
            ],
            dict(
                support_positional_add=True,
            ),
        ),
        # Increase precision of a type from a default is ALTER
        (
            "STRUCT<id INT, address VARCHAR>",  # default of 1 --> VARCHAR(1)
            "STRUCT<id INT, address VARCHAR(2)>",
            [
                TableAlterOperation.alter_type(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR(2)",
                    current_type="VARCHAR",
                    expected_table_struct="STRUCT<id INT, address VARCHAR(2)>",
                )
            ],
            dict(
                parameterized_type_defaults={
                    exp.DataType.build("VARCHAR").this: [(1,)],
                },
            ),
        ),
        # Decrease precision of a type to a default is DROP/ADD
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR>",  # default of 1 --> VARCHAR(1)
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("address"),
                    "STRUCT<id INT>",
                    "VARCHAR(120)",
                ),
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR",
                    expected_table_struct="STRUCT<id INT, address VARCHAR>",
                    position=TableAlterColumnPosition.last("id"),
                    is_part_of_destructive_change=True,
                ),
            ],
            dict(
                parameterized_type_defaults={
                    exp.DataType.build("VARCHAR").this: [(1,)],
                },
                support_positional_add=True,
            ),
        ),
        # Change the precision of a type to exact "max" value is ALTER
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR(max)>",
            [
                TableAlterOperation.alter_type(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR(max)",
                    current_type="VARCHAR(120)",
                    expected_table_struct="STRUCT<id INT, address VARCHAR(max)>",
                )
            ],
            dict(
                max_parameter_length={
                    exp.DataType.build("VARCHAR").this: 120,
                },
            ),
        ),
        # Increase the precision of a type to larger "max" is ALTER
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR(max)>",
            [
                TableAlterOperation.alter_type(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR(max)",
                    current_type="VARCHAR(120)",
                    expected_table_struct="STRUCT<id INT, address VARCHAR(max)>",
                )
            ],
            dict(
                max_parameter_length={
                    exp.DataType.build("VARCHAR").this: 121,
                },
            ),
        ),
        # Decrease the precision of a type from "max" to smaller value is DROP/ADD
        (
            "STRUCT<id INT, address VARCHAR(max)>",
            "STRUCT<id INT, address VARCHAR(120)>",
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("address"),
                    "STRUCT<id INT>",
                    "VARCHAR(max)",
                ),
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR(120)",
                    expected_table_struct="STRUCT<id INT, address VARCHAR(120)>",
                    position=TableAlterColumnPosition.last("id"),
                    is_part_of_destructive_change=True,
                ),
            ],
            dict(
                support_positional_add=True,
                max_parameter_length={
                    exp.DataType.build("VARCHAR").this: 121,
                },
            ),
        ),
        # Increase the precision of a type to no-precision unlimited is ALTER
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address VARCHAR>",
            [
                TableAlterOperation.alter_type(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR",
                    current_type="VARCHAR(120)",
                    expected_table_struct="STRUCT<id INT, address VARCHAR>",
                )
            ],
            dict(
                types_with_unlimited_length={
                    exp.DataType.build("VARCHAR").this: {
                        exp.DataType.build("VARCHAR").this,
                    },
                }
            ),
        ),
        # Decrease the precision of a type from no-precision unlimited to any precision is DROP/ADD
        (
            "STRUCT<id INT, address VARCHAR>",
            "STRUCT<id INT, address VARCHAR(120)>",
            [
                TableAlterOperation.drop(
                    TableAlterColumn.primitive("address"),
                    "STRUCT<id INT>",
                    "VARCHAR",
                ),
                TableAlterOperation.add(
                    TableAlterColumn.primitive("address"),
                    "VARCHAR(120)",
                    expected_table_struct="STRUCT<id INT, address VARCHAR(120)>",
                    position=TableAlterColumnPosition.last("id"),
                    is_part_of_destructive_change=True,
                ),
            ],
            dict(
                support_positional_add=True,
                types_with_unlimited_length={
                    exp.DataType.build("VARCHAR").this: {
                        exp.DataType.build("VARCHAR").this,
                    },
                },
            ),
        ),
        # Increase the precision of one type to a different no-precision unlimited type is ALTER
        (
            "STRUCT<id INT, address VARCHAR(120)>",
            "STRUCT<id INT, address TEXT>",
            [
                TableAlterOperation.alter_type(
                    TableAlterColumn.primitive("address"),
                    "TEXT",
                    current_type="VARCHAR(120)",
                    expected_table_struct="STRUCT<id INT, address TEXT>",
                )
            ],
            dict(
                types_with_unlimited_length={
                    exp.DataType.build("TEXT").this: {
                        exp.DataType.build("VARCHAR").this,
                    },
                }
            ),
        ),
        # ############
        # Coerce Tests
        # ############
        # Single coercion results in no operation
        (
            "STRUCT<id INT, name STRING, revenue FLOAT>",
            "STRUCT<id INT, name STRING, revenue INT>",
            [],
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                support_coercing_compatible_types=True,
                compatible_types={
                    exp.DataType.build("INT"): {exp.DataType.build("FLOAT")},
                },
            ),
        ),
        (
            "STRUCT<id INT, name STRING, revenue FLOAT>",
            "STRUCT<id INT, name STRING, revenue INT>",
            [],
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                coerceable_types={
                    exp.DataType.build("FLOAT"): {exp.DataType.build("INT")},
                },
            ),
        ),
        (
            "STRUCT<id INT, name STRING, revenue FLOAT>",
            "STRUCT<id INT, name INT, revenue INT>",
            [],
            dict(
                support_positional_add=True,
                support_nested_operations=True,
                support_coercing_compatible_types=True,
                compatible_types={
                    exp.DataType.build("INT"): {exp.DataType.build("FLOAT")},
                },
                coerceable_types={
                    exp.DataType.build("STRING"): {exp.DataType.build("INT")},
                },
            ),
        ),
        # Coercion with an alter results in a single alter
        (
            "STRUCT<id INT, name STRING, revenue FLOAT, total INT>",
            "STRUCT<id INT, name STRING, revenue INT, total FLOAT>",
            [
                TableAlterOperation.alter_type(
                    TableAlterColumn.primitive("total"),
                    "FLOAT",
                    current_type="INT",
                    # Note that the resulting table struct will not match what we defined as the desired
                    # result since it could be coerced
                    expected_table_struct="STRUCT<id INT, name STRING, revenue FLOAT, total FLOAT>",
                )
            ],
            dict(
                support_positional_add=False,
                support_nested_operations=True,
                support_coercing_compatible_types=True,
                compatible_types={
                    exp.DataType.build("INT"): {exp.DataType.build("FLOAT")},
                },
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

    alter_expressions = engine_adapter.get_alter_expressions("apply_to_table", "schema_from_table")
    engine_adapter.alter_table(alter_expressions)
    assert engine_adapter.columns("apply_to_table") == {
        "id": exp.DataType.build("int"),
        "ds": exp.DataType.build("text"),
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


def test_get_schema_differ():
    # Test that known dialects return SchemaDiffer instances
    for dialect in ["bigquery", "snowflake", "postgres", "databricks", "spark", "duckdb"]:
        schema_differ = get_schema_differ(dialect)
        assert isinstance(schema_differ, SchemaDiffer)

    # Test specific configurations
    # Databricks should support positional add and nested operations
    databricks_differ = get_schema_differ("databricks")
    assert databricks_differ.support_positional_add is True
    assert databricks_differ.support_nested_operations is True
    assert databricks_differ.support_nested_drop is True

    # BigQuery should have specific compatible types configured
    bigquery_differ = get_schema_differ("bigquery")
    assert len(bigquery_differ.compatible_types) > 0
    assert bigquery_differ.support_coercing_compatible_types is True

    # Snowflake should have parameterized type defaults
    snowflake_differ = get_schema_differ("snowflake")
    assert len(snowflake_differ.parameterized_type_defaults) > 0

    # Postgres should support drop cascade
    postgres_differ = get_schema_differ("postgres")
    assert postgres_differ.drop_cascade is True
    assert len(postgres_differ.types_with_unlimited_length) > 0

    # Test dialect aliases work correctly
    schema_differ_pg = get_schema_differ("postgresql")
    schema_differ_postgres = get_schema_differ("postgres")
    assert schema_differ_pg.drop_cascade == schema_differ_postgres.drop_cascade

    # Test unknown dialect returns default SchemaDiffer
    schema_differ_unknown = get_schema_differ("unknown_dialect")
    assert isinstance(schema_differ_unknown, SchemaDiffer)
    assert schema_differ_unknown.support_positional_add is False
    assert schema_differ_unknown.support_nested_operations is False

    # Test case insensitivity
    schema_differ_upper = get_schema_differ("BIGQUERY")
    schema_differ_lower = get_schema_differ("bigquery")
    assert (
        schema_differ_upper.support_coercing_compatible_types
        == schema_differ_lower.support_coercing_compatible_types
    )


def test_destructive_add_operation_metadata():
    """Test that ADD operations part of destructive changes are properly marked with metadata."""
    # Test scenario: Type change that requires DROP + ADD
    current_schema = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
        "value": exp.DataType.build("INT"),
    }
    new_schema = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
        "value": exp.DataType.build("STRING"),  # Type change requiring DROP+ADD
    }

    differ = SchemaDiffer()
    alter_expressions = differ.compare_columns("test_table", current_schema, new_schema)

    # Should generate DROP value + ADD value
    assert len(alter_expressions) == 2

    drop_expr = alter_expressions[0]
    add_expr = alter_expressions[1]

    # Verify we have the expected operations
    assert "DROP COLUMN value" in drop_expr.sql()
    assert "ADD COLUMN value" in add_expr.sql()  # SQLGlot might use TEXT instead of STRING

    # The ADD operation should be marked as destructive support via metadata
    add_actions = add_expr.args.get("actions", [])
    assert len(add_actions) == 1
    add_action = add_actions[0]

    # Check that the ADD action has metadata marking it as destructive
    assert add_action.meta.get("sqlmesh_destructive") is True


def test_pure_additive_add_operation_no_metadata():
    """Test that pure ADD operations (not part of destructive changes) have no destructive metadata."""
    current_schema = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
    }
    new_schema = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
        "email": exp.DataType.build("STRING"),  # Pure addition
        "created_at": exp.DataType.build("TIMESTAMP"),  # Pure addition
    }

    differ = SchemaDiffer()
    alter_expressions = differ.compare_columns("test_table", current_schema, new_schema)

    # Should generate ADD email + ADD created_at
    assert len(alter_expressions) == 2

    for add_expr in alter_expressions:
        assert "ADD COLUMN" in add_expr.sql()

        add_actions = add_expr.args.get("actions", [])
        assert len(add_actions) == 1
        add_action = add_actions[0]

        # Pure ADD operations should NOT have destructive metadata
        assert (
            add_action.meta.get("sqlmesh_destructive") is None
            or add_action.meta.get("sqlmesh_destructive") is False
        )


def test_filtering_with_destructive_metadata():
    """Test that filtering functions respect destructive metadata on ADD operations."""
    from sqlmesh.core.schema_diff import (
        filter_additive_changes,
        filter_destructive_changes,
        has_additive_changes,
        get_additive_changes,
    )

    # Scenario: Type change (destructive) + pure addition (additive)
    current_schema = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
    }
    new_schema = {
        "id": exp.DataType.build("BIGINT"),  # Type change -> DROP + ADD (both destructive)
        "name": exp.DataType.build("STRING"),
        "email": exp.DataType.build("STRING"),  # Pure addition (additive)
    }

    differ = SchemaDiffer()
    all_expressions = differ.compare_columns("test_table", current_schema, new_schema)

    # Should have: DROP id + ADD id (with metadata) + ADD email (without metadata)
    assert len(all_expressions) == 3

    # Test filter_additive_changes - should remove the pure ADD operations
    non_additive = filter_additive_changes(all_expressions)
    # Should keep: DROP id + ADD id (marked destructive)
    # Should remove: ADD email (pure additive)
    assert len(non_additive) == 2
    for expr in non_additive:
        sql = expr.sql()
        # Should not contain the pure additive change
        assert "ADD COLUMN email" not in sql

    # Test filter_destructive_changes - should remove DROP and marked ADD operations
    non_destructive = filter_destructive_changes(all_expressions)
    # Should keep: ADD email (pure additive)
    # Should remove: DROP id + ADD id (destructive)
    assert len(non_destructive) == 1
    assert "ADD COLUMN email" in non_destructive[0].sql()

    # Test has_additive_changes - should only count pure additive changes
    assert has_additive_changes(all_expressions) is True  # Has pure ADD email

    # Test get_additive_changes - should only return pure additive changes
    additive_changes = get_additive_changes(all_expressions)
    assert len(additive_changes) == 1
    assert "ADD COLUMN email" in additive_changes[0].sql()

    # Test is_only_additive_changes - should return False due to destructive changes
    assert is_only_additive_changes(all_expressions) is False


def test_complex_destructive_scenario_metadata():
    """Test complex scenario with multiple destructive changes and metadata propagation."""
    current_schema = {
        "id": exp.DataType.build("INT"),
        "value": exp.DataType.build("STRING"),
        "old_col": exp.DataType.build("DECIMAL(10,2)"),
        "unchanged": exp.DataType.build("STRING"),
    }
    new_schema = {
        "id": exp.DataType.build("BIGINT"),  # Type change requiring DROP+ADD
        "value": exp.DataType.build("INT"),  # Type change requiring DROP+ADD
        "old_col": exp.DataType.build("DECIMAL(5,1)"),  # Precision decrease requiring DROP+ADD
        "unchanged": exp.DataType.build("STRING"),
        "pure_new": exp.DataType.build("INT"),  # Pure addition
    }

    differ = SchemaDiffer()
    alter_expressions = differ.compare_columns("test_table", current_schema, new_schema)

    # Expected operations based on real behavior:
    # 1. DROP id → ADD id BIGINT (destructive pair)
    # 2. DROP value → ADD value INT (destructive pair)
    # 3. DROP old_col → ADD old_col DECIMAL(5,1) (destructive pair)
    # 4. ADD pure_new INT (additive)

    # Count ADD operations with destructive metadata
    destructive_add_count = 0
    pure_add_count = 0

    for expr in alter_expressions:
        if "ADD" in expr.sql():
            actions = expr.args.get("actions", [])
            if actions and actions[0].meta.get("sqlmesh_destructive"):
                destructive_add_count += 1
            else:
                pure_add_count += 1

    # Should have 3 destructive ADD operations (id, value, old_col) and 1 pure ADD (pure_new)
    assert destructive_add_count == 3
    assert pure_add_count == 1


def test_integration_destructive_add_filtering():
    """Integration test demonstrating that the fix works in realistic scenarios."""
    from sqlmesh.core.schema_diff import filter_destructive_changes, filter_additive_changes

    # Scenario: User has a model with type changes + pure additions
    # When using on_destructive_change=IGNORE, they should only see pure additions
    current_schema = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("STRING"),
    }
    new_schema = {
        "id": exp.DataType.build(
            "BIGINT"
        ),  # Type change -> generates DROP id + ADD id (with destructive flag)
        "name": exp.DataType.build("STRING"),
        "email": exp.DataType.build(
            "STRING"
        ),  # Pure addition -> generates ADD email (no destructive flag)
        "age": exp.DataType.build(
            "INT"
        ),  # Pure addition -> generates ADD age (no destructive flag)
    }

    differ = SchemaDiffer()
    all_changes = differ.compare_columns("test_table", current_schema, new_schema)

    # Should have 4 operations: DROP id + ADD id (destructive) + ADD email + ADD age (additive)
    assert len(all_changes) == 4

    # When user sets on_destructive_change=IGNORE, they should filter out destructive changes
    non_destructive_changes = filter_destructive_changes(all_changes)

    # Should only have the 2 pure ADD operations (email, age)
    assert len(non_destructive_changes) == 2
    for expr in non_destructive_changes:
        assert "ADD COLUMN" in expr.sql()
        # Verify these are pure additions, not part of destructive changes
        actions = expr.args.get("actions", [])
        assert len(actions) == 1
        add_action = actions[0]
        assert not add_action.meta.get("sqlmesh_destructive", False)

    # When user sets on_additive_change=IGNORE, they should filter out additive changes
    non_additive_changes = filter_additive_changes(all_changes)

    # Should only have the 2 destructive operations (DROP id + ADD id with metadata)
    assert len(non_additive_changes) == 2
    drop_found = False
    add_found = False
    for expr in non_additive_changes:
        if "DROP COLUMN" in expr.sql():
            drop_found = True
        elif "ADD COLUMN" in expr.sql():
            add_found = True
            # Verify this ADD operation has destructive metadata
            actions = expr.args.get("actions", [])
            assert len(actions) == 1
            add_action = actions[0]
            assert add_action.meta.get("sqlmesh_destructive") is True

    assert drop_found and add_found


def test_filter_additive_changes_removes_only_additive_expressions():
    """Test that filter_additive_changes removes only additive alter expressions."""
    # Create mixed alter expressions (both additive and destructive)
    current_schema = {"id": exp.DataType.build("INT"), "name": exp.DataType.build("STRING")}
    new_schema = {
        "id": exp.DataType.build("INT"),  # no change
        "new_col": exp.DataType.build("STRING"),  # additive - should be removed
    }

    differ = SchemaDiffer()
    alter_expressions = differ.compare_columns("test_table", current_schema, new_schema)

    # Should have additive changes initially
    assert has_additive_changes(alter_expressions)

    # After filtering, should have no additive changes
    filtered_expressions = filter_additive_changes(alter_expressions)
    assert not has_additive_changes(filtered_expressions)

    # Original expressions should still have additive changes (not mutated)
    assert has_additive_changes(alter_expressions)


def test_filter_destructive_changes_removes_only_destructive_expressions():
    """Test that filter_destructive_changes removes only destructive alter expressions."""
    # Create mixed alter expressions (both additive and destructive)
    current_schema = {"id": exp.DataType.build("INT"), "name": exp.DataType.build("STRING")}
    new_schema = {
        "id": exp.DataType.build("INT"),  # no change
        "name": exp.DataType.build("STRING"),  # no change
        "new_col": exp.DataType.build("STRING"),  # additive - should remain
    }

    differ = SchemaDiffer()
    alter_expressions = differ.compare_columns("test_table", current_schema, new_schema)

    # Should have no destructive changes initially (only additive)
    assert not has_drop_alteration(alter_expressions)

    # After filtering destructive changes, should still have the same expressions
    filtered_expressions = filter_destructive_changes(alter_expressions)
    assert len(filtered_expressions) == len(alter_expressions)

    # Now test with actual destructive changes
    destructive_new_schema = {"id": exp.DataType.build("INT")}  # removes name column
    destructive_expressions = differ.compare_columns(
        "test_table", current_schema, destructive_new_schema
    )

    # Should have destructive changes
    assert has_drop_alteration(destructive_expressions)

    # After filtering, should have no destructive changes
    filtered_destructive = filter_destructive_changes(destructive_expressions)
    assert not has_drop_alteration(filtered_destructive)


def test_filter_mixed_changes_correctly():
    """Test filtering when there are both additive and destructive changes."""
    # Schema change that both adds and removes columns
    current_schema = {
        "id": exp.DataType.build("INT"),
        "old_col": exp.DataType.build("STRING"),  # will be removed (destructive)
    }
    new_schema = {
        "id": exp.DataType.build("INT"),  # unchanged
        "new_col": exp.DataType.build("STRING"),  # will be added (additive)
    }

    differ = SchemaDiffer()
    alter_expressions = differ.compare_columns("test_table", current_schema, new_schema)

    # Should have both types of changes
    assert has_additive_changes(alter_expressions)
    assert has_drop_alteration(alter_expressions)

    # Filter out additive changes - should only have destructive
    no_additive = filter_additive_changes(alter_expressions)
    assert not has_additive_changes(no_additive)
    assert has_drop_alteration(no_additive)

    # Filter out destructive changes - should only have additive
    no_destructive = filter_destructive_changes(alter_expressions)
    assert has_additive_changes(no_destructive)
    assert not has_drop_alteration(no_destructive)

    # Filter out both - should have no changes
    no_changes = filter_destructive_changes(filter_additive_changes(alter_expressions))
    assert not has_additive_changes(no_changes)
    assert not has_drop_alteration(no_changes)
    assert len(no_changes) == 0


def test_alter_column_integration_with_schema_differ():
    """Test that ALTER COLUMN operations work correctly with the SchemaDiffer."""
    # Test additive ALTER COLUMN operations
    current_schema = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("VARCHAR(50)"),
        "value": exp.DataType.build("DECIMAL(10,2)"),
    }
    additive_schema = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("VARCHAR(100)"),  # Increase precision - additive
        "value": exp.DataType.build("DECIMAL(15,5)"),  # Increase precision/scale - additive
    }

    # Use a SchemaDiffer that supports compatible type changes
    differ = SchemaDiffer(
        compatible_types={
            exp.DataType.build("VARCHAR(50)"): {exp.DataType.build("VARCHAR(100)")},
            exp.DataType.build("DECIMAL(10,2)"): {exp.DataType.build("DECIMAL(15,5)")},
        }
    )

    alter_expressions = differ.compare_columns("test_table", current_schema, additive_schema)

    # Should generate ALTER COLUMN operations
    assert len(alter_expressions) == 2
    for expr in alter_expressions:
        assert "ALTER COLUMN" in expr.sql()

    # These should be classified as additive
    from sqlmesh.core.schema_diff import has_additive_changes

    assert has_additive_changes(alter_expressions)
    assert is_only_additive_changes(alter_expressions)


def test_alter_column_destructive_integration():
    """Test that destructive ALTER COLUMN operations are correctly handled."""
    current_schema = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("VARCHAR(100)"),
        "value": exp.DataType.build("DECIMAL(15,5)"),
    }
    destructive_schema = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("VARCHAR(50)"),  # Decrease precision - destructive
        "value": exp.DataType.build("DECIMAL(10,2)"),  # Decrease precision/scale - destructive
    }

    differ = SchemaDiffer()
    alter_expressions = differ.compare_columns("test_table", current_schema, destructive_schema)

    # Should generate DROP + ADD operations (destructive changes)
    # because the type changes are not compatible
    assert len(alter_expressions) >= 2

    from sqlmesh.core.schema_diff import has_additive_changes, has_drop_alteration

    assert not has_additive_changes(alter_expressions)
    assert has_drop_alteration(alter_expressions)


def test_alter_column_mixed_additive_destructive():
    """Test mixed scenario with both additive and destructive ALTER COLUMN operations."""
    current_schema = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("VARCHAR(50)"),
        "description": exp.DataType.build("VARCHAR(100)"),
        "value": exp.DataType.build("DECIMAL(10,2)"),
    }
    mixed_schema = {
        "id": exp.DataType.build("INT"),
        "name": exp.DataType.build("VARCHAR(100)"),  # Increase precision - additive
        "description": exp.DataType.build("VARCHAR(50)"),  # Decrease precision - destructive
        "value": exp.DataType.build("DECIMAL(15,5)"),  # Increase precision - additive
    }

    differ = SchemaDiffer(
        compatible_types={
            # Only define compatible types for additive changes
            exp.DataType.build("VARCHAR(50)"): {exp.DataType.build("VARCHAR(100)")},
            exp.DataType.build("DECIMAL(10,2)"): {exp.DataType.build("DECIMAL(15,5)")},
        }
    )

    alter_expressions = differ.compare_columns("test_table", current_schema, mixed_schema)

    from sqlmesh.core.schema_diff import (
        has_additive_changes,
        has_drop_alteration,
        filter_destructive_changes,
        get_additive_changes,
    )

    # Should have both additive and destructive changes
    assert has_additive_changes(alter_expressions)
    assert has_drop_alteration(alter_expressions)  # From destructive description change

    # Test filtering
    additive_only = get_additive_changes(alter_expressions)
    assert len(additive_only) >= 1  # Should have additive ALTER COLUMN operations

    non_destructive = filter_destructive_changes(alter_expressions)
    assert has_additive_changes(non_destructive)
    assert not has_drop_alteration(non_destructive)


def test_alter_column_with_default_values():
    """Test ALTER COLUMN operations involving default values."""
    # This test is more conceptual since current SQLMesh schema diff
    # doesn't directly handle DEFAULT value changes, but the framework should support it
    current_schema = {
        "id": exp.DataType.build("INT"),
        "status": exp.DataType.build("VARCHAR(50)"),
    }
    # In practice, default value changes would need to be detected at a higher level
    # and passed down with appropriate metadata
    schema_with_default = {
        "id": exp.DataType.build("INT"),
        "status": exp.DataType.build("VARCHAR(50)"),  # Same type, but with default (additive)
    }

    differ = SchemaDiffer()
    alter_expressions = differ.compare_columns("test_table", current_schema, schema_with_default)

    # No schema changes detected since types are the same
    assert len(alter_expressions) == 0
