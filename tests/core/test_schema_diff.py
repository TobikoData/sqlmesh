import typing as t
from unittest.mock import call

from pytest_mock.plugin import MockerFixture
from sqlglot import exp

from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.schema_diff import SchemaDelta, SchemaDiffCalculator


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

    calculator = SchemaDiffCalculator(engine_adapter_mock)
    assert calculator.calculate(apply_to_table_name, schema_from_table_name) == [
        SchemaDelta.drop("name", "STRING"),
        SchemaDelta.add("name", "INT"),
        SchemaDelta.drop("price", "DOUBLE"),
        SchemaDelta.add("new_column", "DOUBLE"),
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

    def is_type_transition_allowed(src: str, tgt: str) -> bool:
        return src == "INT" and tgt == "BIGINT"

    calculator = SchemaDiffCalculator(engine_adapter_mock, is_type_transition_allowed)
    assert calculator.calculate(apply_to_table_name, schema_from_table_name) == [
        SchemaDelta.alter_type("id", "BIGINT"),
        SchemaDelta.drop("ds", "STRING"),
        SchemaDelta.add("ds", "INT"),
    ]

    engine_adapter_mock.columns.assert_has_calls(
        [call(apply_to_table_name), call(schema_from_table_name)]
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

    calculator = SchemaDiffCalculator(engine_adapter)
    assert calculator.calculate("apply_to_table", "schema_from_table") == [
        SchemaDelta.drop("name", "VARCHAR"),
        SchemaDelta.add("name", "INTEGER"),
        SchemaDelta.drop("price", "DOUBLE"),
        SchemaDelta.add("new_column", "DOUBLE"),
    ]
