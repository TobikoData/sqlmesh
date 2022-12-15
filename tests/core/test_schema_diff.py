import typing as t
from unittest.mock import call

from pytest_mock.plugin import MockerFixture

from sqlmesh.core.schema_diff import SchemaDelta, SchemaDiffCalculator


def test_schema_diff_calculate(mocker: MockerFixture):
    source_table_name = "source_table"
    target_table_name = "target_table"

    def table_schemas(table_name: str) -> t.List[t.Tuple[str, str]]:
        if table_name == "source_table":
            return [
                ("id", "int"),
                ("name", "string"),
                ("price", "double"),
                ("ds", "string"),
                ("# Partition Information", ""),
                ("# col_name", "data_type"),
                ("ds", "string"),
            ]
        else:
            return [
                ("name", "int"),
                ("id", "int"),
                ("ds", "string"),
                ("new_column", "double"),
                ("# Partition Information", ""),
                ("# col_name", "data_type"),
                ("ds", "string"),
            ]

    engine_adapter_mock = mocker.Mock()
    engine_adapter_mock.describe_table.side_effect = table_schemas

    calculator = SchemaDiffCalculator(engine_adapter_mock)
    assert calculator.calculate(source_table_name, target_table_name) == [
        SchemaDelta.drop("name", "STRING"),
        SchemaDelta.add("name", "INT"),
        SchemaDelta.drop("price", "DOUBLE"),
        SchemaDelta.add("new_column", "DOUBLE"),
    ]

    engine_adapter_mock.describe_table.assert_has_calls(
        [call(source_table_name), call(target_table_name)]
    )


def test_schema_diff_calculate_type_transitions(mocker: MockerFixture):
    source_table_name = "source_table"
    target_table_name = "target_table"

    def table_schemas(table_name: str) -> t.List[t.Tuple[str, str]]:
        if table_name == "source_table":
            return [
                ("id", "int"),
                ("ds", "string"),
            ]
        else:
            return [
                ("id", "bigint"),
                ("ds", "int"),
            ]

    engine_adapter_mock = mocker.Mock()
    engine_adapter_mock.describe_table.side_effect = table_schemas

    def is_type_transition_allowed(src: str, tgt: str) -> bool:
        return src == "INT" and tgt == "BIGINT"

    calculator = SchemaDiffCalculator(engine_adapter_mock, is_type_transition_allowed)
    assert calculator.calculate(source_table_name, target_table_name) == [
        SchemaDelta.alter_type("id", "BIGINT"),
        SchemaDelta.drop("ds", "STRING"),
        SchemaDelta.add("ds", "INT"),
    ]

    engine_adapter_mock.describe_table.assert_has_calls(
        [call(source_table_name), call(target_table_name)]
    )
