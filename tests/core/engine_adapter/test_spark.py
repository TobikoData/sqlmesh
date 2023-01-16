from unittest.mock import call

from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp

from sqlmesh.core.engine_adapter import SparkEngineAdapter


def test_create_table_properties(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }

    adapter = SparkEngineAdapter(lambda: connection_mock)  # type: ignore
    adapter.create_table(
        "test_table",
        columns_to_types,
        partitioned_by=["colb"],
        storage_format="ICEBERG",
    )

    cursor_mock.execute.assert_called_once_with(
        "CREATE TABLE IF NOT EXISTS `test_table` (`cola` INT, `colb` STRING) USING ICEBERG PARTITIONED BY (`colb`)"
    )


def test_alter_table(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = SparkEngineAdapter(lambda: connection_mock)  # type: ignore

    adapter.alter_table(
        "test_table",
        {"a": "INT", "b": "STRING"},
        ["c", "d"],
    )

    adapter.alter_table(
        "test_table",
        {"e": "DOUBLE"},
        [],
    )

    adapter.alter_table(
        "test_table",
        {},
        ["f"],
    )

    cursor_mock.execute.assert_has_calls(
        [
            # 1st call.
            call("""ALTER TABLE `test_table` ADD COLUMNS (`a` INT, `b` STRING)"""),
            call("""ALTER TABLE `test_table` DROP COLUMNS (`c`, `d`)"""),
            # 2nd call.
            call("""ALTER TABLE `test_table` ADD COLUMNS (`e` DOUBLE)"""),
            # 3d call.
            call("""ALTER TABLE `test_table` DROP COLUMNS (`f`)"""),
        ]
    )
