# type: ignore
from unittest.mock import call

from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import SparkEngineAdapter
from sqlmesh.core.schema_diff import SchemaDelta


def test_create_table_properties(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
    }

    adapter = SparkEngineAdapter(lambda: connection_mock)
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

    adapter = SparkEngineAdapter(lambda: connection_mock)

    adapter.alter_table(
        "test_table",
        operations=[
            SchemaDelta.drop("c", "STRUCT<d: INT, e: INT, f: INT>"),
            SchemaDelta.drop("d", "STRUCT<e: INT, f: INT>"),
            SchemaDelta.add("a", "INT", "STRUCT<e: INT, f: INT, a: INT>"),
            SchemaDelta.add("b", "STRING", "STRUCT<e: INT, f: INT, a: INT, b: STRING>"),
        ],
    )

    adapter.alter_table(
        "test_table",
        operations=[
            SchemaDelta.add("e", "DOUBLE", "STRUCT<e: INT, f: INT, a: INT, b: STRING, e: DOUBLE>")
        ],
    )

    adapter.alter_table(
        "test_table",
        operations=[SchemaDelta.drop("f", "STRUCT<e: INT, a: INT, b: STRING, e: DOUBLE>")],
    )

    cursor_mock.execute.assert_has_calls(
        [
            # 1st call.
            call("""ALTER TABLE `test_table` DROP COLUMN `c`"""),
            call("""ALTER TABLE `test_table` DROP COLUMN `d`"""),
            call("""ALTER TABLE `test_table` ADD COLUMN `a` INT"""),
            call("""ALTER TABLE `test_table` ADD COLUMN `b` STRING"""),
            # 2nd call.
            call("""ALTER TABLE `test_table` ADD COLUMN `e` DOUBLE"""),
            # 3d call.
            call("""ALTER TABLE `test_table` DROP COLUMN `f`"""),
        ]
    )


def test_replace_query(mocker: MockerFixture):
    connection_mock = mocker.NonCallableMock()
    cursor_mock = mocker.Mock()
    connection_mock.cursor.return_value = cursor_mock

    adapter = SparkEngineAdapter(lambda: connection_mock, "spark")
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    cursor_mock.execute.assert_called_once_with(
        "INSERT OVERWRITE TABLE `test_table` (`a`) SELECT `a` FROM `tbl`"
    )
