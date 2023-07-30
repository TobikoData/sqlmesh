# type: ignore
import typing as t
from unittest.mock import call

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import SparkEngineAdapter
from sqlmesh.utils.errors import SQLMeshError


def test_create_table_properties(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(SparkEngineAdapter)

    columns_to_types = {
        "cola": exp.DataType.build("INT"),
        "colb": exp.DataType.build("TEXT"),
        "colc": exp.DataType.build("TEXT"),
    }
    adapter.create_table(
        "test_table",
        columns_to_types,
        partitioned_by=[exp.to_column("colb")],
        clustered_by=["colc"],
        storage_format="ICEBERG",
    )

    adapter.cursor.execute.assert_called_once_with(
        "CREATE TABLE IF NOT EXISTS `test_table` (`cola` INT, `colb` STRING, `colc` STRING) USING ICEBERG PARTITIONED BY (`colb`)"
    )

    adapter.cursor.reset_mock()
    adapter.create_table(
        "test_table",
        columns_to_types,
        partitioned_by=[exp.to_column("cola"), exp.to_column("colb")],
        storage_format="ICEBERG",
    )

    adapter.cursor.execute.assert_called_once_with(
        "CREATE TABLE IF NOT EXISTS `test_table` (`cola` INT, `colb` STRING, `colc` STRING) USING ICEBERG PARTITIONED BY (`cola`, `colb`)"
    )

    with pytest.raises(SQLMeshError):
        adapter.create_table(
            "test_table",
            columns_to_types,
            partitioned_by=[parse_one("DATE(cola)")],
            storage_format="ICEBERG",
        )


def test_alter_table(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(SparkEngineAdapter)
    current_table_name = "test_table"
    target_table_name = "test_table__1"

    def table_columns(table_name: str) -> t.Dict[str, exp.DataType]:
        if table_name == current_table_name:
            return {
                "id": exp.DataType.build("INT"),
                "a": exp.DataType.build("INT"),
                "b": exp.DataType.build("STRING"),
                "complex": exp.DataType.build("STRUCT<complex_a: INT, complex_b: STRING>"),
                "ds": exp.DataType.build("STRING"),
            }
        else:
            return {
                "id": exp.DataType.build("BIGINT"),
                "a": exp.DataType.build("STRING"),
                "complex": exp.DataType.build("STRUCT<complex_a: INT>"),
                "ds": exp.DataType.build("INT"),
            }

    adapter.columns = table_columns

    adapter.alter_table(current_table_name, target_table_name)

    adapter.cursor.execute.assert_has_calls(
        [
            call("""ALTER TABLE `test_table` DROP COLUMN `b`"""),
            call("""ALTER TABLE `test_table` DROP COLUMN `id`"""),
            call("""ALTER TABLE `test_table` ADD COLUMN `id` BIGINT"""),
            call("""ALTER TABLE `test_table` DROP COLUMN `a`"""),
            call("""ALTER TABLE `test_table` ADD COLUMN `a` STRING"""),
            call("""ALTER TABLE `test_table` DROP COLUMN `complex`"""),
            call("""ALTER TABLE `test_table` ADD COLUMN `complex` STRUCT<`complex_a`: INT>"""),
            call("""ALTER TABLE `test_table` DROP COLUMN `ds`"""),
            call("""ALTER TABLE `test_table` ADD COLUMN `ds` INT"""),
        ]
    )


def test_replace_query(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(SparkEngineAdapter)
    adapter.replace_query("test_table", parse_one("SELECT a FROM tbl"), {"a": "int"})

    adapter.cursor.execute.assert_called_once_with(
        "INSERT OVERWRITE TABLE `test_table` (`a`) SELECT * FROM (SELECT `a` FROM `tbl`) AS `_subquery` WHERE 1 = 1"
    )


def test_replace_query_pandas(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(SparkEngineAdapter)
    mocker.patch("sqlmesh.core.engine_adapter.spark.SparkEngineAdapter._use_spark_session", False)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query("test_table", df, {"a": "int", "b": "int"})

    adapter.cursor.execute.assert_called_once_with(
        "INSERT OVERWRITE TABLE `test_table` (`a`, `b`) SELECT * FROM (SELECT CAST(`a` AS INT) AS `a`, CAST(`b` AS INT) AS `b` FROM VALUES (1, 4), (2, 5), (3, 6) AS `test_table`(`a`, `b`)) AS `_subquery` WHERE 1 = 1"
    )


def test_create_table_table_options(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(SparkEngineAdapter)

    adapter.create_table(
        "test_table",
        {"a": "int", "b": "int"},
        table_properties={
            "test.conf.key": exp.convert("value"),
        },
    )

    adapter.cursor.execute.assert_called_once_with(
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` int, `b` int) TBLPROPERTIES ('test.conf.key'='value')"
    )


def test_create_state_table(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(SparkEngineAdapter)

    adapter.create_state_table("test_table", {"a": "int", "b": "int"}, primary_key=["a"])

    adapter.cursor.execute.assert_called_once_with(
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` int, `b` int) PARTITIONED BY (`a`)"
    )
