# type: ignore
import typing as t
from unittest.mock import call

import pandas as pd
import pytest
from pyspark.sql import types as spark_types
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter import SparkEngineAdapter
from sqlmesh.utils.errors import SQLMeshError
from tests.core.engine_adapter import to_sql_calls


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


def test_create_view_properties(make_mocked_engine_adapter: t.Callable):
    adapter = make_mocked_engine_adapter(SparkEngineAdapter)

    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"), table_properties={"a": exp.convert(1)})  # type: ignore
    adapter.cursor.execute.assert_called_once_with(
        "CREATE OR REPLACE VIEW `test_view` TBLPROPERTIES ('a'=1) AS SELECT `a` FROM `tbl`"
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

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` int)",
        "INSERT OVERWRITE TABLE `test_table` (`a`) SELECT `a` FROM (SELECT `a` FROM `tbl`) AS `_subquery` WHERE TRUE",
    ]


def test_replace_query_pandas(make_mocked_engine_adapter: t.Callable, mocker: MockerFixture):
    adapter = make_mocked_engine_adapter(SparkEngineAdapter)
    mocker.patch("sqlmesh.core.engine_adapter.spark.SparkEngineAdapter._use_spark_session", False)
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    adapter.replace_query(
        "test_table", df, {"a": exp.DataType.build("int"), "b": exp.DataType.build("int")}
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT, `b` INT)",
        "INSERT OVERWRITE TABLE `test_table` (`a`, `b`) SELECT `a`, `b` FROM (SELECT CAST(`a` AS INT) AS `a`, CAST(`b` AS INT) AS `b` FROM VALUES (1, 4), (2, 5), (3, 6) AS `t`(`a`, `b`)) AS `_subquery` WHERE TRUE",
    ]


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


test_primitive_params = [
    ("byte", spark_types.ByteType()),
    ("short", spark_types.ShortType()),
    ("int", spark_types.IntegerType()),
    ("bigint", spark_types.LongType()),
    ("float", spark_types.FloatType()),
    ("double", spark_types.DoubleType()),
    ("decimal", spark_types.DecimalType()),
    ("text", spark_types.StringType()),
    # Spark supports VARCHAR and CHAR but SQLGlot currently converts them to strings
    ("varchar(25)", spark_types.StringType()),
    ("char(30)", spark_types.StringType()),
    ("binary", spark_types.BinaryType()),
    ("boolean", spark_types.BooleanType()),
    ("date", spark_types.DateType()),
    ("datetime", spark_types.TimestampNTZType()),
    ("timestamp", spark_types.TimestampType()),
]


@pytest.mark.parametrize(
    "type_name, spark_type",
    test_primitive_params,
    ids=[x[0] for x in test_primitive_params],
)
def test_col_to_types_to_spark_schema_primitives(type_name, spark_type):
    from pyspark.sql import types as spark_types

    assert SparkEngineAdapter.sqlglot_to_spark_types(
        {f"col_{type_name}": exp.DataType.build(type_name, dialect="spark")}
    ) == spark_types.StructType([spark_types.StructField(f"col_{type_name}", spark_type)])


test_complex_params = [
    (
        "map",
        "map<int, string>",
        spark_types.MapType(spark_types.IntegerType(), spark_types.StringType()),
    ),
    (
        "array_primitives",
        "array<int>",
        spark_types.ArrayType(spark_types.IntegerType(), True),
    ),
    (
        "array_struct",
        "array<struct<cola:int,colb:array<string>>>",
        spark_types.ArrayType(
            spark_types.StructType(
                [
                    spark_types.StructField("cola", spark_types.IntegerType()),
                    spark_types.StructField(
                        "colb", spark_types.ArrayType(spark_types.StringType(), True)
                    ),
                ]
            ),
            True,
        ),
    ),
    (
        "struct_primitives",
        "struct<col1: int, col2: string>",
        spark_types.StructType(
            [
                spark_types.StructField("col1", spark_types.IntegerType()),
                spark_types.StructField("col2", spark_types.StringType()),
            ]
        ),
    ),
    (
        "struct_of_arrays",
        "struct<col1: array<int>, col2: array<string>>",
        spark_types.StructType(
            [
                spark_types.StructField(
                    "col1", spark_types.ArrayType(spark_types.IntegerType(), True)
                ),
                spark_types.StructField(
                    "col2", spark_types.ArrayType(spark_types.StringType(), True)
                ),
            ]
        ),
    ),
    (
        "struct_and_array",
        "struct<col1: array<struct<cola:int,colb:array<string>>>, col2: struct<col1: int, col2: string>>",
        spark_types.StructType(
            [
                spark_types.StructField(
                    "col1",
                    spark_types.ArrayType(
                        spark_types.StructType(
                            [
                                spark_types.StructField("cola", spark_types.IntegerType()),
                                spark_types.StructField(
                                    "colb",
                                    spark_types.ArrayType(spark_types.StringType(), True),
                                ),
                            ]
                        ),
                        True,
                    ),
                ),
                spark_types.StructField(
                    "col2",
                    spark_types.StructType(
                        [
                            spark_types.StructField("col1", spark_types.IntegerType()),
                            spark_types.StructField("col2", spark_types.StringType()),
                        ]
                    ),
                ),
            ]
        ),
    ),
    (
        "array_of_maps",
        "array<map<int, string>>",
        spark_types.ArrayType(
            spark_types.MapType(spark_types.IntegerType(), spark_types.StringType()), True
        ),
    ),
    (
        "struct_with_struct",
        "struct<col1: struct<cola:int,colb:array<string>>, col2: struct<col1: int, col2: string>>",
        spark_types.StructType(
            [
                spark_types.StructField(
                    "col1",
                    spark_types.StructType(
                        [
                            spark_types.StructField("cola", spark_types.IntegerType()),
                            spark_types.StructField(
                                "colb",
                                spark_types.ArrayType(spark_types.StringType(), True),
                            ),
                        ]
                    ),
                ),
                spark_types.StructField(
                    "col2",
                    spark_types.StructType(
                        [
                            spark_types.StructField("col1", spark_types.IntegerType()),
                            spark_types.StructField("col2", spark_types.StringType()),
                        ]
                    ),
                ),
            ]
        ),
    ),
]


@pytest.mark.parametrize(
    "type_name, spark_type",
    [x[1:] for x in test_complex_params],
    ids=[x[0] for x in test_complex_params],
)
def test_col_to_types_to_spark_schema_complex(type_name, spark_type):
    actual = SparkEngineAdapter.sqlglot_to_spark_types(
        {f"col_{type_name}": exp.DataType.build(type_name, dialect="spark")}
    )
    expected = spark_types.StructType([spark_types.StructField(f"col_{type_name}", spark_type)])
    assert actual == expected


@pytest.mark.parametrize(
    "type_name, spark_type",
    test_primitive_params,
    ids=[x[0] for x in test_primitive_params],
)
def test_spark_struct_primitives_to_col_to_types(type_name, spark_type):
    actual = SparkEngineAdapter.spark_to_sqlglot_types(
        spark_types.StructType([spark_types.StructField(f"col_{type_name}", spark_type)])
    )
    expected = {f"col_{type_name}": exp.DataType.build(type_name, dialect="spark")}
    assert actual == expected


@pytest.mark.parametrize(
    "type_name, spark_type",
    [x[1:] for x in test_complex_params],
    ids=[x[0] for x in test_complex_params],
)
def test_spark_struct_complex_to_col_to_types(type_name, spark_type):
    actual = SparkEngineAdapter.spark_to_sqlglot_types(
        spark_types.StructType([spark_types.StructField(f"col_{type_name}", spark_type)])
    )
    expected = {f"col_{type_name}": exp.DataType.build(type_name, dialect="spark")}
    assert actual == expected
