import pytest
from pyspark.sql import SparkSession

from sqlmesh.engines.spark.db_api import errors
from sqlmesh.engines.spark.db_api import spark_session as spark_session_db

pytestmark = [
    pytest.mark.slow,
    pytest.mark.spark_pyspark,
]


def test_spark_session_cursor(spark_session: SparkSession):
    connection = spark_session_db.connection(spark_session)
    cursor = connection.cursor()

    with pytest.raises(errors.ProgrammingError):
        cursor.fetchone()

    cursor.execute(
        """
        SELECT *
        FROM
        VALUES ('key1', 1),
               ('key2', 2),
               ('key3', 3),
               ('key4', 4),
               ('key5', 5),
               ('key6', 6),
               ('key7', 7) AS data(key, value)
        """
    )

    assert cursor.fetchone() == ("key1", 1)
    assert cursor.fetchmany(size=2) == [
        ("key2", 2),
        ("key3", 3),
    ]
    assert cursor.fetchone() == ("key4", 4)
    assert cursor.fetchall() == [
        ("key5", 5),
        ("key6", 6),
        ("key7", 7),
    ]

    assert cursor.fetchone() is None
    assert cursor.fetchmany(size=2) == []
    assert cursor.fetchall() == []

    with pytest.raises(errors.ProgrammingError):
        cursor.fetchmany(size=-1)


def test_spark_session_cursor_execute_parameters_not_supported(
    spark_session: SparkSession,
):
    connection = spark_session_db.connection(spark_session)
    with pytest.raises(errors.NotSupportedError):
        connection.cursor().execute("SELECT 1 WHERE a = ?", ["value"])
