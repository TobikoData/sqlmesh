import typing as t

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> t.Generator[SparkSession, None, None]:
    session = (
        SparkSession.builder.master("local")
        .appName("SQLMesh Test")
        .enableHiveSupport()
        .getOrCreate()
    )
    yield session
    session.stop()
