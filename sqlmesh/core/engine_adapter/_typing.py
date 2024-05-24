import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.utils import optional_import

if t.TYPE_CHECKING:
    import pyspark
    import pyspark.sql.connect.dataframe

    snowpark = optional_import("snowflake.snowpark")

    Query = t.Union[exp.Query, exp.DerivedTable]
    PySparkSession = t.Union[pyspark.sql.SparkSession, pyspark.sql.connect.dataframe.SparkSession]
    PySparkDataFrame = t.Union[pyspark.sql.DataFrame, pyspark.sql.connect.dataframe.DataFrame]

    # snowpark is not available on python 3.12
    from snowflake.snowpark import Session as SnowparkSession  # noqa
    from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame

    DF = t.Union[
        pd.DataFrame,
        pyspark.sql.DataFrame,
        pyspark.sql.connect.dataframe.DataFrame,
        SnowparkDataFrame,
    ]

    QueryOrDF = t.Union[Query, DF]
