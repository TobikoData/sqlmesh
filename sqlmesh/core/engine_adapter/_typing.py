import typing as t

from sqlglot import exp

from sqlmesh.utils import optional_import

if t.TYPE_CHECKING:
    import pandas as pd
    import pyspark
    import pyspark.sql.connect.dataframe
    from bigframes.session import Session as BigframeSession  # noqa
    from bigframes.dataframe import DataFrame as BigframeDataFrame

    snowpark = optional_import("snowflake.snowpark")

    Query = exp.Query
    PySparkSession = t.Union[pyspark.sql.SparkSession, pyspark.sql.connect.dataframe.SparkSession]
    PySparkDataFrame = t.Union[pyspark.sql.DataFrame, pyspark.sql.connect.dataframe.DataFrame]

    # snowpark is not available on python 3.12
    from snowflake.snowpark import Session as SnowparkSession  # noqa
    from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame

    DF = t.Union[
        pd.DataFrame,
        pyspark.sql.DataFrame,
        pyspark.sql.connect.dataframe.DataFrame,
        BigframeDataFrame,
        SnowparkDataFrame,
    ]

    QueryOrDF = t.Union[Query, DF]
    GrantsConfig = t.Dict[str, t.List[str]]
    DCL = t.TypeVar("DCL", exp.Grant, exp.Revoke)
