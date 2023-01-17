import typing as t

import pandas as pd
from sqlglot import exp

TARGET_ALIAS = "__MERGE_TARGET__"
SOURCE_ALIAS = "__MERGE_SOURCE__"
DF_TYPES: t.Tuple = (pd.DataFrame,)
Query = t.Union[exp.Subqueryable, exp.DerivedTable]
QUERY_TYPES: t.Tuple = (exp.Subqueryable, exp.DerivedTable)


if t.TYPE_CHECKING:
    import pyspark

    PySparkSession = pyspark.sql.SparkSession
    PySparkDataFrame = pyspark.sql.DataFrame
    DF = t.Union[pd.DataFrame, PySparkDataFrame]
    QueryOrDF = t.Union[Query, DF]
else:
    try:
        import pyspark

        PySparkSession = pyspark.sql.SparkSession
        PySparkDataFrame = pyspark.sql.DataFrame
        DF_TYPES += (PySparkDataFrame,)
    except ImportError:
        PySparkSession = None
        PySparkDataFrame = None
