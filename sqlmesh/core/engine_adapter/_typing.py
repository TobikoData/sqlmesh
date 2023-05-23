import typing as t

import pandas as pd
from sqlglot import exp

if t.TYPE_CHECKING:
    import pyspark
    import pyspark.sql.connect.dataframe

    Query = t.Union[exp.Subqueryable, exp.DerivedTable]
    PySparkSession = t.Union[pyspark.sql.SparkSession, pyspark.sql.connect.dataframe.SparkSession]
    PySparkDataFrame = t.Union[pyspark.sql.DataFrame, pyspark.sql.connect.dataframe.DataFrame]
    DF = t.Union[pd.DataFrame, pyspark.sql.DataFrame, pyspark.sql.connect.dataframe.DataFrame]
    QueryOrDF = t.Union[Query, DF]
