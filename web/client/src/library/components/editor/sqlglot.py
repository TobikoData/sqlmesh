import json
import typing as t

import sqlglot
from sqlglot import Tokenizer, exp
from sqlglot.dialects import (
    TSQL,
    BigQuery,
    ClickHouse,
    Databricks,
    Drill,
    DuckDB,
    Hive,
    MySQL,
    Oracle,
    Postgres,
    Presto,
    Redshift,
    Snowflake,
    Spark,
    SQLite,
    StarRocks,
    Tableau,
    Teradata,
    Trino,
)
from sqlglot.dialects.dialect import DialectType

dialects = {
    "bigquery": BigQuery,
    "clickhouse": ClickHouse,
    "databricks": Databricks,
    "drill": Drill,
    "duckdb": DuckDB,
    "hive": Hive,
    "mysql": MySQL,
    "oracle": Oracle,
    "postgres": Postgres,
    "presto": Presto,
    "redshift": Redshift,
    "snowflake": Snowflake,
    "spark": Spark,
    "sqlite": SQLite,
    "starrocks": StarRocks,
    "tableau": Tableau,
    "teradata": Teradata,
    "trino": Trino,
    "tsql": TSQL,
}


def parse_to_json(sql: str, read: DialectType = None) -> str:
    return json.dumps([exp.dump() if exp else {} for exp in sqlglot.parse(sql, read=read)])


def get_dielect(dialect_type: t.Optional[str] = None) -> str:
    dialect: t.Optional[t.Any] = dialects[dialect_type] if dialect_type in dialects else None
    tokenizer = dialect.Tokenizer if dialect is not None else Tokenizer

    return json.dumps(
        {
            "keywords": " ".join(list(tokenizer.KEYWORDS.keys())) + " ",
            "types": [t.value for t in exp.DataType.Type],
        }
    )


def get_dielects() -> str:
    return json.dumps(list(dialects.keys()))


[sqlglot.transpile, parse_to_json, get_dielect, get_dielects]


# from sqlglot.dialects.bigquery import BigQuery
# from sqlglot.dialects.clickhouse import ClickHouse
# from sqlglot.dialects.databricks import Databricks
# from sqlglot.dialects.drill import Drill
# from sqlglot.dialects.duckdb import DuckDB
# from sqlglot.dialects.hive import Hive
# from sqlglot.dialects.mysql import MySQL
# from sqlglot.dialects.oracle import Oracle
# from sqlglot.dialects.postgres import Postgres
# from sqlglot.dialects.presto import Presto
# from sqlglot.dialects.redshift import Redshift
# from sqlglot.dialects.snowflake import Snowflake
# from sqlglot.dialects.spark import Spark
# from sqlglot.dialects.sqlite import SQLite
# from sqlglot.dialects.starrocks import StarRocks
# from sqlglot.dialects.tableau import Tableau
# from sqlglot.dialects.teradata import Teradata
# from sqlglot.dialects.trino import Trino
# from sqlglot.dialects.tsql import TSQL
