from __future__ import annotations

import typing as t

from sqlglot import exp, parse_one

from sqlmesh.core.engine_adapter.base import EngineAdapter

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import DF


class SnowflakeEngineAdapter(EngineAdapter):
    DEFAULT_SQL_GEN_KWARGS = {"identify": False}
    DIALECT = "snowflake"
    ESCAPE_JSON = True

    def _fetch_native_df(self, query: t.Union[exp.Expression, str]) -> DF:
        self.execute(query)
        df = self.cursor.fetch_pandas_all()
        # Snowflake returns uppercase column names if the columns are not quoted (so case-insensitive)
        # so replace the column names returned by Snowflake with the column names in the expression
        # if the expression was a select expression
        if isinstance(query, str):
            parsed_query = parse_one(query, read=self.dialect)
            if parsed_query is None:
                # If we didn't get a result from parsing we will just optimistically assume that the df is fine
                return df
            query = parsed_query
        if isinstance(query, exp.Subqueryable):
            df.columns = query.named_selects
        return df
