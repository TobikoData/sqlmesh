from __future__ import annotations

import functools
import logging
import typing as t

import pandas as pd
from sqlglot import exp
from sqlglot.errors import ErrorLevel
from sqlglot.transforms import remove_precision_parameterized_types

from sqlmesh.core.engine_adapter._typing import DF_TYPES, Query
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    TransactionType,
)
from sqlmesh.core.model.meta import IntervalUnit
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.utils.date import to_datetime
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from google.cloud.bigquery.client import Client as BigQueryClient
    from google.cloud.bigquery.client import Connection as BigQueryConnection
    from google.cloud.bigquery.job.base import _AsyncJob as BigQueryQueryResult
    from google.cloud.bigquery.table import Table as BigQueryTable

    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF


logger = logging.getLogger(__name__)


class BigQueryEngineAdapter(EngineAdapter):
    """
    BigQuery Engine Adapter using the `google-cloud-bigquery` library's DB API.

    TODO: Consider writing a custom implementation using the BigQuery API directly because we are already starting
    to override some of the DB API behavior.
    """

    DIALECT = "bigquery"
    DEFAULT_BATCH_SIZE = 1000
    ESCAPE_JSON = True
    # SQL is not supported for adding columns to structs: https://cloud.google.com/bigquery/docs/managing-table-schemas#api_1
    # Can explore doing this with the API in the future
    SCHEMA_DIFFER = SchemaDiffer(
        compatible_types={
            exp.DataType.build("INT64", dialect=DIALECT): {
                exp.DataType.build("NUMERIC", dialect=DIALECT),
                exp.DataType.build("FLOAT64", dialect=DIALECT),
            },
            exp.DataType.build("NUMERIC", dialect=DIALECT): {
                exp.DataType.build("FLOAT64", dialect=DIALECT),
            },
            exp.DataType.build("DATE", dialect=DIALECT): {
                exp.DataType.build("DATETIME", dialect=DIALECT),
            },
        },
    )

    @property
    def client(self) -> BigQueryClient:
        return self.cursor.connection._client

    @property
    def connection(self) -> BigQueryConnection:
        return self.cursor.connection

    @property
    def _job_params(self) -> t.Dict[str, t.Any]:
        from sqlmesh.core.config.connection import BigQueryPriority

        params = {
            "use_legacy_sql": False,
            "priority": self._extra_config.get(
                "priority", BigQueryPriority.INTERACTIVE.bigquery_constant
            ),
        }
        if self._extra_config.get("maximum_bytes_billed"):
            params["maximum_bytes_billed"] = self._extra_config.get("maximum_bytes_billed")
        return params

    def create_schema(self, schema_name: str, ignore_if_exists: bool = True) -> None:
        """Create a schema from a name or qualified table name."""
        from google.api_core.exceptions import Conflict

        try:
            super().create_schema(schema_name, ignore_if_exists=ignore_if_exists)
        except Conflict as e:
            for arg in e.args:
                if ignore_if_exists and "Already Exists: " in arg.message:
                    return
            raise e

    def columns(self, table_name: TableName) -> t.Dict[str, exp.DataType]:
        """Fetches column names and types for the target table."""
        table = self._get_table(table_name)
        return {
            field.name: exp.DataType.build(field.field_type, dialect=self.dialect)
            for field in table.schema
        }

    def fetchone(
        self,
        query: t.Union[exp.Expression, str],
        ignore_unsupported_errors: bool = False,
    ) -> t.Tuple:
        """
        BigQuery's `fetchone` method doesn't call execute and therefore would not benefit from the execute
        configuration we have in place. Therefore this implementation calls execute instead.
        """
        self.execute(query, ignore_unsupported_errors=ignore_unsupported_errors)
        try:
            return next(self.cursor._query_data)
        except StopIteration:
            return ()

    def fetchall(
        self,
        query: t.Union[exp.Expression, str],
        ignore_unsupported_errors: bool = False,
    ) -> t.List[t.Tuple]:
        """
        BigQuery's `fetchone` method doesn't call execute and therefore would not benefit from the execute
        configuration we have in place. Therefore this implementation calls execute instead.
        """
        self.execute(query, ignore_unsupported_errors=ignore_unsupported_errors)
        return list(self.cursor._query_data)

    def __load_pandas_to_temp_table(
        self,
        table: TableName,
        df: pd.DataFrame,
        columns_to_types: t.Dict[str, exp.DataType],
    ) -> t.Tuple[BigQueryQueryResult, str]:
        """
        Loads a pandas dataframe into a temporary table in BigQuery. Returns the result of the load and the name of the
        temporary table. The temporary table will be deleted after 3 hours.
        """
        from google.cloud import bigquery

        precisionless_col_to_types = {
            col_name: remove_precision_parameterized_types(col_type)
            for col_name, col_type in columns_to_types.items()
        }

        temp_table_name = ".".join(
            [self.client.project, self._get_temp_table(table).sql(dialect=self.dialect)]
        )
        schema = [
            bigquery.SchemaField(col_name, col_type.sql(dialect=self.dialect))
            for col_name, col_type in precisionless_col_to_types.items()
        ]
        bq_table = bigquery.Table(table_ref=temp_table_name, schema=schema)
        bq_table.expires = to_datetime("in 3 hours")
        self.client.create_table(bq_table)
        result = self.client.load_table_from_dataframe(df, bq_table).result()
        if result.errors:
            raise SQLMeshError(result.errors)
        return result, temp_table_name

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        where: t.Optional[exp.Condition] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        """
        Bigquery does not directly support `INSERT OVERWRITE` but it does support `MERGE` with a `False`
        condition and delete that mimics an `INSERT OVERWRITE`. Based on documentation this should have the
        same runtime performance as `INSERT OVERWRITE`.

        If a Pandas DataFrame is provided, it will be loaded into a temporary table and then merged with the
        target table. This temporary table is deleted after the merge is complete or after it's expiration time has
        passed.
        """
        table = exp.to_table(table_name)
        query: t.Union[Query, exp.Select]
        is_pandas = isinstance(query_or_df, pd.DataFrame)
        temp_table_name: t.Optional[str] = None
        if isinstance(query_or_df, DF_TYPES):
            if not is_pandas:
                raise SQLMeshError("BigQuery only supports pandas DataFrames")
            if columns_to_types is None:
                raise SQLMeshError("columns_to_types must be provided when using Pandas DataFrames")
            if table.db is None:
                raise SQLMeshError("table_name must be qualified when using Pandas DataFrames")
            query_or_df = t.cast(pd.DataFrame, query_or_df)
            result, temp_table_name = self.__load_pandas_to_temp_table(
                table, query_or_df, columns_to_types
            )
            if result.errors:
                raise SQLMeshError(result.errors)
            query = exp.select(*columns_to_types).from_(exp.to_table(temp_table_name))
        else:
            query = t.cast(Query, query_or_df)
        columns = [
            exp.to_column(col)
            for col in (columns_to_types or [col.alias_or_name for col in query.expressions])
        ]
        when_not_matched_by_source = exp.When(
            matched=False,
            source=True,
            condition=where,
            then=exp.Delete(),
        )
        when_not_matched_by_target = exp.When(
            matched=False,
            source=False,
            then=exp.Insert(
                this=exp.Tuple(expressions=columns),
                expression=exp.Tuple(expressions=columns),
            ),
        )
        self._merge(
            target_table=table,
            source_table=query,
            on=exp.false(),
            match_expressions=[when_not_matched_by_source, when_not_matched_by_target],
        )
        if is_pandas:
            assert temp_table_name is not None
            self.drop_table(temp_table_name)

    def table_exists(self, table_name: TableName) -> bool:
        from google.cloud.exceptions import NotFound

        try:
            self._get_table(table_name)
            return True
        except NotFound:
            return False

    def _get_table(self, table_name: TableName) -> BigQueryTable:
        """
        Returns a BigQueryTable object for the given table name.

        Raises: `google.cloud.exceptions.NotFound` if the table does not exist.
        """
        if isinstance(table_name, exp.Table):
            table_name = table_name.sql(dialect=self.dialect)

        return self.client.get_table(table_name)

    def _fetch_native_df(self, query: t.Union[exp.Expression, str]) -> DF:
        self.execute(query)
        return self.cursor._query_job.to_dataframe()

    def _create_table_properties(
        self,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[str]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
    ) -> t.Optional[exp.Properties]:
        if not partitioned_by:
            return None
        if partition_interval_unit is None:
            raise SQLMeshError("partition_interval_unit is required when partitioning a table")
        if partition_interval_unit == IntervalUnit.MINUTE:
            raise SQLMeshError("BigQuery does not support partitioning by minute")
        if len(partitioned_by) > 1:
            raise SQLMeshError("BigQuery only supports partitioning by a single column")
        partition_col = exp.to_column(partitioned_by[0])
        this: t.Union[exp.Func, exp.Column]
        if partition_interval_unit == IntervalUnit.HOUR:
            this = exp.func(
                "TIMESTAMP_TRUNC",
                partition_col,
                exp.var(IntervalUnit.HOUR.value.upper()),
                dialect=self.dialect,
            )
        else:
            this = partition_col

        partition_columns_property = exp.PartitionedByProperty(this=this)
        return exp.Properties(expressions=[partition_columns_property])

    def create_state_table(
        self,
        table_name: str,
        columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
    ) -> None:
        self.create_table(
            table_name,
            columns_to_types,
        )

    def supports_transactions(self, transaction_type: TransactionType) -> bool:
        return False

    def _retryable_execute(
        self,
        sql: str,
    ) -> None:
        """
        BigQuery's Python DB API implementation does not support retries, so we have to implement them ourselves.
        So we update the cursor's query job and query data with the results of the new query job. This makes sure
        that other cursor based operations execute correctly.
        """
        from google.cloud.bigquery import QueryJobConfig

        job_config = QueryJobConfig(**self._job_params)
        self.cursor._query_job = self.client.query(
            sql,
            job_config=job_config,
            timeout=self._extra_config.get("job_creation_timeout_seconds"),
        )
        results = self.cursor._query_job.result(
            timeout=self._extra_config.get("job_execution_timeout_seconds")  # type: ignore
        )
        self.cursor._query_data = iter(results) if results.total_rows else iter([])
        query_results = self.cursor._query_job._query_results
        self.cursor._set_rowcount(query_results)
        self.cursor._set_description(query_results.schema)

    def execute(
        self,
        sql: t.Union[str, exp.Expression],
        ignore_unsupported_errors: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """Execute a sql query."""
        from google.api_core import retry

        to_sql_kwargs = (
            {"unsupported_level": ErrorLevel.IGNORE} if ignore_unsupported_errors else {}
        )
        sql = self._to_sql(sql, **to_sql_kwargs) if isinstance(sql, exp.Expression) else sql
        logger.debug(f"Executing SQL:\n{sql}")
        retry.retry_target(
            target=functools.partial(self._retryable_execute, sql=sql),
            predicate=_ErrorCounter(self._extra_config["job_retries"]).should_retry,
            sleep_generator=retry.exponential_sleep_generator(initial=1.0, maximum=3.0),
            deadline=self._extra_config.get("job_retry_deadline_seconds"),
        )

    def _get_data_objects(
        self, schema_name: str, catalog_name: t.Optional[str] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        from google.cloud.bigquery import DatasetReference

        dataset_ref = DatasetReference(
            project=catalog_name or self.client.project, dataset_id=schema_name
        )
        all_tables = self.client.list_tables(dataset_ref)
        return [
            DataObject(
                catalog=table.project,
                schema=table.dataset_id,
                name=table.table_id,
                type=DataObjectType.from_str(table.table_type),
            )
            for table in all_tables
        ]


class _ErrorCounter:
    """
    A class that counts errors and determines whether or not to retry based on the number of errors and the error
    type.

    Reference implementation: https://github.com/dbt-labs/dbt-bigquery/blob/8339a034929b12e027f0a143abf46582f3f6ffbc/dbt/adapters/bigquery/connections.py#L672

    TODO: Implement a retry configuration that works across all engines
    """

    def __init__(self, num_retries: int) -> None:
        self.num_retries = num_retries
        self.error_count = 0

    @property
    def retryable_errors(self) -> t.Tuple[t.Type[Exception], ...]:
        from google.cloud.exceptions import BadRequest, ServerError
        from requests.exceptions import ConnectionError

        return (ServerError, BadRequest, ConnectionError)

    def _is_retryable(self, error: t.Type[Exception]) -> bool:
        from google.api_core.exceptions import Forbidden

        if isinstance(error, self.retryable_errors):
            return True
        elif isinstance(error, Forbidden) and any(
            e["reason"] == "rateLimitExceeded" for e in error.errors
        ):
            return True
        return False

    def should_retry(self, error: t.Type[Exception]) -> bool:
        if self.num_retries == 0:
            return False
        self.error_count += 1
        if self._is_retryable(error) and self.error_count <= self.num_retries:
            logger.debug(
                f"Retry Num {self.error_count} of {self.num_retries}. Error: {repr(error)}"
            )
            return True
        return False
