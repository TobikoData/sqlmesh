from __future__ import annotations

import contextlib
import logging
import typing as t

import pandas as pd
from sqlglot import exp
from sqlglot.errors import ErrorLevel
from sqlglot.helper import ensure_list
from sqlglot.transforms import remove_precision_parameterized_types

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    TransactionType,
)
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.utils.date import to_datetime
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pandas import columns_to_types_from_df

if t.TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.cloud import bigquery
    from google.cloud.bigquery import StandardSqlDataType
    from google.cloud.bigquery.client import Client as BigQueryClient
    from google.cloud.bigquery.client import Connection as BigQueryConnection
    from google.cloud.bigquery.job.base import _AsyncJob as BigQueryQueryResult
    from google.cloud.bigquery.table import Table as BigQueryTable

    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query, QueryOrDF


logger = logging.getLogger(__name__)


class BigQueryEngineAdapter(EngineAdapter):
    """
    BigQuery Engine Adapter using the `google-cloud-bigquery` library's DB API.
    """

    DIALECT = "bigquery"
    DEFAULT_BATCH_SIZE = 1000
    ESCAPE_JSON = True
    SUPPORTS_MATERIALIZED_VIEWS = True

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

    def _begin_session(self) -> None:
        from google.cloud.bigquery import QueryJobConfig

        job = self.client.query("SELECT 1;", job_config=QueryJobConfig(create_session=True))
        session_info = job.session_info
        session_id = session_info.session_id if session_info else None
        self._session_id = session_id
        job.result()

    def _end_session(self) -> None:
        self._session_id = None

    def _is_session_active(self) -> bool:
        return self._session_id is not None

    def create_schema(self, schema_name: str, ignore_if_exists: bool = True) -> None:
        """Create a schema from a name or qualified table name."""
        from google.api_core.exceptions import Conflict

        try:
            super().create_schema(schema_name, ignore_if_exists=ignore_if_exists)
        except Conflict as e:
            if "Already Exists:" in str(e):
                return
            raise e

    def columns(
        self, table_name: TableName, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        """Fetches column names and types for the target table."""

        def dtype_to_sql(dtype: t.Optional[StandardSqlDataType]) -> str:
            assert dtype

            kind = dtype.type_kind
            assert kind

            # Not using the enum value to preserve compatibility with older versions
            # of the BigQuery library.
            if kind.name == "ARRAY":
                return f"ARRAY<{dtype_to_sql(dtype.array_element_type)}>"
            if kind.name == "STRUCT":
                struct_type = dtype.struct_type
                assert struct_type
                fields = ", ".join(
                    f"{field.name} {dtype_to_sql(field.type)}" for field in struct_type.fields
                )
                return f"STRUCT<{fields}>"
            if kind.name == "TYPE_KIND_UNSPECIFIED":
                return "JSON"
            return kind.name

        table = self._get_table(table_name)
        columns = {
            field.name: exp.DataType.build(
                dtype_to_sql(field.to_standard_sql().type), dialect=self.dialect
            )
            for field in table.schema
        }
        if include_pseudo_columns and table.time_partitioning and not table.time_partitioning.field:
            columns["_PARTITIONTIME"] = exp.DataType.build("TIMESTAMP")
            if table.time_partitioning.type_ == "DAY":
                columns["_PARTITIONDATE"] = exp.DataType.build("DATE")
        return columns

    def fetchone(
        self,
        query: t.Union[exp.Expression, str],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = False,
    ) -> t.Tuple:
        """
        BigQuery's `fetchone` method doesn't call execute and therefore would not benefit from the execute
        configuration we have in place. Therefore this implementation calls execute instead.
        """
        self.execute(
            query,
            ignore_unsupported_errors=ignore_unsupported_errors,
            quote_identifiers=quote_identifiers,
        )
        try:
            return next(self._query_data)
        except StopIteration:
            return ()

    def fetchall(
        self,
        query: t.Union[exp.Expression, str],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = False,
    ) -> t.List[t.Tuple]:
        """
        BigQuery's `fetchone` method doesn't call execute and therefore would not benefit from the execute
        configuration we have in place. Therefore this implementation calls execute instead.
        """
        self.execute(
            query,
            ignore_unsupported_errors=ignore_unsupported_errors,
            quote_identifiers=quote_identifiers,
        )
        return list(self._query_data)

    def _create_table_from_df(
        self,
        table_name: TableName,
        df: DF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        replace: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """
        Creates a table from a pandas dataframe. Will create the table if it doesn't exist. Will replace the contents
        of the table if `replace` is true.
        """
        assert isinstance(df, pd.DataFrame)
        if columns_to_types is None:
            columns_to_types = columns_to_types_from_df(df)
        table = self.__get_bq_table(table_name, columns_to_types)
        self._db_call(self.client.create_table, table=table, exists_ok=exists)
        self.__load_pandas_to_table(table, df, columns_to_types, replace=replace)

    def _insert_append_pandas_df(
        self,
        table_name: TableName,
        df: pd.DataFrame,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        contains_json: bool = False,
    ) -> None:
        """
        Appends to a table from a pandas dataframe. Will create the table if it doesn't exist.
        """
        if columns_to_types is None:
            columns_to_types = columns_to_types_from_df(df)
        table = self.__get_bq_table(table_name, columns_to_types)
        self.__load_pandas_to_table(table, df, columns_to_types, replace=False)

    def __load_pandas_to_table(
        self,
        table: bigquery.Table,
        df: pd.DataFrame,
        columns_to_types: t.Dict[str, exp.DataType],
        replace: bool = False,
    ) -> BigQueryQueryResult:
        """
        Loads a pandas dataframe into a table in BigQuery. Will do an overwrite if replace is True. Note that
        the replace will replace the entire table, not just the rows that are in the dataframe.
        """
        from google.cloud import bigquery

        job_config = bigquery.job.LoadJobConfig(schema=self.__get_bq_schema(columns_to_types))
        if replace:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        logger.debug(f"Loading dataframe to BigQuery. Table Path: {table.path}")
        # This client call does not support retry so we don't use the `_db_call` method.
        result = self.__retry(
            self.__db_load_table_from_dataframe,
        )(df=df, table=table, job_config=job_config)
        if result.errors:
            raise SQLMeshError(result.errors)
        return result

    def __db_load_table_from_dataframe(
        self, df: pd.DataFrame, table: bigquery.Table, job_config: bigquery.LoadJobConfig
    ) -> None:
        job = self.client.load_table_from_dataframe(
            dataframe=df, destination=table, job_config=job_config
        )
        return self._db_call(job.result)

    def __get_bq_schema(
        self, columns_to_types: t.Dict[str, exp.DataType]
    ) -> t.List[bigquery.SchemaField]:
        """
        Returns a bigquery schema object from a dictionary of column names to types.
        """
        from google.cloud import bigquery

        precisionless_col_to_types = {
            col_name: remove_precision_parameterized_types(col_type)
            for col_name, col_type in columns_to_types.items()
        }
        return [
            bigquery.SchemaField(col_name, col_type.sql(dialect=self.dialect))
            for col_name, col_type in precisionless_col_to_types.items()
        ]

    def __get_temp_bq_table(
        self, table: TableName, columns_to_type: t.Dict[str, exp.DataType]
    ) -> bigquery.Table:
        """
        Returns a bigquery table object that is temporary and will expire in 3 hours.
        """
        bq_table = self.__get_bq_table(self._get_temp_table(table), columns_to_type)
        bq_table.expires = to_datetime("in 3 hours")
        return bq_table

    def __get_bq_table(
        self, table: TableName, columns_to_type: t.Dict[str, exp.DataType]
    ) -> bigquery.Table:
        """
        Returns a bigquery table object with a schema defines that matches the columns_to_type dictionary.
        """
        from google.cloud import bigquery

        table_ = exp.to_table(table).copy()

        if not table_.catalog:
            table_.set("catalog", exp.to_identifier(self.client.project))

        return bigquery.Table(
            table_ref=self._table_name(table_),
            schema=self.__get_bq_schema(columns_to_type),
        )

    @property
    def __retry(self) -> Retry:
        from google.api_core import retry

        return retry.Retry(
            predicate=_ErrorCounter(self._extra_config["job_retries"]).should_retry,
            sleep_generator=retry.exponential_sleep_generator(initial=1.0, maximum=3.0),
            deadline=self._extra_config.get("job_retry_deadline_seconds"),
        )

    @contextlib.contextmanager
    def __try_load_pandas_to_temp_table(
        self,
        reference_table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
    ) -> t.Generator[Query, None, None]:
        reference_table = exp.to_table(reference_table_name)
        df = self.try_get_pandas_df(query_or_df)
        if df is None:
            yield t.cast("Query", query_or_df)
            return
        if columns_to_types is None:
            raise SQLMeshError("columns_to_types must be provided when using Pandas DataFrames")
        if reference_table.db is None:
            raise SQLMeshError("table must be qualified when using Pandas DataFrames")
        temp_bq_table = self.__get_temp_bq_table(reference_table, columns_to_types)
        self._db_call(self.client.create_table, table=temp_bq_table, exists_ok=False)
        result = self.__load_pandas_to_table(temp_bq_table, df, columns_to_types, replace=False)
        if result.errors:
            raise SQLMeshError(result.errors)

        temp_table = exp.table_(
            temp_bq_table.table_id,
            db=temp_bq_table.dataset_id,
            catalog=temp_bq_table.project,
        )
        yield exp.select(*columns_to_types).from_(temp_table)
        self.drop_table(temp_table)

    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[str],
    ) -> None:
        with self.__try_load_pandas_to_temp_table(
            target_table, source_table, columns_to_types
        ) as source_table:
            return super().merge(target_table, source_table, columns_to_types, unique_key)

    def insert_overwrite_by_partition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        partitioned_by: t.List[exp.Expression],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        if len(partitioned_by) != 1:
            raise SQLMeshError(
                f"Bigquery only supports partitioning by one column, {len(partitioned_by)} were provided."
            )

        partition_exp = partitioned_by[0]
        partition_sql = partition_exp.sql(dialect=self.dialect)
        partition_column = partition_exp.find(exp.Column)

        if not partition_column:
            raise SQLMeshError(
                f"The partition expression '{partition_sql}' doesn't contain a column."
            )

        with self.session(), self.temp_table(query_or_df, name=table_name) as temp_table_name:
            if columns_to_types is None:
                columns_to_types = self.columns(temp_table_name)

            partition_type_sql = columns_to_types[partition_column.name].sql(dialect=self.dialect)
            temp_table_name_sql = temp_table_name.sql(dialect=self.dialect)
            self.execute(
                f"DECLARE _sqlmesh_target_partitions_ ARRAY<{partition_type_sql}> DEFAULT (SELECT ARRAY_AGG(DISTINCT {partition_sql}) FROM {temp_table_name_sql});"
            )

            where = t.cast(exp.Condition, partition_exp).isin(unnest="_sqlmesh_target_partitions_")

            self._insert_overwrite_by_condition(
                table_name,
                exp.select("*").from_(temp_table_name),
                where=where,
                columns_to_types=columns_to_types,
            )

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
        with self.__try_load_pandas_to_temp_table(
            table_name, query_or_df, columns_to_types
        ) as source_table:
            query = self._add_where_to_query(source_table, where)

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
                target_table=table_name,
                source_table=query,
                on=exp.false(),
                match_expressions=[when_not_matched_by_source, when_not_matched_by_target],
            )

    def table_exists(self, table_name: TableName) -> bool:
        try:
            from google.cloud.exceptions import NotFound
        except ModuleNotFoundError:
            from google.api_core.exceptions import NotFound

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
        return self._db_call(self.client.get_table, table=self._table_name(table_name))

    def _table_name(self, table_name: TableName) -> str:
        # the api doesn't support backticks, so we can't call exp.table_name or sql
        return ".".join(part.name for part in exp.to_table(table_name).parts)

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        self.execute(query, quote_identifiers=quote_identifiers)
        return self._query_job.to_dataframe()

    def _create_table_properties(
        self,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[str]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
    ) -> t.Optional[exp.Properties]:
        properties: t.List[exp.Expression] = []

        if partitioned_by:
            if partition_interval_unit is None:
                raise SQLMeshError("partition_interval_unit is required when partitioning a table")
            if len(partitioned_by) > 1:
                raise SQLMeshError("BigQuery only supports partitioning by a single column")

            this = partitioned_by[0]

            if isinstance(this, exp.Column):
                if partition_interval_unit == IntervalUnit.MINUTE:
                    raise SQLMeshError("BigQuery does not support partitioning by minute")

                if partition_interval_unit == IntervalUnit.HOUR:
                    trunc_func = "TIMESTAMP_TRUNC"
                elif partition_interval_unit in (IntervalUnit.MONTH, IntervalUnit.YEAR):
                    trunc_func = "DATE_TRUNC"
                else:
                    trunc_func = ""

                if trunc_func:
                    this = exp.func(
                        trunc_func,
                        this,
                        exp.var(partition_interval_unit.value.upper()),
                        dialect=self.dialect,
                    )

            properties.append(exp.PartitionedByProperty(this=this))

        if clustered_by:
            properties.append(exp.Cluster(expressions=[exp.column(col) for col in clustered_by]))

        for key, value in (table_properties or {}).items():
            properties.append(exp.Property(this=key, value=value))

        if properties:
            return exp.Properties(expressions=properties)
        return None

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

    def _db_call(self, func: t.Callable[..., t.Any], *args: t.Any, **kwargs: t.Any) -> t.Any:
        return func(
            retry=self.__retry,
            *args,
            **kwargs,
        )

    def execute(
        self,
        expressions: t.Union[str, exp.Expression, t.Sequence[exp.Expression]],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """Execute a sql query."""
        from google.cloud.bigquery import QueryJobConfig
        from google.cloud.bigquery.query import ConnectionProperty

        to_sql_kwargs = (
            {"unsupported_level": ErrorLevel.IGNORE} if ignore_unsupported_errors else {}
        )

        for e in ensure_list(expressions):
            sql = (
                self._to_sql(e, quote=quote_identifiers, **to_sql_kwargs)
                if isinstance(e, exp.Expression)
                else e
            )
            logger.debug(f"Executing SQL:\n{sql}")

            # BigQuery's Python DB API implementation does not support retries, so we have to implement them ourselves.
            # So we update the cursor's query job and query data with the results of the new query job. This makes sure
            # that other cursor based operations execute correctly.
            session_id = self._session_id
            connection_properties = (
                [
                    ConnectionProperty(key="session_id", value=session_id),
                ]
                if session_id
                else []
            )

            job_config = QueryJobConfig(
                **self._job_params, connection_properties=connection_properties
            )
            self._query_job = self._db_call(
                self.client.query,
                query=sql,
                job_config=job_config,
                timeout=self._extra_config.get("job_creation_timeout_seconds"),
            )
            results = self._db_call(
                self._query_job.result,
                timeout=self._extra_config.get("job_execution_timeout_seconds"),  # type: ignore
            )
            self._query_data = iter(results) if results.total_rows else iter([])
            query_results = self._query_job._query_results
            self.cursor._set_rowcount(query_results)
            self.cursor._set_description(query_results.schema)

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
        all_tables = self._db_call(self.client.list_tables, dataset=dataset_ref)
        return [
            DataObject(
                catalog=table.project,
                schema=table.dataset_id,
                name=table.table_id,
                type=DataObjectType.from_str(table.table_type),
            )
            for table in all_tables
        ]

    @property
    def _query_data(self) -> t.Any:
        return self._connection_pool.get_attribute("query_data")

    @_query_data.setter
    def _query_data(self, value: t.Any) -> None:
        return self._connection_pool.set_attribute("query_data", value)

    @property
    def _query_job(self) -> t.Any:
        return self._connection_pool.get_attribute("query_job")

    @_query_job.setter
    def _query_job(self, value: t.Any) -> None:
        return self._connection_pool.set_attribute("query_job", value)

    @property
    def _session_id(self) -> t.Any:
        return self._connection_pool.get_attribute("session_id")

    @_session_id.setter
    def _session_id(self, value: t.Any) -> None:
        return self._connection_pool.set_attribute("session_id", value)


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
        try:
            from google.cloud.exceptions import ServerError
        except ModuleNotFoundError:
            from google.api_core.exceptions import ServerError
        from requests.exceptions import ConnectionError

        return (ServerError, ConnectionError)

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
