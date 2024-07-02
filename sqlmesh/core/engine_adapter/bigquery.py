from __future__ import annotations

import logging
import typing as t

import pandas as pd
from sqlglot import exp
from sqlglot.transforms import remove_precision_parameterized_types

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.mixins import InsertOverwriteWithMergeMixin
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    DataObject,
    DataObjectType,
    SourceQuery,
    set_catalog,
)
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.utils.date import to_datetime
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.cloud import bigquery
    from google.cloud.bigquery import StandardSqlDataType
    from google.cloud.bigquery.client import Client as BigQueryClient
    from google.cloud.bigquery.client import Connection as BigQueryConnection
    from google.cloud.bigquery.job.base import _AsyncJob as BigQueryQueryResult
    from google.cloud.bigquery.table import Table as BigQueryTable

    from sqlmesh.core._typing import SchemaName, SessionProperties, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query
    from sqlmesh.core.engine_adapter.base import QueryOrDF

logger = logging.getLogger(__name__)


@set_catalog()
class BigQueryEngineAdapter(InsertOverwriteWithMergeMixin):
    """
    BigQuery Engine Adapter using the `google-cloud-bigquery` library's DB API.
    """

    DIALECT = "bigquery"
    DEFAULT_BATCH_SIZE = 1000
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_CLONING = True
    CATALOG_SUPPORT = CatalogSupport.FULL_SUPPORT
    MAX_TABLE_COMMENT_LENGTH = 1024
    MAX_COLUMN_COMMENT_LENGTH = 1024

    # SQL is not supported for adding columns to structs: https://cloud.google.com/bigquery/docs/managing-table-schemas#api_1
    # Can explore doing this with the API in the future
    SCHEMA_DIFFER = SchemaDiffer(
        compatible_types={
            exp.DataType.build("INT64", dialect=DIALECT): {
                exp.DataType.build("NUMERIC", dialect=DIALECT),
                exp.DataType.build("FLOAT64", dialect=DIALECT),
                exp.DataType.build("BIGNUMERIC", dialect=DIALECT),
            },
            exp.DataType.build("NUMERIC", dialect=DIALECT): {
                exp.DataType.build("FLOAT64", dialect=DIALECT),
                exp.DataType.build("BIGNUMERIC", dialect=DIALECT),
            },
            exp.DataType.build("DATE", dialect=DIALECT): {
                exp.DataType.build("DATETIME", dialect=DIALECT),
            },
        },
        support_coercing_compatible_types=True,
        parameterized_type_defaults={
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(38, 9), (0,)],
            exp.DataType.build("BIGDECIMAL", dialect=DIALECT).this: [(76.76, 38), (0,)],
        },
        types_with_unlimited_length={
            # parameterized `STRING(n)` can ALTER to unparameterized `STRING`
            exp.DataType.build("STRING", dialect=DIALECT).this: {
                exp.DataType.build("STRING", dialect=DIALECT).this,
            },
            # parameterized `BYTES(n)` can ALTER to unparameterized `BYTES`
            exp.DataType.build("BYTES", dialect=DIALECT).this: {
                exp.DataType.build("BYTES", dialect=DIALECT).this,
            },
        },
    )

    @property
    def client(self) -> BigQueryClient:
        return self.connection._client

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

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        temp_bq_table = self.__get_temp_bq_table(
            self._get_temp_table(target_table or "pandas"), columns_to_types
        )
        temp_table = exp.table_(
            temp_bq_table.table_id,
            db=temp_bq_table.dataset_id,
            catalog=temp_bq_table.project,
        )

        def query_factory() -> Query:
            if not self.table_exists(temp_table):
                # Make mypy happy
                assert isinstance(df, pd.DataFrame)
                self._db_call(self.client.create_table, table=temp_bq_table, exists_ok=False)
                result = self.__load_pandas_to_table(
                    temp_bq_table, df, columns_to_types, replace=False
                )
                if result.errors:
                    raise SQLMeshError(result.errors)
            return self._select_columns(columns_to_types).from_(temp_table)

        return [
            SourceQuery(
                query_factory=query_factory,
                cleanup_func=lambda: self.drop_table(temp_table),
            )
        ]

    def _begin_session(self, properties: SessionProperties) -> None:
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

    def get_current_catalog(self) -> t.Optional[str]:
        """Returns the catalog name of the current connection."""
        return self.client.project

    def set_current_catalog(self, catalog: str) -> None:
        """Sets the catalog name of the current connection."""
        self.client.project = catalog

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
    ) -> None:
        """Create a schema from a name or qualified table name."""
        from google.api_core.exceptions import Conflict

        try:
            super().create_schema(
                schema_name,
                ignore_if_exists=ignore_if_exists,
                warn_on_error=False,
            )
        except Exception as e:
            is_already_exists_error = isinstance(e, Conflict) and "Already Exists:" in str(e)
            if is_already_exists_error and ignore_if_exists:
                return
            if not warn_on_error:
                raise
            logger.warning("Failed to create schema '%s': %s", schema_name, e)

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
        logger.info(f"Loading dataframe to BigQuery. Table Path: {table.path}")
        # This client call does not support retry so we don't use the `_db_call` method.
        result = self.__retry(
            self.__db_load_table_from_dataframe,
        )(df=df, table=table, job_config=job_config)
        if result.errors:
            raise SQLMeshError(result.errors)
        return result

    def __db_load_table_from_dataframe(
        self, df: pd.DataFrame, table: bigquery.Table, job_config: bigquery.LoadJobConfig
    ) -> BigQueryQueryResult:
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
        self, table: exp.Table, columns_to_type: t.Dict[str, exp.DataType]
    ) -> bigquery.Table:
        """
        Returns a bigquery table object that is temporary and will expire in 3 hours.
        """
        bq_table = self.__get_bq_table(table, columns_to_type)
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
            deadline=self._extra_config.get("job_retry_deadline_seconds"),
            initial=1.0,
            maximum=3.0,
        )

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
        partition_column = partition_exp.find(exp.Column)

        granularity = partition_exp.args.get("unit")
        if granularity:
            granularity = granularity.name.lower()

        if not partition_column:
            partition_sql = partition_exp.sql(dialect=self.dialect)
            raise SQLMeshError(
                f"The partition expression '{partition_sql}' doesn't contain a column."
            )
        with self.session({}), self.temp_table(
            query_or_df, name=table_name, partitioned_by=partitioned_by
        ) as temp_table_name:
            if columns_to_types is None or columns_to_types[
                partition_column.name
            ] == exp.DataType.build("unknown"):
                columns_to_types = self.columns(temp_table_name)

            partition_type_sql = columns_to_types[partition_column.name].sql(dialect=self.dialect)

            select_array_agg_partitions = select_partitions_expr(
                temp_table_name.db,
                temp_table_name.name,
                partition_type_sql,
                granularity=granularity,
                agg_func="ARRAY_AGG",
            )

            self.execute(
                f"DECLARE _sqlmesh_target_partitions_ ARRAY<{partition_type_sql}> DEFAULT ({select_array_agg_partitions});"
            )

            where = t.cast(exp.Condition, partition_exp).isin(unnest="_sqlmesh_target_partitions_")

            self._insert_overwrite_by_condition(
                table_name,
                [SourceQuery(query_factory=lambda: exp.select("*").from_(temp_table_name))],
                columns_to_types,
                where=where,
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

    def _create_column_comments(
        self,
        table_name: TableName,
        column_comments: t.Dict[str, str],
        table_kind: str = "TABLE",
    ) -> None:
        table = self._get_table(table_name)

        # convert Table object to dict
        table_def = table.to_api_repr()

        # set the column descriptions
        for i in range(len(table_def["schema"]["fields"])):
            comment = column_comments.get(table_def["schema"]["fields"][i]["name"], None)
            if comment:
                table_def["schema"]["fields"][i]["description"] = self._truncate_comment(
                    comment, self.MAX_COLUMN_COMMENT_LENGTH
                )

        # An "etag" is BQ versioning metadata that changes when an object is updated/modified. `update_table`
        # compares the etags of the table object passed to it and the remote table, erroring if the etags
        # don't match. We set the local etag to None to avoid this check.
        table_def["etag"] = None

        # convert dict back to a Table object
        table = table.from_api_repr(table_def)

        # update table schema
        logger.info(f"Registering column comments for table {table_name}")
        self._db_call(self.client.update_table, table=table, fields=["schema"])

    def _build_description_property_exp(
        self,
        description: str,
        trunc_method: t.Callable,
    ) -> exp.Property:
        return exp.Property(
            this=exp.to_identifier("description", quoted=True),
            value=exp.Literal.string(trunc_method(description)),
        )

    def _build_table_properties_exp(
        self,
        catalog_name: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[str]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
    ) -> t.Optional[exp.Properties]:
        properties: t.List[exp.Expression] = []

        if partitioned_by:
            if len(partitioned_by) > 1:
                raise SQLMeshError("BigQuery only supports partitioning by a single column")

            this = partitioned_by[0]

            if (
                isinstance(this, exp.Column)
                and partition_interval_unit is not None
                and not partition_interval_unit.is_minute
            ):
                column_type: t.Optional[exp.DataType] = (columns_to_types or {}).get(this.name)

                if column_type == exp.DataType.build(
                    "date", dialect=self.dialect
                ) and partition_interval_unit in (
                    IntervalUnit.MONTH,
                    IntervalUnit.YEAR,
                ):
                    trunc_func = "DATE_TRUNC"
                elif column_type == exp.DataType.build("timestamp", dialect=self.dialect):
                    trunc_func = "TIMESTAMP_TRUNC"
                elif column_type == exp.DataType.build("datetime", dialect=self.dialect):
                    trunc_func = "DATETIME_TRUNC"
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

        if table_description:
            properties.append(
                self._build_description_property_exp(
                    table_description, self._truncate_table_comment
                ),
            )

        properties.extend(self._table_or_view_properties_to_expressions(table_properties))

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _build_col_comment_exp(
        self, col_name: str, column_descriptions: t.Dict[str, str]
    ) -> t.List[exp.ColumnConstraint]:
        comment = column_descriptions.get(col_name, None)
        if comment:
            return [
                exp.ColumnConstraint(
                    kind=exp.Properties(
                        expressions=[
                            self._build_description_property_exp(
                                comment, self._truncate_column_comment
                            ),
                        ]
                    )
                )
            ]
        return []

    def _build_view_properties_exp(
        self,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        table_description: t.Optional[str] = None,
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for view"""
        properties: t.List[exp.Expression] = []

        if table_description:
            properties.append(
                self._build_description_property_exp(
                    table_description, self._truncate_table_comment
                ),
            )

        properties.extend(self._table_or_view_properties_to_expressions(view_properties))

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _build_create_comment_table_exp(
        self, table: exp.Table, table_comment: str, table_kind: str
    ) -> exp.Comment | str:
        table_sql = table.sql(dialect=self.dialect, identify=True)

        truncated_comment = self._truncate_table_comment(table_comment)
        comment_sql = exp.Literal.string(truncated_comment).sql(dialect=self.dialect)

        return f"ALTER {table_kind} {table_sql} SET OPTIONS(description = {comment_sql})"

    def _build_create_comment_column_exp(
        self, table: exp.Table, column_name: str, column_comment: str, table_kind: str = "TABLE"
    ) -> exp.Comment | str:
        table_sql = table.sql(dialect=self.dialect, identify=True)
        column_sql = exp.column(column_name).sql(dialect=self.dialect, identify=True)

        truncated_comment = self._truncate_column_comment(column_comment)
        comment_sql = exp.Literal.string(truncated_comment).sql(dialect=self.dialect)

        return f"ALTER {table_kind} {table_sql} ALTER COLUMN {column_sql} SET OPTIONS(description = {comment_sql})"

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

    def _db_call(self, func: t.Callable[..., t.Any], *args: t.Any, **kwargs: t.Any) -> t.Any:
        return func(
            retry=self.__retry,
            *args,
            **kwargs,
        )

    def _execute(
        self,
        sql: str,
        **kwargs: t.Any,
    ) -> None:
        """Execute a sql query."""
        from google.cloud.bigquery import QueryJobConfig
        from google.cloud.bigquery.query import ConnectionProperty

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

        job_config = QueryJobConfig(**self._job_params, connection_properties=connection_properties)
        self._query_job = self._db_call(
            self.client.query,
            query=sql,
            job_config=job_config,
            timeout=self._extra_config.get("job_creation_timeout_seconds"),
        )

        logger.debug(
            "BigQuery job created: https://console.cloud.google.com/bigquery?project=%s&j=bq:%s:%s",
            self._query_job.project,
            self._query_job.location,
            self._query_job.job_id,
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
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """

        # The BigQuery Client's list_tables method does not support filtering by table name, so we have to
        # resort to using SQL instead.
        schema = to_schema(schema_name)
        catalog = schema.catalog or self.get_current_catalog()
        query = exp.select(
            exp.column("table_catalog").as_("catalog"),
            exp.column("table_name").as_("name"),
            exp.column("table_schema").as_("schema_name"),
            exp.case()
            .when(exp.column("table_type").eq("BASE TABLE"), exp.Literal.string("TABLE"))
            .when(exp.column("table_type").eq("CLONE"), exp.Literal.string("TABLE"))
            .when(exp.column("table_type").eq("EXTERNAL"), exp.Literal.string("TABLE"))
            .when(exp.column("table_type").eq("SNAPSHOT"), exp.Literal.string("TABLE"))
            .when(exp.column("table_type").eq("VIEW"), exp.Literal.string("VIEW"))
            .when(
                exp.column("table_type").eq("MATERIALIZED VIEW"),
                exp.Literal.string("MATERIALIZED_VIEW"),
            )
            .else_(exp.column("table_type"))
            .as_("type"),
        ).from_(
            exp.to_table(
                f"`{catalog}`.`{schema.db}`.INFORMATION_SCHEMA.TABLES", dialect=self.dialect
            )
        )
        if object_names:
            query = query.where(exp.column("table_name").isin(*object_names))

        try:
            df = self.fetchdf(query, quote_identifiers=True)
        except Exception as e:
            if "Not found" in str(e):
                return []
            raise

        if df.empty:
            return []
        return [
            DataObject(
                catalog=row.catalog,  # type: ignore
                schema=row.schema_name,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
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

    def _is_retryable(self, error: BaseException) -> bool:
        from google.api_core.exceptions import Forbidden

        if isinstance(error, self.retryable_errors):
            return True
        elif isinstance(error, Forbidden) and any(
            e["reason"] == "rateLimitExceeded" for e in error.errors
        ):
            return True
        return False

    def should_retry(self, error: BaseException) -> bool:
        if self.num_retries == 0:
            return False
        self.error_count += 1
        if self._is_retryable(error) and self.error_count <= self.num_retries:
            logger.info(f"Retry Num {self.error_count} of {self.num_retries}. Error: {repr(error)}")
            return True
        return False


def select_partitions_expr(
    schema: str,
    table_name: str,
    data_type: t.Union[str, exp.DataType],
    granularity: t.Optional[str] = None,
    agg_func: str = "MAX",
    database: t.Optional[str] = None,
) -> str:
    """Generates a SQL expression that aggregates partition values for a table.

    Args:
        schema: The schema (BigQueyr dataset) of the table.
        table_name: The name of the table.
        data_type: The data type of the partition column.
        granularity: The granularity of the partition. Supported values are: 'day', 'month', 'year' and 'hour'.
        agg_func: The aggregation function to use.
        database: The database (BigQuery project ID) of the table.

    Returns:
        A SELECT statement that aggregates partition values for a table.
    """
    partitions_table_name = f"`{schema}`.INFORMATION_SCHEMA.PARTITIONS"
    if database:
        partitions_table_name = f"`{database}`.{partitions_table_name}"

    if isinstance(data_type, exp.DataType):
        data_type = data_type.sql(dialect="bigquery")
    data_type = data_type.upper()

    parse_fun = f"PARSE_{data_type}" if data_type in ("DATE", "DATETIME", "TIMESTAMP") else None
    if parse_fun:
        granularity = granularity or "day"
        parse_format = GRANULARITY_TO_PARTITION_FORMAT[granularity.lower()]
        partition_expr = exp.func(
            parse_fun,
            exp.Literal.string(parse_format),
            exp.column("partition_id"),
            dialect="bigquery",
        )
    else:
        partition_expr = exp.cast(exp.column("partition_id"), "INT64", dialect="bigquery")

    return (
        exp.select(exp.func(agg_func, partition_expr))
        .from_(partitions_table_name, dialect="bigquery")
        .where(
            f"table_name = '{table_name}' AND partition_id IS NOT NULL AND partition_id != '__NULL__'",
            copy=False,
        )
        .sql(dialect="bigquery")
    )


GRANULARITY_TO_PARTITION_FORMAT = {
    "day": "%Y%m%d",
    "month": "%Y%m",
    "year": "%Y",
    "hour": "%Y%m%d%H",
}
