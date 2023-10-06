"""
# EngineAdapter

Engine adapters are how SQLMesh connects and interacts with various data stores. They allow SQLMesh to
generalize its functionality to different engines that have Python Database API 2.0-compliant
connections. Rather than executing queries directly against your data stores, SQLMesh components such as
the SnapshotEvaluator delegate them to engine adapters so these components can be engine-agnostic.
"""
from __future__ import annotations

import contextlib
import itertools
import logging
import sys
import types
import typing as t
import uuid
from datetime import datetime, timezone
from enum import Enum
from functools import partial

import pandas as pd
from sqlglot import Dialect, exp
from sqlglot.errors import ErrorLevel
from sqlglot.helper import ensure_list
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core.dialect import add_table, select_from_values_for_batch_range
from sqlmesh.core.engine_adapter.shared import DataObject
from sqlmesh.core.model.kind import TimeColumn
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.utils import double_escape
from sqlmesh.utils.connection_pool import create_connection_pool
from sqlmesh.utils.date import TimeLike, make_inclusive, to_ts
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pandas import columns_to_types_from_df

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import (
        DF,
        PySparkDataFrame,
        PySparkSession,
        Query,
        QueryOrDF,
    )
    from sqlmesh.core.node import IntervalUnit

logger = logging.getLogger(__name__)


MERGE_TARGET_ALIAS = "__MERGE_TARGET__"
MERGE_SOURCE_ALIAS = "__MERGE_SOURCE__"


class InsertOverwriteStrategy(Enum):
    DELETE_INSERT = 1
    INSERT_OVERWRITE = 2
    # Note: Replace where on Databricks requires that `spark.sql.sources.partitionOverwriteMode` be set to `static`
    REPLACE_WHERE = 3

    @property
    def is_delete_insert(self) -> bool:
        return self == InsertOverwriteStrategy.DELETE_INSERT

    @property
    def is_insert_overwrite(self) -> bool:
        return self == InsertOverwriteStrategy.INSERT_OVERWRITE

    @property
    def is_replace_where(self) -> bool:
        return self == InsertOverwriteStrategy.REPLACE_WHERE

    @property
    def requires_condition(self) -> bool:
        return self.is_replace_where or self.is_delete_insert


class SourceQuery:
    def __init__(
        self,
        query_factory: t.Callable[[], Query],
        cleanup_func: t.Optional[t.Callable[[], None]] = None,
        **kwargs: t.Any,
    ) -> None:
        self.query_factory = query_factory
        self.cleanup_func = cleanup_func

    def __enter__(self) -> Query:
        return self.query_factory()

    def __exit__(
        self,
        exc_type: t.Optional[t.Type[BaseException]],
        exc_val: t.Optional[BaseException],
        exc_tb: t.Optional[types.TracebackType],
    ) -> t.Optional[bool]:
        if self.cleanup_func:
            self.cleanup_func()
        return None


class EngineAdapter:
    """Base class wrapping a Database API compliant connection.

    The EngineAdapter is an easily-subclassable interface that interacts
    with the underlying engine and data store.

    Args:
        connection_factory: a callable which produces a new Database API-compliant
            connection on every call.
        dialect: The dialect with which this adapter is associated.
        multithreaded: Indicates whether this adapter will be used by more than one thread.
    """

    DIALECT = ""
    DEFAULT_BATCH_SIZE = 10000
    DEFAULT_SQL_GEN_KWARGS: t.Dict[str, str | bool | int] = {}
    ESCAPE_JSON = False
    SUPPORTS_TRANSACTIONS = True
    SUPPORTS_INDEXES = False
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT
    SUPPORTS_MATERIALIZED_VIEWS = False
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = False
    SUPPORTS_CLONING = False
    SCHEMA_DIFFER = SchemaDiffer()
    SUPPORTS_TUPLE_IN = True

    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        dialect: str = "",
        sql_gen_kwargs: t.Optional[t.Dict[str, Dialect | bool | str]] = None,
        multithreaded: bool = False,
        cursor_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
        **kwargs: t.Any,
    ):
        self.dialect = dialect.lower() or self.DIALECT
        self._connection_pool = create_connection_pool(
            connection_factory, multithreaded, cursor_kwargs=cursor_kwargs
        )
        self.sql_gen_kwargs = sql_gen_kwargs or {}
        self._extra_config = kwargs

    @property
    def cursor(self) -> t.Any:
        return self._connection_pool.get_cursor()

    @property
    def spark(self) -> t.Optional[PySparkSession]:
        return None

    @classmethod
    def is_pandas_df(cls, value: t.Any) -> bool:
        return isinstance(value, pd.DataFrame)

    @classmethod
    def _to_utc_timestamp(cls, col: t.Union[str, exp.Literal, exp.Column, exp.Null]) -> exp.Cast:
        def ensure_utc_exp(
            ts: t.Union[str, exp.Literal, exp.Column, exp.Null]
        ) -> t.Union[exp.Literal, exp.Column, exp.Null]:
            if not isinstance(ts, (str, exp.Literal)):
                return ts
            if isinstance(ts, exp.Literal):
                if not ts.is_string:
                    raise SQLMeshError("Timestamp literal must be a string")
                ts = ts.name
            return exp.Literal.string(
                datetime.fromisoformat(ts).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            )

        return exp.cast(ensure_utc_exp(col), "TIMESTAMP")

    @classmethod
    def _casted_columns(cls, columns_to_types: t.Dict[str, exp.DataType]) -> t.List[exp.Alias]:
        return [
            exp.alias_(exp.cast(column, to=kind), column, copy=False)
            for column, kind in columns_to_types.items()
        ]

    def _get_source_queries(
        self,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        target_table: TableName,
        *,
        batch_size: t.Optional[int] = None,
    ) -> t.List[SourceQuery]:
        batch_size = self.DEFAULT_BATCH_SIZE if batch_size is None else batch_size
        if isinstance(query_or_df, (exp.Subqueryable, exp.DerivedTable)):
            return [SourceQuery(query_factory=lambda: query_or_df)]  # type: ignore
        if not columns_to_types:
            raise SQLMeshError(
                "It is expected that if a DF is passed in then columns_to_types is set"
            )
        return self._df_to_source_queries(
            query_or_df, columns_to_types, batch_size, target_table=target_table
        )

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        assert isinstance(df, pd.DataFrame)
        num_rows = len(df.index)
        batch_size = sys.maxsize if batch_size == 0 else batch_size
        values = list(df.itertuples(index=False, name=None))
        return [
            SourceQuery(
                query_factory=partial(
                    self._values_to_sql,
                    values=values,
                    columns_to_types=columns_to_types,
                    batch_start=i,
                    batch_end=min(i + batch_size, num_rows),
                ),
            )
            for i in range(0, num_rows, batch_size)
        ]

    def _get_source_queries_and_columns_to_types(
        self,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        target_table: TableName,
        *,
        batch_size: t.Optional[int] = None,
    ) -> t.Tuple[t.List[SourceQuery], t.Optional[t.Dict[str, exp.DataType]]]:
        columns_to_types = self._columns_to_types(query_or_df, columns_to_types)
        return (
            self._get_source_queries(
                query_or_df, columns_to_types, target_table=target_table, batch_size=batch_size
            ),
            columns_to_types,
        )

    @t.overload
    def _columns_to_types(
        self, query_or_df: DF, columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    ) -> t.Dict[str, exp.DataType]:
        ...

    @t.overload
    def _columns_to_types(
        self, query_or_df: Query, columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    ) -> t.Optional[t.Dict[str, exp.DataType]]:
        ...

    def _columns_to_types(
        self, query_or_df: QueryOrDF, columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    ) -> t.Optional[t.Dict[str, exp.DataType]]:
        if columns_to_types:
            return columns_to_types
        if self.is_pandas_df(query_or_df):
            return columns_to_types_from_df(t.cast(pd.DataFrame, query_or_df))
        return columns_to_types

    def recycle(self) -> None:
        """Closes all open connections and releases all allocated resources associated with any thread
        except the calling one."""
        self._connection_pool.close_all(exclude_calling_thread=True)

    def close(self) -> t.Any:
        """Closes all open connections and releases all allocated resources."""
        self._connection_pool.close_all()

    def replace_query(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        **kwargs: t.Any,
    ) -> None:
        """Replaces an existing table with a query.

        For partition based engines (hive, spark), insert override is used. For other systems, create or replace is used.

        Args:
            table_name: The name of the table (eg. prod.table)
            query_or_df: The SQL query to run or a dataframe.
            columns_to_types: Only used if a dataframe is provided. A mapping between the column name and its data type.
                Expected to be ordered to match the order of values in the dataframe.
            kwargs: Optional create table properties.
        """
        table = exp.to_table(table_name)
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, target_table=table_name
        )
        return self._create_table_from_source_queries(
            table, source_queries, columns_to_types, replace=True, **kwargs
        )

    def create_index(
        self,
        table_name: TableName,
        index_name: str,
        columns: t.Tuple[str, ...],
        exists: bool = True,
    ) -> None:
        """Creates a new index for the given table if supported

        Args:
            table_name: The name of the target table.
            index_name: The name of the index.
            columns: The list of columns that constitute the index.
            exists: Indicates whether to include the IF NOT EXISTS check.
        """
        if not self.SUPPORTS_INDEXES:
            return

        expression = exp.Create(
            this=exp.Index(
                this=exp.to_identifier(index_name),
                table=exp.to_table(table_name),
                columns=[exp.to_column(c) for c in columns],
            ),
            kind="INDEX",
            exists=exists,
        )
        self.execute(expression)

    def create_table(
        self,
        table_name: TableName,
        columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
        exists: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """Create a table using a DDL statement

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            columns_to_types: A mapping between the column name and its data type.
            primary_key: Determines the table primary key.
            exists: Indicates whether to include the IF NOT EXISTS check.
            kwargs: Optional create table properties.
        """
        self._create_table_from_columns(table_name, columns_to_types, primary_key, exists, **kwargs)

    def ctas(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """Create a table using a CTAS statement

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            query_or_df: The SQL query to run or a dataframe for the CTAS.
            columns_to_types: A mapping between the column name and its data type. Required if using a DataFrame.
            exists: Indicates whether to include the IF NOT EXISTS check.
            kwargs: Optional create table properties.
        """
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, target_table=table_name
        )
        return self._create_table_from_source_queries(
            table_name, source_queries, columns_to_types, exists, **kwargs
        )

    def create_state_table(
        self,
        table_name: str,
        columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
    ) -> None:
        """Create a table to store SQLMesh internal state.

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            columns_to_types: A mapping between the column name and its data type.
            primary_key: Determines the table primary key.
        """
        self.create_table(
            table_name,
            columns_to_types,
            primary_key=primary_key,
        )

    def _create_table_from_columns(
        self,
        table_name: TableName,
        columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
        exists: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """
        Create a table using a DDL statement.

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            columns_to_types: Mapping between the column name and its data type.
            exists: Indicates whether to include the IF NOT EXISTS check.
            kwargs: Optional create table properties.
        """
        table = exp.to_table(table_name)
        primary_key_expression = (
            [exp.PrimaryKey(expressions=[exp.to_column(k) for k in primary_key])]
            if primary_key and self.SUPPORTS_INDEXES
            else []
        )
        schema = exp.Schema(
            this=table,
            expressions=[
                exp.ColumnDef(this=exp.to_identifier(column), kind=kind)
                for column, kind in columns_to_types.items()
            ]
            + primary_key_expression,
        )
        self._create_table(schema, None, exists=exists, columns_to_types=columns_to_types, **kwargs)

    def _create_table_from_source_queries(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        replace: bool = False,
        **kwargs: t.Any,
    ) -> None:
        table = exp.to_table(table_name)
        with self.transaction(condition=len(source_queries) > 1):
            for i, source_query in enumerate(source_queries):
                with source_query as query:
                    if i == 0:
                        self._create_table(
                            table,
                            query,
                            columns_to_types=columns_to_types,
                            exists=exists,
                            replace=replace,
                            **kwargs,
                        )
                    else:
                        self._insert_append_query(
                            table_name, query, columns_to_types or self.columns(table)
                        )

    def _create_table(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        **kwargs: t.Any,
    ) -> None:
        exists = False if replace else exists
        if not isinstance(table_name_or_schema, exp.Schema):
            table_name_or_schema = exp.to_table(table_name_or_schema)
        properties = (
            self._create_table_properties(**kwargs, columns_to_types=columns_to_types)
            if kwargs
            else None
        )
        create = exp.Create(
            this=table_name_or_schema,
            kind="TABLE",
            replace=replace,
            exists=exists,
            expression=expression,
            properties=properties,
        )
        self.execute(create)

    def create_table_like(
        self,
        target_table_name: TableName,
        source_table_name: TableName,
        exists: bool = True,
    ) -> None:
        """
        Create a table like another table or view.
        """
        target_table = exp.to_table(target_table_name)
        source_table = exp.to_table(source_table_name)
        create_expression = exp.Create(
            this=target_table,
            kind="TABLE",
            exists=exists,
            properties=exp.Properties(
                expressions=[
                    exp.LikeProperty(this=source_table),
                ]
            ),
        )
        self.execute(create_expression)

    def clone_table(
        self,
        target_table_name: TableName,
        source_table_name: TableName,
        replace: bool = False,
        clone_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
        **kwargs: t.Any,
    ) -> None:
        """Creates a table with the target name by cloning the source table.

        Args:
            target_table_name: The name of the table that should be created.
            source_table_name: The name of the source table that should be cloned.
            replace: Whether or not to replace an existing table.
        """
        if not self.SUPPORTS_CLONING:
            raise NotImplementedError(f"Engine does not support cloning: {type(self)}")
        self.execute(
            exp.Create(
                this=exp.to_table(target_table_name),
                kind="TABLE",
                replace=replace,
                clone=exp.Clone(this=exp.to_table(source_table_name), **(clone_kwargs or {})),
                **kwargs,
            )
        )

    def drop_table(self, table_name: TableName, exists: bool = True) -> None:
        """Drops a table.

        Args:
            table_name: The name of the table to drop.
            exists: If exists, defaults to True.
        """
        drop_expression = exp.Drop(this=exp.to_table(table_name), kind="TABLE", exists=exists)
        self.execute(drop_expression)

    def alter_table(
        self,
        current_table_name: TableName,
        target_table_name: TableName,
    ) -> None:
        """
        Performs the required alter statements to change the current table into the structure of the target table.
        """
        with self.transaction():
            for alter_expression in self.SCHEMA_DIFFER.compare_columns(
                current_table_name,
                self.columns(current_table_name),
                self.columns(target_table_name),
            ):
                self.execute(alter_expression)

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        **create_kwargs: t.Any,
    ) -> None:
        """Create a view with a query or dataframe.

        If a dataframe is passed in, it will be converted into a literal values statement.
        This should only be done if the dataframe is very small!

        Args:
            view_name: The view name.
            query_or_df: A query or dataframe.
            columns_to_types: Columns to use in the view statement.
            replace: Whether or not to replace an existing view defaults to True.
            materialized: Whether to create a a materialized view. Only used for engines that support this feature.
            create_kwargs: Additional kwargs to pass into the Create expression
        """
        if self.is_pandas_df(query_or_df):
            values = list(t.cast(pd.DataFrame, query_or_df).itertuples(index=False, name=None))
            columns_to_types = columns_to_types or self._columns_to_types(query_or_df)
            if not columns_to_types:
                raise SQLMeshError("columns_to_types must be provided for dataframes")
            query_or_df = self._values_to_sql(
                values,
                columns_to_types,
                batch_start=0,
                batch_end=len(values),
            )

        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, batch_size=0, target_table=view_name
        )
        if len(source_queries) != 1:
            raise SQLMeshError("Only one source query is supported for creating views")

        schema: t.Union[exp.Table, exp.Schema] = exp.to_table(view_name)
        if columns_to_types:
            schema = exp.Schema(
                this=exp.to_table(view_name),
                expressions=[exp.column(column) for column in columns_to_types],
            )

        properties = create_kwargs.pop("properties", None)
        if not properties:
            properties = exp.Properties(expressions=[])

        if materialized and self.SUPPORTS_MATERIALIZED_VIEWS:
            properties.append("expressions", exp.MaterializedProperty())

            if not self.SUPPORTS_MATERIALIZED_VIEW_SCHEMA and isinstance(schema, exp.Schema):
                schema = schema.this

        create_view_properties = self._create_view_properties(
            create_kwargs.pop("table_properties", None)
        )
        if create_view_properties:
            for view_property in create_view_properties.expressions:
                properties.append("expressions", view_property)

        if properties.expressions:
            create_kwargs["properties"] = properties

        with source_queries[0] as query:
            self.execute(
                exp.Create(
                    this=schema,
                    kind="VIEW",
                    replace=replace,
                    expression=query,
                    **create_kwargs,
                )
            )

    def create_schema(
        self,
        schema_name: str,
        catalog_name: t.Optional[str] = None,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
    ) -> None:
        """Create a schema from a name or qualified table name."""
        try:
            self.execute(
                exp.Create(
                    this=exp.table_(schema_name, catalog_name),
                    kind="SCHEMA",
                    exists=ignore_if_exists,
                )
            )
        except Exception as e:
            if not warn_on_error:
                raise
            logger.warning("Failed to create schema '%s': %s", schema_name, e)

    def drop_schema(
        self, schema_name: str, ignore_if_not_exists: bool = True, cascade: bool = False
    ) -> None:
        """Drop a schema from a name or qualified table name."""
        self.execute(
            exp.Drop(
                this=exp.to_identifier(schema_name.split(".")[0]),
                kind="SCHEMA",
                exists=ignore_if_not_exists,
                cascade=cascade,
            )
        )

    def drop_view(
        self, view_name: TableName, ignore_if_not_exists: bool = True, materialized: bool = False
    ) -> None:
        """Drop a view."""
        self.execute(
            exp.Drop(
                this=exp.to_table(view_name),
                exists=ignore_if_not_exists,
                materialized=materialized,
                kind="VIEW",
            )
        )

    def columns(
        self, table_name: TableName, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        """Fetches column names and types for the target table."""
        self.execute(exp.Describe(this=exp.to_table(table_name), kind="TABLE"))
        describe_output = self.cursor.fetchall()
        return {
            # Note: MySQL  returns the column type as bytes.
            column_name: exp.DataType.build(_decoded_str(column_type), dialect=self.dialect)
            for column_name, column_type, *_ in itertools.takewhile(
                lambda t: not t[0].startswith("#"),
                describe_output,
            )
        }

    def table_exists(self, table_name: TableName) -> bool:
        try:
            self.execute(exp.Describe(this=exp.to_table(table_name), kind="TABLE"))
            return True
        except Exception:
            return False

    def delete_from(self, table_name: TableName, where: t.Union[str, exp.Expression]) -> None:
        self.execute(exp.delete(table_name, where))

    def insert_append(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        contains_json: bool = False,
    ) -> None:
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, target_table=table_name
        )
        self._insert_append_source_queries(
            table_name, source_queries, columns_to_types, contains_json
        )

    @t.overload
    @classmethod
    def _escape_json(cls, value: Query) -> Query:
        ...

    @t.overload
    @classmethod
    def _escape_json(cls, value: str) -> str:
        ...

    @classmethod
    def _escape_json(cls, value: Query | str) -> Query | str:
        """
        Some engines need to add an extra escape to literals that contain JSON values. By default we don't do this
        though
        """
        if cls.ESCAPE_JSON:
            if isinstance(value, str):
                return double_escape(value)
            return value.transform(
                lambda e: exp.Literal.string(double_escape(e.name))
                if isinstance(e, exp.Literal) and e.args["is_string"]
                else e
            )
        return value

    def _insert_append_source_queries(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        contains_json: bool = False,
    ) -> None:
        with self.transaction(condition=len(source_queries) > 0):
            columns_to_types = columns_to_types or self.columns(table_name)
            for source_query in source_queries:
                with source_query as query:
                    self._insert_append_query(table_name, query, columns_to_types, contains_json)

    def _insert_append_query(
        self,
        table_name: TableName,
        query: Query,
        columns_to_types: t.Dict[str, exp.DataType],
        contains_json: bool = False,
        order_projections: bool = True,
    ) -> None:
        if contains_json:
            query = self._escape_json(query)
        if order_projections and query.named_selects != list(columns_to_types):
            if isinstance(query, exp.Subqueryable):
                query = query.subquery(alias="_ordered_projections")
            query = exp.select(*columns_to_types).from_(query)
        self.execute(exp.insert(query, table_name, columns=list(columns_to_types)))

    def insert_overwrite_by_partition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        partitioned_by: t.List[exp.Expression],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        raise NotImplementedError(
            "Insert Overwrite by Partition (not time) is not supported by this engine"
        )

    def insert_overwrite_by_time_partition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        start: TimeLike,
        end: TimeLike,
        time_formatter: t.Callable[
            [TimeLike, t.Optional[t.Dict[str, exp.DataType]]], exp.Expression
        ],
        time_column: TimeColumn | exp.Column | str,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        **kwargs: t.Any,
    ) -> None:
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, target_table=table_name
        )
        columns_to_types = columns_to_types or self.columns(table_name)
        low, high = [time_formatter(dt, columns_to_types) for dt in make_inclusive(start, end)]
        if isinstance(time_column, TimeColumn):
            time_column = time_column.column
        where = exp.Between(
            this=exp.to_column(time_column),
            low=low,
            high=high,
        )
        self._insert_overwrite_by_condition(table_name, source_queries, columns_to_types, where)

    @classmethod
    def _values_to_sql(
        cls,
        values: t.List[t.Tuple[t.Any, ...]],
        columns_to_types: t.Dict[str, exp.DataType],
        batch_start: int,
        batch_end: int,
        alias: str = "t",
        contains_json: bool = False,
    ) -> Query:
        query = select_from_values_for_batch_range(
            values=values,
            columns_to_types=columns_to_types,
            batch_start=batch_start,
            batch_end=batch_end,
            alias=alias,
        )
        if contains_json:
            query = t.cast(exp.Select, cls._escape_json(query))
        return query

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
    ) -> None:
        table = exp.to_table(table_name)
        if self.INSERT_OVERWRITE_STRATEGY.requires_condition and not where:
            raise SQLMeshError(
                "Where condition is required when doing a delete/insert or replace/where for insert/overwrite"
            )
        with self.transaction(
            condition=len(source_queries) > 0 or self.INSERT_OVERWRITE_STRATEGY.is_delete_insert
        ):
            columns_to_types = columns_to_types or self.columns(table_name)
            for i, source_query in enumerate(source_queries):
                with source_query as query:
                    query = self._add_where_to_query(query, where, columns_to_types)
                    if i > 0 or self.INSERT_OVERWRITE_STRATEGY.is_delete_insert:
                        if i == 0:
                            if not where:
                                raise SQLMeshError(
                                    "Where condition is required when doing a delete/insert"
                                )
                            assert where is not None
                            self.delete_from(table_name, where=where)
                        self._insert_append_query(
                            table_name,
                            query,
                            columns_to_types=columns_to_types,
                            order_projections=False,
                        )
                    else:
                        insert_exp = exp.insert(
                            query,
                            table,
                            # Change once Databricks supports REPLACE WHERE with columns
                            columns=list(columns_to_types)
                            if not self.INSERT_OVERWRITE_STRATEGY.is_replace_where
                            else None,
                            overwrite=self.INSERT_OVERWRITE_STRATEGY.is_insert_overwrite,
                        )
                        if self.INSERT_OVERWRITE_STRATEGY.is_replace_where:
                            insert_exp.set("where", where)
                        self.execute(insert_exp)

    def update_table(
        self,
        table_name: TableName,
        properties: t.Dict[str, t.Any],
        where: t.Optional[str | exp.Condition] = None,
        contains_json: bool = False,
    ) -> None:
        if contains_json and properties:
            properties = {
                k: self._escape_json(v)
                if isinstance(v, (str, exp.Subqueryable, exp.DerivedTable))
                else v
                for k, v in properties.items()
            }
        self.execute(exp.update(table_name, properties, where=where))

    def _merge(
        self,
        target_table: TableName,
        query: Query,
        on: exp.Expression,
        match_expressions: t.List[exp.When],
    ) -> None:
        this = exp.alias_(exp.to_table(target_table), alias=MERGE_TARGET_ALIAS, table=True)
        using = exp.alias_(
            exp.Subquery(this=query), alias=MERGE_SOURCE_ALIAS, copy=False, table=True
        )
        self.execute(
            exp.Merge(
                this=this,
                using=using,
                on=on,
                expressions=match_expressions,
            )
        )

    def scd_type_2(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        unique_key: t.Sequence[exp.Expression],
        valid_from_name: str,
        valid_to_name: str,
        updated_at_name: str,
        execution_time: TimeLike,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        **kwargs: t.Any,
    ) -> None:
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            source_table, columns_to_types, target_table=target_table, batch_size=0
        )
        columns_to_types = columns_to_types or self.columns(target_table)
        if valid_from_name not in columns_to_types or valid_to_name not in columns_to_types:
            columns_to_types = self.columns(target_table)
        if updated_at_name not in columns_to_types:
            raise SQLMeshError(
                f"Column {updated_at_name} not found in {target_table}. Table must contain an `updated_at` timestamp for SCD Type 2"
            )
        unmanaged_columns = [
            col for col in columns_to_types if col not in {valid_from_name, valid_to_name}
        ]
        with source_queries[0] as source_query:
            query = (
                exp.Select()  # type: ignore
                .with_(
                    "source",
                    exp.select(exp.true().as_("_exists"), *unmanaged_columns)
                    .distinct(*unique_key)
                    .from_(source_query.subquery("raw_source")),  # type: ignore
                )
                # Historical Records that Do Not Change
                .with_(
                    "static",
                    exp.select(*columns_to_types)
                    .from_(target_table)
                    .where(f"{valid_to_name} IS NOT NULL"),
                )
                # Latest Records that can be updated
                .with_(
                    "latest",
                    exp.select(*columns_to_types)
                    .from_(target_table)
                    .where(f"{valid_to_name} IS NULL"),
                )
                # Deleted records which can be used to determine `valid_from` for undeleted source records
                .with_(
                    "deleted",
                    exp.select(*[f"static.{col}" for col in columns_to_types])
                    .from_("static")
                    .join(
                        "latest",
                        on=exp.and_(
                            *[
                                add_table(key, "static").eq(add_table(key, "latest"))
                                for key in unique_key
                            ]
                        ),
                        join_type="left",
                    )
                    .where(f"latest.{valid_to_name} IS NULL"),
                )
                # Get the latest `valid_to` deleted record for each unique key
                .with_(
                    "latest_deleted",
                    exp.select(
                        exp.true().as_("_exists"),
                        *(part.as_(f"_key{i}") for i, part in enumerate(unique_key)),
                        f"MAX({valid_to_name}) AS {valid_to_name}",
                    )
                    .from_("deleted")
                    .group_by(*unique_key),
                )
                # Do a full join between latest records and source table in order to combine them together
                # MySQL doesn't suport full join so going to do a left then right join and remove dups with union
                .with_(
                    "joined",
                    exp.select(
                        exp.column("_exists", table="source"),
                        *(
                            exp.column(col, table="latest").as_(f"t_{col}")
                            for col in columns_to_types
                        ),
                        *(exp.column(col, table="source").as_(col) for col in unmanaged_columns),
                    )
                    .from_("latest")
                    .join(
                        "source",
                        on=exp.and_(
                            *[
                                add_table(key, "latest").eq(add_table(key, "source"))
                                for key in unique_key
                            ]
                        ),
                        join_type="left",
                    )
                    .union(
                        exp.select(
                            exp.column("_exists", table="source"),
                            *(
                                exp.column(col, table="latest").as_(f"t_{col}")
                                for col in columns_to_types
                            ),
                            *(
                                exp.column(col, table="source").as_(col)
                                for col in unmanaged_columns
                            ),
                        )
                        .from_("latest")
                        .join(
                            "source",
                            on=exp.and_(
                                *[
                                    add_table(key, "latest").eq(add_table(key, "source"))
                                    for key in unique_key
                                ]
                            ),
                            join_type="right",
                        )
                    ),
                )
                # Get deleted, new, no longer current, or unchanged records
                .with_(
                    "updated_rows",
                    exp.select(
                        *(
                            exp.func(
                                "COALESCE",
                                exp.column(f"t_{col}", table="joined"),
                                exp.column(col, table="joined"),
                            ).as_(col)
                            for col in unmanaged_columns
                        ),
                        f"""
                        CASE
                            WHEN t_{valid_from_name} IS NULL
                                 AND latest_deleted._exists IS NOT NULL
                            THEN CASE
                                    WHEN latest_deleted.{valid_to_name} > {updated_at_name}
                                    THEN latest_deleted.{valid_to_name}
                                    ELSE {updated_at_name}
                                 END
                            WHEN t_{valid_from_name} IS NULL
                            THEN {self._to_utc_timestamp('1970-01-01 00:00:00+00:00')}
                            ELSE t_{valid_from_name}
                        END AS {valid_from_name}""",
                        f"""
                        CASE
                            WHEN {updated_at_name} > t_{updated_at_name}
                            THEN {updated_at_name}
                            WHEN joined._exists IS NULL
                            THEN {self._to_utc_timestamp(to_ts(execution_time))}
                            ELSE t_{valid_to_name}
                        END AS {valid_to_name}""",
                    )
                    .from_("joined")
                    .join(
                        "latest_deleted",
                        on=exp.and_(
                            *[
                                add_table(part, "joined").eq(
                                    exp.column(f"_key{i}", "latest_deleted")
                                )
                                for i, part in enumerate(unique_key)
                            ]
                        ),
                        join_type="left",
                    ),
                )
                # Get records that have been "updated" which means inserting a new record with previous `valid_from`
                .with_(
                    "inserted_rows",
                    exp.select(
                        *unmanaged_columns,
                        f"{updated_at_name} as {valid_from_name}",
                        f"{self._to_utc_timestamp(exp.null())} as {valid_to_name}",
                    )
                    .from_("joined")
                    .where(f"{updated_at_name} > t_{updated_at_name}"),
                )
                .select("*")
                .from_("static")
                .union(
                    "SELECT * FROM updated_rows",
                    distinct=False,
                )
                .union(
                    "SELECT * FROM inserted_rows",
                    distinct=False,
                )
            )
            self.replace_query(
                target_table,
                query,
                columns_to_types=columns_to_types,
            )

    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expression],
    ) -> None:
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            source_table, columns_to_types, target_table=target_table
        )
        columns_to_types = columns_to_types or self.columns(target_table)
        on = exp.and_(
            *(
                add_table(part, MERGE_TARGET_ALIAS).eq(add_table(part, MERGE_SOURCE_ALIAS))
                for part in unique_key
            )
        )
        when_matched = exp.When(
            matched=True,
            source=False,
            then=exp.Update(
                expressions=[
                    exp.column(col, MERGE_TARGET_ALIAS).eq(exp.column(col, MERGE_SOURCE_ALIAS))
                    for col in columns_to_types
                ],
            ),
        )
        when_not_matched = exp.When(
            matched=False,
            source=False,
            then=exp.Insert(
                this=exp.Tuple(expressions=[exp.column(col) for col in columns_to_types]),
                expression=exp.Tuple(
                    expressions=[exp.column(col, MERGE_SOURCE_ALIAS) for col in columns_to_types]
                ),
            ),
        )
        for source_query in source_queries:
            with source_query as query:
                self._merge(
                    target_table=target_table,
                    query=query,
                    on=on,
                    match_expressions=[when_matched, when_not_matched],
                )

    def rename_table(
        self,
        old_table_name: TableName,
        new_table_name: TableName,
    ) -> None:
        self.execute(exp.rename_table(old_table_name, new_table_name))

    def fetchone(
        self,
        query: t.Union[exp.Expression, str],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = False,
    ) -> t.Tuple:
        self.execute(
            query,
            ignore_unsupported_errors=ignore_unsupported_errors,
            quote_identifiers=quote_identifiers,
        )
        return self.cursor.fetchone()

    def fetchall(
        self,
        query: t.Union[exp.Expression, str],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = False,
    ) -> t.List[t.Tuple]:
        self.execute(
            query,
            ignore_unsupported_errors=ignore_unsupported_errors,
            quote_identifiers=quote_identifiers,
        )
        return self.cursor.fetchall()

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        """Fetches a DataFrame that can be either Pandas or PySpark from the cursor"""
        self.execute(query, quote_identifiers=quote_identifiers)
        return self.cursor.fetchdf()

    def fetchdf(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> pd.DataFrame:
        """Fetches a Pandas DataFrame from the cursor"""
        df = self._fetch_native_df(query, quote_identifiers=quote_identifiers)
        if not isinstance(df, pd.DataFrame):
            raise NotImplementedError(
                "The cursor's `fetch_native_df` method is not returning a pandas DataFrame. Need to update `fetchdf` so a Pandas DataFrame is returned"
            )
        return df

    def fetch_pyspark_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> PySparkDataFrame:
        """Fetches a PySpark DataFrame from the cursor"""
        raise NotImplementedError(f"Engine does not support PySpark DataFrames: {type(self)}")

    @contextlib.contextmanager
    def transaction(
        self,
        condition: t.Optional[bool] = None,
    ) -> t.Iterator[None]:
        """A transaction context manager."""
        if (
            self._connection_pool.is_transaction_active
            or not self.SUPPORTS_TRANSACTIONS
            or (condition is not None and not condition)
        ):
            yield
            return
        self._connection_pool.begin()
        try:
            yield
        except Exception as e:
            self._connection_pool.rollback()
            raise e
        else:
            self._connection_pool.commit()

    @contextlib.contextmanager
    def session(self) -> t.Iterator[None]:
        """A session context manager."""
        if self._is_session_active():
            yield
            return

        self._begin_session()
        try:
            yield
        finally:
            self._end_session()

    def _begin_session(self) -> None:
        """Begin a new session."""

    def _end_session(self) -> None:
        """End the existing session."""

    def _is_session_active(self) -> bool:
        """Indicates whether or not a session is active."""
        return False

    def execute(
        self,
        expressions: t.Union[str, exp.Expression, t.Sequence[exp.Expression]],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """Execute a sql query."""
        to_sql_kwargs = (
            {"unsupported_level": ErrorLevel.IGNORE} if ignore_unsupported_errors else {}
        )

        with self.transaction():
            for e in ensure_list(expressions):
                sql = (
                    self._to_sql(e, quote=quote_identifiers, **to_sql_kwargs)
                    if isinstance(e, exp.Expression)
                    else e
                )
                logger.debug(f"Executing SQL:\n{sql}")
                self.cursor.execute(sql, **kwargs)

    @contextlib.contextmanager
    def temp_table(
        self,
        query_or_df: QueryOrDF,
        name: TableName = "diff",
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        **kwargs: t.Any,
    ) -> t.Iterator[exp.Table]:
        """A context manager for working a temp table.

        The table will be created with a random guid and cleaned up after the block.

        Args:
            query_or_df: The query or df to create a temp table for.
            name: The base name of the temp table.
            columns_to_types: A mapping between the column name and its data type.

        Yields:
            The table expression
        """
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types=columns_to_types, target_table=name
        )
        with self.transaction():
            table = self._get_temp_table(name)
            if table.db:
                self.create_schema(table.db)
            self._create_table_from_source_queries(
                table, source_queries, columns_to_types, exists=True, **kwargs
            )

            try:
                yield table
            finally:
                self.drop_table(table)

    def _create_table_properties(
        self,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[str]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for ddl."""
        return None

    def _create_view_properties(
        self,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for view"""
        return None

    def _to_sql(self, expression: exp.Expression, quote: bool = True, **kwargs: t.Any) -> str:
        """
        Converts an expression to a SQL string. Has a set of default kwargs to apply, and then default
        kwargs defined for the given dialect, and then kwargs provided by the user when defining the engine
        adapter, and then finally kwargs provided by the user when calling this method.
        """
        sql_gen_kwargs = {
            "dialect": self.dialect,
            "pretty": False,
            "comments": False,
            **self.DEFAULT_SQL_GEN_KWARGS,
            **self.sql_gen_kwargs,
            **kwargs,
        }

        if quote:
            quote_identifiers(expression)

        return expression.sql(**sql_gen_kwargs)  # type: ignore

    def _get_data_objects(
        self, schema_name: str, catalog_name: t.Optional[str] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """

        raise NotImplementedError()

    def _get_temp_table(
        self,
        table: TableName,
        table_only: bool = False,
    ) -> exp.Table:
        """
        Returns the name of the temp table that should be used for the given table name.
        """
        table = t.cast(exp.Table, exp.to_table(table).copy())
        table.set("this", exp.to_identifier(f"__temp_{table.name}_{uuid.uuid4().hex}"))

        if table_only:
            table.set("db", None)
            table.set("catalog", None)

        return table

    def _add_where_to_query(
        self,
        query: Query,
        where: t.Optional[exp.Expression],
        columns_to_type: t.Dict[str, exp.DataType],
    ) -> Query:
        if not where or not isinstance(query, exp.Subqueryable):
            return query

        query = t.cast(exp.Subqueryable, query.copy())
        with_ = query.args.pop("with", None)
        query = (
            exp.select(*columns_to_type, copy=False)
            .from_(query.subquery("_subquery", copy=False), copy=False)
            .where(where, copy=False)
        )

        if with_:
            query.set("with", with_)

        return query

    def _truncate_table(self, table_name: TableName) -> str:
        table = quote_identifiers(exp.to_table(table_name))
        return f"TRUNCATE {table.sql(dialect=self.dialect)}"


class EngineAdapterWithIndexSupport(EngineAdapter):
    SUPPORTS_INDEXES = True


def _decoded_str(value: t.Union[str, bytes]) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value
