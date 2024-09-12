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
import typing as t
from functools import partial

import pandas as pd
from sqlglot import Dialect, exp
from sqlglot.errors import ErrorLevel
from sqlglot.helper import ensure_list
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core.dialect import (
    add_table,
    schema_,
    select_from_values_for_batch_range,
    to_schema,
)
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    EngineRunMode,
    InsertOverwriteStrategy,
    SourceQuery,
    set_catalog,
)
from sqlmesh.core.model.kind import TimeColumn
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.utils import columns_to_types_all_known, random_id
from sqlmesh.utils.connection_pool import create_connection_pool
from sqlmesh.utils.date import TimeLike, make_inclusive, to_time_column
from sqlmesh.utils.errors import SQLMeshError, UnsupportedCatalogOperationError
from sqlmesh.utils.pandas import columns_to_types_from_df

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, SessionProperties, TableName
    from sqlmesh.core.engine_adapter._typing import (
        DF,
        PySparkDataFrame,
        PySparkSession,
        Query,
        QueryOrDF,
        SnowparkSession,
    )
    from sqlmesh.core.node import IntervalUnit

logger = logging.getLogger(__name__)

MERGE_TARGET_ALIAS = "__MERGE_TARGET__"
MERGE_SOURCE_ALIAS = "__MERGE_SOURCE__"


@set_catalog()
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
    DATA_OBJECT_FILTER_BATCH_SIZE = 4000
    SUPPORTS_TRANSACTIONS = True
    SUPPORTS_INDEXES = False
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_AND_COMMANDS
    MAX_TABLE_COMMENT_LENGTH: t.Optional[int] = None
    MAX_COLUMN_COMMENT_LENGTH: t.Optional[int] = None
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT
    SUPPORTS_MATERIALIZED_VIEWS = False
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = False
    SUPPORTS_VIEW_SCHEMA = True
    SUPPORTS_CLONING = False
    SUPPORTS_MANAGED_MODELS = False
    SCHEMA_DIFFER = SchemaDiffer()
    SUPPORTS_TUPLE_IN = True
    CATALOG_SUPPORT = CatalogSupport.UNSUPPORTED
    HAS_VIEW_BINDING = False
    SUPPORTS_REPLACE_TABLE = True
    DEFAULT_CATALOG_TYPE = DIALECT
    QUOTE_IDENTIFIERS_IN_VIEWS = True

    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        dialect: str = "",
        sql_gen_kwargs: t.Optional[t.Dict[str, Dialect | bool | str]] = None,
        multithreaded: bool = False,
        cursor_kwargs: t.Optional[t.Dict[str, t.Any]] = None,
        cursor_init: t.Optional[t.Callable[[t.Any], None]] = None,
        default_catalog: t.Optional[str] = None,
        execute_log_level: int = logging.DEBUG,
        register_comments: bool = True,
        pre_ping: bool = False,
        **kwargs: t.Any,
    ):
        self.dialect = dialect.lower() or self.DIALECT
        self._connection_pool = create_connection_pool(
            connection_factory, multithreaded, cursor_kwargs=cursor_kwargs, cursor_init=cursor_init
        )
        self._sql_gen_kwargs = sql_gen_kwargs or {}
        self._default_catalog = default_catalog
        self._execute_log_level = execute_log_level
        self._extra_config = kwargs
        self._register_comments = register_comments
        self._pre_ping = pre_ping

    def with_log_level(self, level: int) -> EngineAdapter:
        adapter = self.__class__(
            lambda: None,
            dialect=self.dialect,
            sql_gen_kwargs=self._sql_gen_kwargs,
            default_catalog=self._default_catalog,
            execute_log_level=level,
            register_comments=self._register_comments,
            **self._extra_config,
        )

        adapter._connection_pool = self._connection_pool

        return adapter

    @property
    def cursor(self) -> t.Any:
        return self._connection_pool.get_cursor()

    @property
    def connection(self) -> t.Any:
        return self._connection_pool.get()

    @property
    def spark(self) -> t.Optional[PySparkSession]:
        return None

    @property
    def snowpark(self) -> t.Optional[SnowparkSession]:
        return None

    @property
    def comments_enabled(self) -> bool:
        return self._register_comments and self.COMMENT_CREATION_TABLE.is_supported

    @classmethod
    def _casted_columns(cls, columns_to_types: t.Dict[str, exp.DataType]) -> t.List[exp.Alias]:
        return [
            exp.alias_(exp.cast(exp.column(column), to=kind), column, copy=False)
            for column, kind in columns_to_types.items()
        ]

    @property
    def default_catalog(self) -> t.Optional[str]:
        if self.CATALOG_SUPPORT.is_unsupported:
            return None
        default_catalog = self._default_catalog or self.get_current_catalog()
        if not default_catalog:
            raise SQLMeshError("Could not determine a default catalog despite it being supported.")
        return default_catalog

    @property
    def engine_run_mode(self) -> EngineRunMode:
        return EngineRunMode.SINGLE_MODE_ENGINE

    def _get_source_queries(
        self,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        target_table: TableName,
        *,
        batch_size: t.Optional[int] = None,
    ) -> t.List[SourceQuery]:
        batch_size = self.DEFAULT_BATCH_SIZE if batch_size is None else batch_size
        if isinstance(query_or_df, (exp.Query, exp.DerivedTable)):
            return [SourceQuery(query_factory=lambda: query_or_df)]  # type: ignore

        if not columns_to_types:
            raise SQLMeshError(
                "It is expected that if a DataFrame is passed in then columns_to_types is set"
            )

        if isinstance(query_or_df, pd.DataFrame) and query_or_df.empty:
            raise SQLMeshError(
                "Cannot construct source query from an empty DataFrame. This error is commonly "
                "related to Python models that produce no data. For such models, consider yielding "
                "from an empty generator if the resulting set is empty, i.e. use `yield from ()`."
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
    ) -> t.Dict[str, exp.DataType]: ...

    @t.overload
    def _columns_to_types(
        self, query_or_df: Query, columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    ) -> t.Optional[t.Dict[str, exp.DataType]]: ...

    def _columns_to_types(
        self, query_or_df: QueryOrDF, columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    ) -> t.Optional[t.Dict[str, exp.DataType]]:
        if columns_to_types:
            return columns_to_types
        if isinstance(query_or_df, pd.DataFrame):
            return columns_to_types_from_df(t.cast(pd.DataFrame, query_or_df))
        return columns_to_types

    def recycle(self) -> None:
        """Closes all open connections and releases all allocated resources associated with any thread
        except the calling one."""
        self._connection_pool.close_all(exclude_calling_thread=True)

    def close(self) -> t.Any:
        """Closes all open connections and releases all allocated resources."""
        self._connection_pool.close_all()

    def get_current_catalog(self) -> t.Optional[str]:
        """Returns the catalog name of the current connection."""
        raise NotImplementedError()

    def set_current_catalog(self, catalog: str) -> None:
        """Sets the catalog name of the current connection."""
        raise NotImplementedError()

    def get_catalog_type(self, catalog: t.Optional[str]) -> str:
        """Intended to be overridden for data virtualization systems like Trino that,
        depending on the target catalog, require slightly different properties to be set when creating / updating tables
        """
        if self.CATALOG_SUPPORT.is_unsupported:
            raise UnsupportedCatalogOperationError(
                f"{self.dialect} does not support catalogs and a catalog was provided: {catalog}"
            )
        return self.DEFAULT_CATALOG_TYPE

    @property
    def current_catalog_type(self) -> str:
        return self.get_catalog_type(self.get_current_catalog())

    def replace_query(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
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
        target_table = exp.to_table(table_name)
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, target_table=target_table
        )
        columns_to_types = columns_to_types or self.columns(target_table)
        query = source_queries[0].query_factory()
        self_referencing = any(
            quote_identifiers(table) == quote_identifiers(target_table)
            for table in query.find_all(exp.Table)
        )
        # If a query references itself then it must have a table created regardless of approach used.
        if self_referencing:
            self._create_table_from_columns(
                target_table,
                columns_to_types,
                exists=True,
                table_description=table_description,
                column_descriptions=column_descriptions,
            )
        # All engines support `CREATE TABLE AS` so we use that if the table doesn't already exist and we
        # use `CREATE OR REPLACE TABLE AS` if the engine supports it
        if self.SUPPORTS_REPLACE_TABLE or not self.table_exists(target_table):
            return self._create_table_from_source_queries(
                target_table,
                source_queries,
                columns_to_types,
                replace=self.SUPPORTS_REPLACE_TABLE,
                table_description=table_description,
                column_descriptions=column_descriptions,
                **kwargs,
            )
        else:
            if self_referencing:
                with self.temp_table(
                    self._select_columns(columns_to_types).from_(target_table),
                    name=target_table,
                    columns_to_types=columns_to_types,
                    **kwargs,
                ) as temp_table:
                    for source_query in source_queries:
                        source_query.add_transform(
                            lambda node: (  # type: ignore
                                temp_table  # type: ignore
                                if isinstance(node, exp.Table)
                                and quote_identifiers(node) == quote_identifiers(target_table)
                                else node
                            )
                        )
                    return self._insert_overwrite_by_condition(
                        target_table,
                        source_queries,
                        columns_to_types,
                    )
            return self._insert_overwrite_by_condition(
                target_table,
                source_queries,
                columns_to_types,
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
                params=exp.IndexParameters(columns=[exp.to_column(c) for c in columns]),
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
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """Create a table using a DDL statement

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            columns_to_types: A mapping between the column name and its data type.
            primary_key: Determines the table primary key.
            exists: Indicates whether to include the IF NOT EXISTS check.
            table_description: Optional table description from MODEL DDL.
            column_descriptions: Optional column descriptions from model query.
            kwargs: Optional create table properties.
        """
        self._create_table_from_columns(
            table_name,
            columns_to_types,
            primary_key,
            exists,
            table_description,
            column_descriptions,
            **kwargs,
        )

    def create_managed_table(
        self,
        table_name: TableName,
        query: Query,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        clustered_by: t.Optional[t.List[str]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """Create a managed table using a query.

        "Managed" means that once the table is created, the data is kept up to date by the underlying database engine and not SQLMesh.

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            query: The SQL query for the engine to base the managed table on
            columns_to_types: A mapping between the column name and its data type.
            partitioned_by: The partition columns or engine specific expressions, only applicable in certain engines. (eg. (ds, hour))
            clustered_by: The cluster columns, only applicable in certain engines. (eg. (ds, hour))
            table_properties: Optional mapping of engine-specific properties to be set on the managed table
            table_description: Optional table description from MODEL DDL.
            column_descriptions: Optional column descriptions from model query.
            kwargs: Optional create table properties.
        """
        raise NotImplementedError(f"Engine does not support managed tables: {type(self)}")

    def ctas(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """Create a table using a CTAS statement

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            query_or_df: The SQL query to run or a dataframe for the CTAS.
            columns_to_types: A mapping between the column name and its data type. Required if using a DataFrame.
            exists: Indicates whether to include the IF NOT EXISTS check.
            table_description: Optional table description from MODEL DDL.
            column_descriptions: Optional column descriptions from model query.
            kwargs: Optional create table properties.
        """
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, target_table=table_name
        )
        return self._create_table_from_source_queries(
            table_name,
            source_queries,
            columns_to_types,
            exists,
            table_description=table_description,
            column_descriptions=column_descriptions,
            **kwargs,
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
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Create a table using a DDL statement.

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            columns_to_types: Mapping between the column name and its data type.
            primary_key: Determines the table primary key.
            exists: Indicates whether to include the IF NOT EXISTS check.
            table_description: Optional table description from MODEL DDL.
            column_descriptions: Optional column descriptions from model query.
            kwargs: Optional create table properties.
        """
        table = exp.to_table(table_name)

        if not columns_to_types_all_known(columns_to_types):
            # It is ok if the columns types are not known if the table already exists and IF NOT EXISTS is set
            if exists and self.table_exists(table_name):
                return
            raise SQLMeshError(
                "Cannot create a table without knowing the column types. "
                "Try casting the columns to an expected type or defining the columns in the model metadata. "
                f"Columns to types: {columns_to_types}"
            )

        primary_key_expression = (
            [exp.PrimaryKey(expressions=[exp.to_column(k) for k in primary_key])]
            if primary_key and self.SUPPORTS_INDEXES
            else []
        )

        schema = self._build_schema_exp(
            table,
            columns_to_types,
            column_descriptions,
            primary_key_expression,
        )

        self._create_table(
            schema,
            None,
            exists=exists,
            columns_to_types=columns_to_types,
            table_description=table_description,
            **kwargs,
        )

        # Register comments with commands if the engine doesn't support comments in the schema or CREATE
        if (
            table_description
            and self.COMMENT_CREATION_TABLE.is_comment_command_only
            and self.comments_enabled
        ):
            self._create_table_comment(table_name, table_description)
        if (
            column_descriptions
            and self.COMMENT_CREATION_TABLE.is_comment_command_only
            and self.comments_enabled
        ):
            self._create_column_comments(table_name, column_descriptions)

    def _build_schema_exp(
        self,
        table: exp.Table,
        columns_to_types: t.Dict[str, exp.DataType],
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        expressions: t.Optional[t.List[exp.PrimaryKey]] = None,
        is_view: bool = False,
    ) -> exp.Schema:
        """
        Build a schema expression for a table, columns, column comments, and additional schema properties.
        """
        expressions = expressions or []
        engine_supports_schema_comments = (
            self.COMMENT_CREATION_VIEW.supports_schema_def
            if is_view
            else self.COMMENT_CREATION_TABLE.supports_schema_def
        )
        return exp.Schema(
            this=table,
            expressions=[
                exp.ColumnDef(
                    this=exp.to_identifier(column),
                    kind=None if is_view else kind,  # don't include column data type for views
                    constraints=(
                        self._build_col_comment_exp(column, column_descriptions)
                        if column_descriptions
                        and engine_supports_schema_comments
                        and self.comments_enabled
                        else None
                    ),
                )
                for column, kind in columns_to_types.items()
            ]
            + expressions,
        )

    def _build_col_comment_exp(
        self, col_name: str, column_descriptions: t.Dict[str, str]
    ) -> t.List[exp.ColumnConstraint]:
        comment = column_descriptions.get(col_name, None)
        if comment:
            return [
                exp.ColumnConstraint(
                    kind=exp.CommentColumnConstraint(
                        this=exp.Literal.string(self._truncate_column_comment(comment))
                    )
                )
            ]
        return []

    def _create_table_from_source_queries(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        replace: bool = False,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        table = exp.to_table(table_name)

        # CTAS calls do not usually include a schema expression. However, most engines
        # permit them in CTAS expressions, and they allow us to register all column comments
        # in a single call rather than in a separate comment command call for each column.
        #
        # This block conditionally builds a schema expression with column comments if the engine
        # supports it and we have columns_to_types. column_to_types is required because the
        # schema expression must include at least column name, data type, and the comment -
        # for example, `(colname INTEGER COMMENT 'comment')`.
        #
        # column_to_types will be available when loading from a DataFrame (by converting from
        # pandas to SQL types), when a model is "annotated" by explicitly specifying column
        # types, and for evaluation methods like `LogicalReplaceQueryMixin.replace_query()`
        # calls and SCD Type 2 model calls.
        schema = None
        columns_to_types_known = columns_to_types and columns_to_types_all_known(columns_to_types)
        if (
            column_descriptions
            and columns_to_types_known
            and self.COMMENT_CREATION_TABLE.is_in_schema_def_ctas
            and self.comments_enabled
        ):
            schema = self._build_schema_exp(table, columns_to_types, column_descriptions)  # type: ignore

        with self.transaction(condition=len(source_queries) > 1):
            for i, source_query in enumerate(source_queries):
                with source_query as query:
                    if columns_to_types and columns_to_types_known:
                        query = self._order_projections_and_filter(
                            query, columns_to_types, coerce_types=True
                        )
                    if i == 0:
                        self._create_table(
                            schema if schema else table,
                            query,
                            columns_to_types=columns_to_types,
                            exists=exists,
                            replace=replace,
                            table_description=table_description,
                            table_kind=table_kind,
                            **kwargs,
                        )
                    else:
                        self._insert_append_query(
                            table_name, query, columns_to_types or self.columns(table)
                        )

        # Register comments with commands if the engine supports comments and we weren't able to
        # register them with the CTAS call's schema expression.
        if (
            table_description
            and self.COMMENT_CREATION_TABLE.is_comment_command_only
            and self.comments_enabled
        ):
            self._create_table_comment(table_name, table_description)
        if column_descriptions and schema is None and self.comments_enabled:
            self._create_column_comments(table_name, column_descriptions)

    def _create_table(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        self.execute(
            self._build_create_table_exp(
                table_name_or_schema,
                expression=expression,
                exists=exists,
                replace=replace,
                columns_to_types=columns_to_types,
                table_description=(
                    table_description
                    if self.COMMENT_CREATION_TABLE.supports_schema_def and self.comments_enabled
                    else None
                ),
                table_kind=table_kind,
                **kwargs,
            )
        )

    def _build_create_table_exp(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> exp.Create:
        exists = False if replace else exists
        catalog_name = None
        if not isinstance(table_name_or_schema, exp.Schema):
            table_name_or_schema = exp.to_table(table_name_or_schema)
            catalog_name = table_name_or_schema.catalog
        else:
            if isinstance(table_name_or_schema.this, exp.Table):
                catalog_name = table_name_or_schema.this.catalog

        properties = (
            self._build_table_properties_exp(
                **kwargs,
                catalog_name=catalog_name,
                columns_to_types=columns_to_types,
                table_description=table_description,
                table_kind=table_kind,
            )
            if kwargs or table_description
            else None
        )
        return exp.Create(
            this=table_name_or_schema,
            kind=table_kind or "TABLE",
            replace=replace,
            exists=exists,
            expression=expression,
            properties=properties,
        )

    def create_table_like(
        self,
        target_table_name: TableName,
        source_table_name: TableName,
        exists: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """Create a table to store SQLMesh internal state based on the definition of another table, including any
        column attributes and indexes defined in the original table.

        Args:
            target_table_name: The name of the table to create. Can be fully qualified or just table name.
            source_table_name: The name of the table to base the new table on.
        """
        self.create_table(target_table_name, self.columns(source_table_name), exists=exists)

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
                clone=exp.Clone(
                    this=exp.to_table(source_table_name),
                    **(clone_kwargs or {}),
                ),
                **kwargs,
            )
        )

    def drop_table(self, table_name: TableName, exists: bool = True) -> None:
        """Drops a table.

        Args:
            table_name: The name of the table to drop.
            exists: If exists, defaults to True.
        """
        self._drop_object(name=table_name, exists=exists)

    def drop_managed_table(self, table_name: TableName, exists: bool = True) -> None:
        """Drops a managed table.

        Args:
            table_name: The name of the table to drop.
            exists: If exists, defaults to True.
        """
        raise NotImplementedError(f"Engine does not support managed tables: {type(self)}")

    def _drop_object(
        self,
        name: TableName | SchemaName,
        exists: bool = True,
        kind: str = "TABLE",
        **drop_args: t.Any,
    ) -> None:
        """Drops an object.

        An object could be a DATABASE, SCHEMA, VIEW, TABLE, DYNAMIC TABLE, TEMPORARY TABLE etc depending on the :kind.

        Args:
            name: The name of the table to drop.
            exists: If exists, defaults to True.
            kind: What kind of object to drop. Defaults to TABLE
            **drop_args: Any extra arguments to set on the Drop expression
        """
        self.execute(exp.Drop(this=exp.to_table(name), kind=kind, exists=exists, **drop_args))

    def get_alter_expressions(
        self,
        current_table_name: TableName,
        target_table_name: TableName,
    ) -> t.List[exp.Alter]:
        """
        Determines the alter statements needed to change the current table into the structure of the target table.
        """
        return self.SCHEMA_DIFFER.compare_columns(
            current_table_name,
            self.columns(current_table_name),
            self.columns(target_table_name),
        )

    def alter_table(
        self,
        alter_expressions: t.List[exp.Alter],
    ) -> None:
        """
        Performs the alter statements to change the current table into the structure of the target table.
        """
        with self.transaction():
            for alter_expression in alter_expressions:
                self.execute(alter_expression)

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
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
            materialized_properties: Optional materialized view properties to add to the view.
            table_description: Optional table description from MODEL DDL.
            column_descriptions: Optional column descriptions from model query.
            view_properties: Optional view properties to add to the view.
            create_kwargs: Additional kwargs to pass into the Create expression
        """
        if materialized_properties and not materialized:
            raise SQLMeshError("Materialized properties are only supported for materialized views")

        if isinstance(query_or_df, pd.DataFrame):
            values: t.List[t.Tuple[t.Any, ...]] = list(
                query_or_df.itertuples(index=False, name=None)
            )
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
            schema = self._build_schema_exp(
                exp.to_table(view_name), columns_to_types, column_descriptions, is_view=True
            )

        properties = create_kwargs.pop("properties", None)
        if not properties:
            properties = exp.Properties(expressions=[])

        if materialized and self.SUPPORTS_MATERIALIZED_VIEWS:
            properties.append("expressions", exp.MaterializedProperty())

            if not self.SUPPORTS_MATERIALIZED_VIEW_SCHEMA and isinstance(schema, exp.Schema):
                schema = schema.this

        if not self.SUPPORTS_VIEW_SCHEMA and isinstance(schema, exp.Schema):
            schema = schema.this

        if materialized_properties:
            partitioned_by = materialized_properties.pop("partitioned_by", None)
            clustered_by = materialized_properties.pop("clustered_by", None)
            if partitioned_by and (
                partitioned_by_prop := self._build_partitioned_by_exp(
                    partitioned_by, **materialized_properties
                )
            ):
                materialized_properties["catalog_name"] = exp.to_table(view_name).catalog
                properties.append("expressions", partitioned_by_prop)
            if clustered_by and (
                clustered_by_prop := self._build_clustered_by_exp(
                    clustered_by, **materialized_properties
                )
            ):
                properties.append("expressions", clustered_by_prop)

        create_view_properties = self._build_view_properties_exp(
            view_properties,
            (
                table_description
                if self.COMMENT_CREATION_VIEW.supports_schema_def and self.comments_enabled
                else None
            ),
            physical_cluster=create_kwargs.pop("physical_cluster", None),
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
                ),
                quote_identifiers=self.QUOTE_IDENTIFIERS_IN_VIEWS,
            )

        # Register table comment with commands if the engine doesn't support doing it in CREATE
        if (
            table_description
            and self.COMMENT_CREATION_VIEW.is_comment_command_only
            and self.comments_enabled
        ):
            self._create_table_comment(view_name, table_description, "VIEW")
        # Register column comments with commands if the engine doesn't support doing it in
        # CREATE or we couldn't do it in the CREATE schema definition because we don't have
        # columns_to_types
        if (
            column_descriptions
            and (
                self.COMMENT_CREATION_VIEW.is_comment_command_only
                or (
                    self.COMMENT_CREATION_VIEW.is_in_schema_def_and_commands
                    and not columns_to_types
                )
            )
            and self.comments_enabled
        ):
            self._create_column_comments(view_name, column_descriptions, "VIEW")

    @set_catalog()
    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
        properties: t.List[exp.Expression] = [],
    ) -> None:
        return self._create_schema(
            schema_name=schema_name,
            ignore_if_exists=ignore_if_exists,
            warn_on_error=warn_on_error,
            properties=properties,
            kind="SCHEMA",
        )

    def _create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool,
        warn_on_error: bool,
        properties: t.List[exp.Expression],
        kind: str,
    ) -> None:
        """Create a schema from a name or qualified table name."""
        try:
            self.execute(
                exp.Create(
                    this=to_schema(schema_name),
                    kind=kind,
                    exists=ignore_if_exists,
                    properties=exp.Properties(  # this renders as '' (empty string) if expressions is empty
                        expressions=properties
                    ),
                )
            )
        except Exception as e:
            if not warn_on_error:
                raise
            logger.warning("Failed to create %s '%s': %s", kind.lower(), schema_name, e)

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
        **drop_args: t.Dict[str, exp.Expression],
    ) -> None:
        return self._drop_object(
            name=schema_name,
            exists=ignore_if_not_exists,
            kind="SCHEMA",
            cascade=cascade,
            **drop_args,
        )

    def drop_view(
        self,
        view_name: TableName,
        ignore_if_not_exists: bool = True,
        materialized: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """Drop a view."""
        self.execute(
            exp.Drop(
                this=exp.to_table(view_name),
                exists=ignore_if_not_exists,
                materialized=materialized and self.SUPPORTS_MATERIALIZED_VIEWS,
                kind="VIEW",
                **kwargs,
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
            if column_name and column_name.strip() and column_type and column_type.strip()
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
    ) -> None:
        source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df, columns_to_types, target_table=table_name
        )
        self._insert_append_source_queries(table_name, source_queries, columns_to_types)

    def _insert_append_source_queries(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        with self.transaction(condition=len(source_queries) > 0):
            columns_to_types = columns_to_types or self.columns(table_name)
            for source_query in source_queries:
                with source_query as query:
                    self._insert_append_query(table_name, query, columns_to_types)

    def _insert_append_query(
        self,
        table_name: TableName,
        query: Query,
        columns_to_types: t.Dict[str, exp.DataType],
        order_projections: bool = True,
    ) -> None:
        if order_projections:
            query = self._order_projections_and_filter(query, columns_to_types)
        self.execute(exp.insert(query, table_name, columns=list(columns_to_types)))

    def insert_overwrite_by_partition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        partitioned_by: t.List[exp.Expression],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        if self.INSERT_OVERWRITE_STRATEGY.is_insert_overwrite:
            target_table = exp.to_table(table_name)
            source_queries, columns_to_types = self._get_source_queries_and_columns_to_types(
                query_or_df, columns_to_types, target_table=target_table
            )
            self._insert_overwrite_by_condition(
                table_name, source_queries, columns_to_types=columns_to_types
            )
        else:
            self._replace_by_key(
                table_name, query_or_df, columns_to_types, partitioned_by, is_unique_key=False
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
        time_column: TimeColumn | exp.Expression | str,
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
            this=exp.to_column(time_column) if isinstance(time_column, str) else time_column,
            low=low,
            high=high,
        )
        self._insert_overwrite_by_condition(table_name, source_queries, columns_to_types, where)

    def _values_to_sql(
        self,
        values: t.List[t.Tuple[t.Any, ...]],
        columns_to_types: t.Dict[str, exp.DataType],
        batch_start: int,
        batch_end: int,
        alias: str = "t",
    ) -> Query:
        return select_from_values_for_batch_range(
            values=values,
            columns_to_types=columns_to_types,
            batch_start=batch_start,
            batch_end=batch_end,
            alias=alias,
        )

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
    ) -> None:
        table = exp.to_table(table_name)
        insert_overwrite_strategy = (
            insert_overwrite_strategy_override or self.INSERT_OVERWRITE_STRATEGY
        )
        with self.transaction(
            condition=len(source_queries) > 0 or insert_overwrite_strategy.is_delete_insert
        ):
            columns_to_types = columns_to_types or self.columns(table_name)
            for i, source_query in enumerate(source_queries):
                with source_query as query:
                    query = self._order_projections_and_filter(query, columns_to_types, where=where)
                    if i > 0 or insert_overwrite_strategy.is_delete_insert:
                        if i == 0:
                            self.delete_from(table_name, where=where or exp.true())
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
                            columns=(
                                list(columns_to_types)
                                if not insert_overwrite_strategy.is_replace_where
                                else None
                            ),
                            overwrite=insert_overwrite_strategy.is_insert_overwrite,
                        )
                        if insert_overwrite_strategy.is_replace_where:
                            insert_exp.set("where", where or exp.true())
                        self.execute(insert_exp)

    def update_table(
        self,
        table_name: TableName,
        properties: t.Dict[str, t.Any],
        where: t.Optional[str | exp.Condition] = None,
    ) -> None:
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

    def scd_type_2_by_time(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        unique_key: t.Sequence[exp.Expression],
        valid_from_col: exp.Column,
        valid_to_col: exp.Column,
        execution_time: TimeLike,
        updated_at_col: exp.Column,
        invalidate_hard_deletes: bool = True,
        updated_at_as_valid_from: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        truncate: bool = False,
        **kwargs: t.Any,
    ) -> None:
        self._scd_type_2(
            target_table=target_table,
            source_table=source_table,
            unique_key=unique_key,
            valid_from_col=valid_from_col,
            valid_to_col=valid_to_col,
            execution_time=execution_time,
            updated_at_col=updated_at_col,
            invalidate_hard_deletes=invalidate_hard_deletes,
            updated_at_as_valid_from=updated_at_as_valid_from,
            columns_to_types=columns_to_types,
            table_description=table_description,
            column_descriptions=column_descriptions,
            truncate=truncate,
        )

    def scd_type_2_by_column(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        unique_key: t.Sequence[exp.Expression],
        valid_from_col: exp.Column,
        valid_to_col: exp.Column,
        execution_time: TimeLike,
        check_columns: t.Union[exp.Star, t.Sequence[exp.Column]],
        invalidate_hard_deletes: bool = True,
        execution_time_as_valid_from: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        truncate: bool = False,
        **kwargs: t.Any,
    ) -> None:
        self._scd_type_2(
            target_table=target_table,
            source_table=source_table,
            unique_key=unique_key,
            valid_from_col=valid_from_col,
            valid_to_col=valid_to_col,
            execution_time=execution_time,
            check_columns=check_columns,
            columns_to_types=columns_to_types,
            invalidate_hard_deletes=invalidate_hard_deletes,
            execution_time_as_valid_from=execution_time_as_valid_from,
            table_description=table_description,
            column_descriptions=column_descriptions,
            truncate=truncate,
        )

    def _scd_type_2(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        unique_key: t.Sequence[exp.Expression],
        valid_from_col: exp.Column,
        valid_to_col: exp.Column,
        execution_time: TimeLike,
        invalidate_hard_deletes: bool = True,
        updated_at_col: t.Optional[exp.Column] = None,
        check_columns: t.Optional[t.Union[exp.Star, t.Sequence[exp.Column]]] = None,
        updated_at_as_valid_from: bool = False,
        execution_time_as_valid_from: bool = False,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        truncate: bool = False,
    ) -> None:
        def remove_managed_columns(
            cols_to_types: t.Dict[str, exp.DataType],
        ) -> t.Dict[str, exp.DataType]:
            return {
                k: v for k, v in cols_to_types.items() if k not in {valid_from_name, valid_to_name}
            }

        valid_from_name = valid_from_col.name
        valid_to_name = valid_to_col.name
        unmanaged_columns_to_types = (
            remove_managed_columns(columns_to_types) if columns_to_types else None
        )
        source_queries, unmanaged_columns_to_types = self._get_source_queries_and_columns_to_types(
            source_table, unmanaged_columns_to_types, target_table=target_table, batch_size=0
        )
        columns_to_types = columns_to_types or self.columns(target_table)
        updated_at_name = updated_at_col.name if updated_at_col else None
        if (
            valid_from_name not in columns_to_types
            or valid_to_name not in columns_to_types
            or not columns_to_types_all_known(columns_to_types)
        ):
            columns_to_types = self.columns(target_table)
        if not columns_to_types:
            raise SQLMeshError(f"Could not get columns_to_types. Does {target_table} exist?")
        unmanaged_columns_to_types = unmanaged_columns_to_types or remove_managed_columns(
            columns_to_types
        )
        if not unique_key:
            raise SQLMeshError("unique_key must be provided for SCD Type 2")
        if check_columns and updated_at_col:
            raise SQLMeshError(
                "Cannot use both `check_columns` and `updated_at_name` for SCD Type 2"
            )
        if check_columns and updated_at_as_valid_from:
            raise SQLMeshError(
                "Cannot use both `check_columns` and `updated_at_as_valid_from` for SCD Type 2"
            )
        if execution_time_as_valid_from and not check_columns:
            raise SQLMeshError(
                "Cannot use `execution_time_as_valid_from` without `check_columns` for SCD Type 2"
            )
        if updated_at_name and updated_at_name not in columns_to_types:
            raise SQLMeshError(
                f"Column {updated_at_name} not found in {target_table}. Table must contain an `updated_at` timestamp for SCD Type 2"
            )
        time_data_type = columns_to_types[valid_from_name]
        select_source_columns: t.List[t.Union[str, exp.Alias]] = [
            col for col in unmanaged_columns_to_types if col != updated_at_name
        ]
        table_columns = [exp.column(c, quoted=True) for c in columns_to_types]
        if updated_at_name:
            select_source_columns.append(
                exp.cast(updated_at_col, time_data_type).as_(updated_at_col.this)  # type: ignore
            )

        # If a star is provided, we include all unmanaged columns in the check.
        # This unnecessarily includes unique key columns but since they are used in the join, and therefore we know
        # they are equal or not, the extra check is not a problem and we gain simplified logic here.
        # If we want to change this, then we just need to check the expressions in unique_key and pull out the
        # column names and then remove them from the unmanaged_columns
        if check_columns and check_columns == exp.Star():
            check_columns = [exp.column(col) for col in unmanaged_columns_to_types]
        execution_ts = to_time_column(execution_time, time_data_type)
        if updated_at_as_valid_from:
            if not updated_at_col:
                raise SQLMeshError(
                    "Cannot use `updated_at_as_valid_from` without `updated_at_name` for SCD Type 2"
                )
            update_valid_from_start: t.Union[str, exp.Expression] = updated_at_col
        elif execution_time_as_valid_from:
            update_valid_from_start = execution_ts
        else:
            update_valid_from_start = to_time_column("1970-01-01 00:00:00+00:00", time_data_type)
        insert_valid_from_start = execution_ts if check_columns else updated_at_col  # type: ignore
        # joined._exists IS NULL is saying "if the row is deleted"
        delete_check = (
            exp.column("_exists", "joined").is_(exp.Null()) if invalidate_hard_deletes else None
        )
        prefixed_valid_to_col = valid_to_col.copy()
        prefixed_valid_to_col.this.set("this", f"t_{prefixed_valid_to_col.name}")
        prefixed_valid_from_col = valid_from_col.copy()
        prefixed_valid_from_col.this.set("this", f"t_{valid_from_col.name}")
        if check_columns:
            row_check_conditions = []
            for col in check_columns:
                col_qualified = col.copy()
                col_qualified.set("table", exp.parse_identifier("joined"))

                t_col = col_qualified.copy()
                t_col.this.set("this", f"t_{col.name}")

                row_check_conditions.extend(
                    [
                        col_qualified.neq(t_col),
                        exp.and_(t_col.is_(exp.Null()), col_qualified.is_(exp.Null()).not_()),
                        exp.and_(t_col.is_(exp.Null()).not_(), col_qualified.is_(exp.Null())),
                    ]
                )
            row_value_check = exp.or_(*row_check_conditions)
            unique_key_conditions = []
            for key in unique_key:
                key_qualified = key.copy()
                key_qualified.set("table", exp.parse_identifier("joined"))
                t_key = key_qualified.copy()
                for col in t_key.find_all(exp.Column):
                    col.this.set("this", f"t_{col.name}")
                unique_key_conditions.extend(
                    [t_key.is_(exp.Null()).not_(), key_qualified.is_(exp.Null()).not_()]
                )
            unique_key_check = exp.and_(*unique_key_conditions)
            # unique_key_check is saying "if the row is updated"
            # row_value_check is saying "if the row has changed"
            updated_row_filter = exp.and_(unique_key_check, row_value_check)
            valid_to_case_stmt = (
                exp.Case()
                .when(
                    exp.and_(
                        exp.or_(
                            delete_check,
                            updated_row_filter,
                        )
                    ),
                    execution_ts,
                )
                .else_(prefixed_valid_to_col)
                .as_(valid_to_col.this)
            )
            valid_from_case_stmt = exp.func(
                "COALESCE",
                prefixed_valid_from_col,
                update_valid_from_start,
            ).as_(valid_from_col.this)
        else:
            assert updated_at_col is not None
            updated_at_col_qualified = updated_at_col.copy()
            updated_at_col_qualified.set("table", exp.parse_identifier("joined"))
            prefixed_updated_at_col = updated_at_col_qualified.copy()
            prefixed_updated_at_col.this.set("this", f"t_{updated_at_col_qualified.name}")
            updated_row_filter = updated_at_col_qualified > prefixed_updated_at_col

            valid_to_case_stmt_builder = exp.Case().when(
                updated_row_filter, updated_at_col_qualified
            )
            if delete_check:
                valid_to_case_stmt_builder = valid_to_case_stmt_builder.when(
                    delete_check, execution_ts
                )
            valid_to_case_stmt = valid_to_case_stmt_builder.else_(prefixed_valid_to_col).as_(
                valid_to_col.this
            )

            valid_from_case_stmt = (
                exp.Case()
                .when(
                    exp.and_(
                        prefixed_valid_from_col.is_(exp.Null()),
                        exp.column("_exists", "latest_deleted").is_(exp.Null()).not_(),
                    ),
                    exp.Case()
                    .when(
                        exp.column(valid_to_col.this, "latest_deleted") > updated_at_col,
                        exp.column(valid_to_col.this, "latest_deleted"),
                    )
                    .else_(updated_at_col),
                )
                .when(
                    prefixed_valid_from_col.is_(exp.Null()),
                    update_valid_from_start,
                )
                .else_(prefixed_valid_from_col)
            ).as_(valid_from_col.this)

        existing_rows_query = exp.select(*table_columns, exp.true().as_("_exists")).from_(
            target_table
        )
        if truncate:
            existing_rows_query = existing_rows_query.limit(0)

        with source_queries[0] as source_query:
            prefixed_columns_to_types = []
            for column in columns_to_types:
                prefixed_col = exp.column(column).copy()
                prefixed_col.this.set("this", f"t_{prefixed_col.name}")
                prefixed_columns_to_types.append(prefixed_col)
            prefixed_unmanaged_columns = []
            for column in unmanaged_columns_to_types:
                prefixed_col = exp.column(column).copy()
                prefixed_col.this.set("this", f"t_{prefixed_col.name}")
                prefixed_unmanaged_columns.append(prefixed_col)
            query = (
                exp.Select()  # type: ignore
                .select(*table_columns)
                .from_("static")
                .union(
                    exp.select(*table_columns).from_("updated_rows"),
                    distinct=False,
                )
                .union(
                    exp.select(*table_columns).from_("inserted_rows"),
                    distinct=False,
                )
                .with_(
                    "source",
                    exp.select(exp.true().as_("_exists"), *select_source_columns)
                    .distinct(*unique_key)
                    .from_(
                        self.inject_query_setting(source_query, check_server_default=True).subquery(  # type: ignore
                            "raw_source"
                        )
                    ),
                )
                # Historical Records that Do Not Change
                .with_(
                    "static",
                    existing_rows_query.where(valid_to_col.is_(exp.Null()).not_()),
                )
                # Latest Records that can be updated
                .with_(
                    "latest",
                    existing_rows_query.where(valid_to_col.is_(exp.Null())),
                )
                # Deleted records which can be used to determine `valid_from` for undeleted source records
                .with_(
                    "deleted",
                    exp.select(*[exp.column(col, "static") for col in columns_to_types])
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
                    .where(exp.column(valid_to_col.this, "latest").is_(exp.Null())),
                )
                # Get the latest `valid_to` deleted record for each unique key
                .with_(
                    "latest_deleted",
                    exp.select(
                        exp.true().as_("_exists"),
                        *(part.as_(f"_key{i}") for i, part in enumerate(unique_key)),
                        exp.Max(this=valid_to_col).as_(valid_to_col.this),
                    )
                    .from_("deleted")
                    .group_by(*unique_key),
                )
                # Do a full join between latest records and source table in order to combine them together
                # MySQL doesn't suport full join so going to do a left then right join and remove dups with union
                # We do a left/right and filter right on only matching to remove the need to do union distinct
                # which allows scd type 2 to be compatible with unhashable data types
                .with_(
                    "joined",
                    exp.select(
                        exp.column("_exists", table="source").as_("_exists"),
                        *(
                            exp.column(col, table="latest").as_(prefixed_columns_to_types[i].this)
                            for i, col in enumerate(columns_to_types)
                        ),
                        *(
                            exp.column(col, table="source").as_(col)
                            for col in unmanaged_columns_to_types
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
                        join_type="left",
                    )
                    .union(
                        exp.select(
                            exp.column("_exists", table="source").as_("_exists"),
                            *(
                                exp.column(col, table="latest").as_(
                                    prefixed_columns_to_types[i].this
                                )
                                for i, col in enumerate(columns_to_types)
                            ),
                            *(
                                exp.column(col, table="source").as_(col)
                                for col in unmanaged_columns_to_types
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
                        .where(exp.column("_exists", table="latest").is_(exp.Null())),
                        distinct=False,
                    ),
                )
                # Get deleted, new, no longer current, or unchanged records
                .with_(
                    "updated_rows",
                    exp.select(
                        *(
                            exp.func(
                                "COALESCE",
                                exp.column(prefixed_unmanaged_columns[i].this, table="joined"),
                                exp.column(col, table="joined"),
                            ).as_(col)
                            for i, col in enumerate(unmanaged_columns_to_types)
                        ),
                        valid_from_case_stmt,
                        valid_to_case_stmt,
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
                        *unmanaged_columns_to_types,
                        insert_valid_from_start.as_(valid_from_col.this),  # type: ignore
                        to_time_column(exp.null(), time_data_type).as_(valid_to_col.this),
                    )
                    .from_("joined")
                    .where(updated_row_filter),
                )
            )

            self.replace_query(
                target_table,
                self.inject_query_setting(query),
                columns_to_types=columns_to_types,
                table_description=table_description,
                column_descriptions=column_descriptions,
            )

    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[exp.Expression],
        when_matched: t.Optional[t.Union[exp.When, t.List[exp.When]]] = None,
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
        if not when_matched:
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
        when_matched = ensure_list(when_matched)
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
        match_expressions = when_matched + [when_not_matched]
        for source_query in source_queries:
            with source_query as query:
                self._merge(
                    target_table=target_table,
                    query=query,
                    on=on,
                    match_expressions=match_expressions,
                )

    def rename_table(
        self,
        old_table_name: TableName,
        new_table_name: TableName,
    ) -> None:
        new_table = exp.to_table(new_table_name)
        if new_table.catalog:
            old_table = exp.to_table(old_table_name)
            catalog = old_table.catalog or self.get_current_catalog()
            if catalog != new_table.catalog:
                raise UnsupportedCatalogOperationError(
                    "Tried to rename table across catalogs which is not supported"
                )
        self._rename_table(old_table_name, new_table_name)

    def get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """Lists all data objects in the target schema.

        Args:
            schema_name: The name of the schema to list data objects from.
            object_names: If provided, only return data objects with these names.

        Returns:
            A list of data objects in the target schema.
        """
        if object_names is not None:
            if not object_names:
                return []
            object_names_list = list(object_names)
            batches = [
                object_names_list[i : i + self.DATA_OBJECT_FILTER_BATCH_SIZE]
                for i in range(0, len(object_names_list), self.DATA_OBJECT_FILTER_BATCH_SIZE)
            ]
            return [
                obj for batch in batches for obj in self._get_data_objects(schema_name, set(batch))
            ]
        return self._get_data_objects(schema_name)

    def fetchone(
        self,
        query: t.Union[exp.Expression, str],
        ignore_unsupported_errors: bool = False,
        quote_identifiers: bool = False,
    ) -> t.Optional[t.Tuple]:
        with self.transaction():
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
        with self.transaction():
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
        with self.transaction():
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

    def wap_supported(self, table_name: TableName) -> bool:
        """Returns whether WAP for the target table is supported."""
        return False

    def wap_table_name(self, table_name: TableName, wap_id: str) -> str:
        """Returns the updated table name for the given WAP ID.

        Args:
            table_name: The name of the target table.
            wap_id: The WAP ID to prepare.

        Returns:
            The updated table name that should be used for writing.
        """
        raise NotImplementedError(f"Engine does not support WAP: {type(self)}")

    def wap_prepare(self, table_name: TableName, wap_id: str) -> str:
        """Prepares the target table for WAP and returns the updated table name.

        Args:
            table_name: The name of the target table.
            wap_id: The WAP ID to prepare.

        Returns:
            The updated table name that should be used for writing.
        """
        raise NotImplementedError(f"Engine does not support WAP: {type(self)}")

    def wap_publish(self, table_name: TableName, wap_id: str) -> None:
        """Publishes changes with the given WAP ID to the target table.

        Args:
            table_name: The name of the target table.
            wap_id: The WAP ID to publish.
        """
        raise NotImplementedError(f"Engine does not support WAP: {type(self)}")

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

        if self._pre_ping:
            try:
                logger.debug("Pinging the database to check the connection")
                self.ping()
            except Exception:
                logger.info("Connection to the database was lost. Reconnecting...")
                self._connection_pool.close()

        self._connection_pool.begin()
        try:
            yield
        except Exception as e:
            self._connection_pool.rollback()
            raise e
        else:
            self._connection_pool.commit()

    @contextlib.contextmanager
    def session(self, properties: SessionProperties) -> t.Iterator[None]:
        """A session context manager."""
        if self._is_session_active():
            yield
            return

        self._begin_session(properties)
        try:
            yield
        finally:
            self._end_session()

    def _begin_session(self, properties: SessionProperties) -> t.Any:
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
                sql = t.cast(
                    str,
                    (
                        self._to_sql(e, quote=quote_identifiers, **to_sql_kwargs)
                        if isinstance(e, exp.Expression)
                        else e
                    ),
                )
                self._log_sql(sql)
                self._execute(sql, **kwargs)

    def _log_sql(self, sql: str) -> None:
        logger.log(self._execute_log_level, "Executing SQL: %s", sql)

    def _execute(self, sql: str, **kwargs: t.Any) -> None:
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
                self.create_schema(schema_(table.args["db"], table.args.get("catalog")))
            self._create_table_from_source_queries(
                table,
                source_queries,
                columns_to_types,
                exists=True,
                table_description=None,
                column_descriptions=None,
                **kwargs,
            )

            try:
                yield table
            finally:
                self.drop_table(table)

    def _table_or_view_properties_to_expressions(
        self, table_or_view_properties: t.Optional[t.Dict[str, exp.Expression]] = None
    ) -> t.List[exp.Property]:
        """Converts model properties (either physical or virtual) to a list of property expressions."""
        if not table_or_view_properties:
            return []
        return [
            exp.Property(this=key, value=value.copy())
            for key, value in table_or_view_properties.items()
        ]

    def _build_partitioned_by_exp(
        self,
        partitioned_by: t.List[exp.Expression],
        *,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        catalog_name: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[t.Union[exp.PartitionedByProperty, exp.Property]]:
        return None

    def _build_clustered_by_exp(
        self,
        clustered_by: t.List[str],
        **kwargs: t.Any,
    ) -> t.Optional[exp.Cluster]:
        return None

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
        """Creates a SQLGlot table properties expression for ddl."""
        properties: t.List[exp.Expression] = []

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _build_view_properties_exp(
        self,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        table_description: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for view"""
        properties: t.List[exp.Expression] = []

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _truncate_comment(self, comment: str, length: t.Optional[int]) -> str:
        return comment[:length] if length else comment

    def _truncate_table_comment(self, comment: str) -> str:
        return self._truncate_comment(comment, self.MAX_TABLE_COMMENT_LENGTH)

    def _truncate_column_comment(self, comment: str) -> str:
        return self._truncate_comment(comment, self.MAX_COLUMN_COMMENT_LENGTH)

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
            **self._sql_gen_kwargs,
            **kwargs,
        }

        expression = expression.copy()

        if quote:
            quote_identifiers(expression)

        return expression.sql(**sql_gen_kwargs, copy=False)  # type: ignore

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        raise NotImplementedError()

    def _get_temp_table(
        self, table: TableName, table_only: bool = False, quoted: bool = True
    ) -> exp.Table:
        """
        Returns the name of the temp table that should be used for the given table name.
        """
        table = t.cast(exp.Table, exp.to_table(table).copy())
        table.set(
            "this", exp.to_identifier(f"__temp_{table.name}_{random_id(short=True)}", quoted=quoted)
        )

        if table_only:
            table.set("db", None)
            table.set("catalog", None)

        return table

    def _order_projections_and_filter(
        self,
        query: Query,
        columns_to_types: t.Dict[str, exp.DataType],
        where: t.Optional[exp.Expression] = None,
        coerce_types: bool = False,
    ) -> Query:
        if not isinstance(query, exp.Query) or (
            not where and not coerce_types and query.named_selects == list(columns_to_types)
        ):
            return query

        query = t.cast(exp.Query, query.copy())
        with_ = query.args.pop("with", None)

        select_exprs: t.List[exp.Expression] = [
            exp.column(c, quoted=True) for c in columns_to_types
        ]
        if coerce_types and columns_to_types_all_known(columns_to_types):
            select_exprs = [
                exp.cast(select_exprs[i], col_tpe).as_(col, quoted=True)
                for i, (col, col_tpe) in enumerate(columns_to_types.items())
            ]

        query = exp.select(*select_exprs).from_(query.subquery("_subquery", copy=False), copy=False)
        if where:
            query = query.where(where, copy=False)

        if with_:
            query.set("with", with_)

        return query

    def _truncate_table(self, table_name: TableName) -> None:
        table = exp.to_table(table_name)
        self.execute(f"TRUNCATE TABLE {table.sql(dialect=self.dialect, identify=True)}")

    def _replace_by_key(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        key: t.Sequence[exp.Expression],
        is_unique_key: bool,
    ) -> None:
        if columns_to_types is None:
            columns_to_types = self.columns(target_table)

        temp_table = self._get_temp_table(target_table)
        key_exp = exp.func("CONCAT_WS", "'__SQLMESH_DELIM__'", *key) if len(key) > 1 else key[0]
        column_names = list(columns_to_types or [])

        with self.transaction():
            self.ctas(temp_table, source_table, columns_to_types=columns_to_types, exists=False)

            try:
                delete_query = exp.select(key_exp).from_(temp_table)
                insert_query = self._select_columns(columns_to_types).from_(temp_table)
                if not is_unique_key:
                    delete_query = delete_query.distinct()
                else:
                    insert_query = insert_query.distinct(*key)

                insert_statement = exp.insert(
                    insert_query,
                    target_table,
                    columns=column_names,
                )
                delete_filter = key_exp.isin(query=delete_query)

                if not self.INSERT_OVERWRITE_STRATEGY.is_replace_where:
                    self.execute(exp.delete(target_table).where(delete_filter))
                else:
                    insert_statement.set("where", delete_filter)
                    insert_statement.set("this", exp.to_table(target_table))

                self.execute(insert_statement)
            finally:
                self.drop_table(temp_table)

    def _build_create_comment_table_exp(
        self, table: exp.Table, table_comment: str, table_kind: str
    ) -> exp.Comment | str:
        return exp.Comment(
            this=table,
            kind=table_kind,
            expression=exp.Literal.string(self._truncate_table_comment(table_comment)),
        )

    def _create_table_comment(
        self, table_name: TableName, table_comment: str, table_kind: str = "TABLE"
    ) -> None:
        table = exp.to_table(table_name)

        try:
            self.execute(self._build_create_comment_table_exp(table, table_comment, table_kind))
        except Exception:
            logger.warning(
                f"Table comment for '{table.alias_or_name}' not registered - this may be due to limited permissions.",
                exc_info=True,
            )

    def _build_create_comment_column_exp(
        self, table: exp.Table, column_name: str, column_comment: str, table_kind: str = "TABLE"
    ) -> exp.Comment | str:
        return exp.Comment(
            this=exp.column(column_name, *reversed(table.parts)),  # type: ignore
            kind="COLUMN",
            expression=exp.Literal.string(self._truncate_column_comment(column_comment)),
        )

    def _create_column_comments(
        self,
        table_name: TableName,
        column_comments: t.Dict[str, str],
        table_kind: str = "TABLE",
    ) -> None:
        table = exp.to_table(table_name)

        for col, comment in column_comments.items():
            try:
                self.execute(self._build_create_comment_column_exp(table, col, comment, table_kind))
            except Exception:
                logger.warning(
                    f"Column comments for column '{col}' in table '{table.alias_or_name}' not registered - this may be due to limited permissions.",
                    exc_info=True,
                )

    def _rename_table(
        self,
        old_table_name: TableName,
        new_table_name: TableName,
    ) -> None:
        self.execute(exp.rename_table(old_table_name, new_table_name))

    def inject_query_setting(
        self,
        query: Query,
        setting_name: str = "join_use_nulls",
        setting_value: str = "1",
        check_server_default: bool = False,
    ) -> Query:
        return query

    def ping(self) -> None:
        try:
            self._execute(exp.select("1").sql(dialect=self.dialect))
        finally:
            self._connection_pool.close_cursor()

    @classmethod
    def _select_columns(cls, columns: t.Iterable[str]) -> exp.Select:
        return exp.select(*(exp.column(c, quoted=True) for c in columns))


class EngineAdapterWithIndexSupport(EngineAdapter):
    SUPPORTS_INDEXES = True


def _decoded_str(value: t.Union[str, bytes]) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value
