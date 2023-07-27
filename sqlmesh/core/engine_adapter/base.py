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
import typing as t
import uuid
from enum import Enum

import pandas as pd
from sqlglot import Dialect, exp
from sqlglot.errors import ErrorLevel
from sqlglot.helper import ensure_list
from sqlglot.optimizer.qualify_columns import quote_identifiers

from sqlmesh.core.dialect import pandas_to_sql
from sqlmesh.core.engine_adapter.shared import DataObject, TransactionType
from sqlmesh.core.model.kind import TimeColumn
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.utils import double_escape, optional_import
from sqlmesh.utils.connection_pool import create_connection_pool
from sqlmesh.utils.date import TimeLike, make_inclusive
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
    SUPPORTS_INDEXES = False
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT
    SUPPORTS_MATERIALIZED_VIEWS = False
    SCHEMA_DIFFER = SchemaDiffer()

    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        dialect: str = "",
        sql_gen_kwargs: t.Optional[t.Dict[str, Dialect | bool | str]] = None,
        multithreaded: bool = False,
        **kwargs: t.Any,
    ):
        self.dialect = dialect.lower() or self.DIALECT
        self._connection_pool = create_connection_pool(connection_factory, multithreaded)
        self.sql_gen_kwargs = sql_gen_kwargs or {}
        self._extra_config = kwargs

    @classmethod
    def is_pandas_df(cls, value: t.Any) -> bool:
        return isinstance(value, pd.DataFrame)

    @classmethod
    def is_pyspark_df(cls, value: t.Any) -> bool:
        return hasattr(value, "sparkSession")

    @classmethod
    def is_df(self, value: t.Any) -> bool:
        return self.is_pandas_df(value) or self.is_pyspark_df(value)

    @classmethod
    def try_get_df(cls, value: t.Any) -> t.Optional[DF]:
        if cls.is_df(value):
            return value
        return None

    @classmethod
    def try_get_pyspark_df(cls, value: t.Any) -> t.Optional[PySparkDataFrame]:
        if cls.is_pyspark_df(value):
            return value
        return None

    @classmethod
    def try_get_pandas_df(cls, value: t.Any) -> t.Optional[pd.DataFrame]:
        if cls.is_pandas_df(value):
            return value
        return None

    @property
    def cursor(self) -> t.Any:
        return self._connection_pool.get_cursor()

    @property
    def spark(self) -> t.Optional[PySparkSession]:
        return None

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
        df = self.try_get_pandas_df(query_or_df)
        if df is not None:
            return self._create_table_from_df(table, df, columns_to_types, replace=True, **kwargs)
        else:
            query_or_df = t.cast("Query", query_or_df)
            return self._create_table_from_query(table, query_or_df, replace=True, **kwargs)

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
        df = self.try_get_pandas_df(query_or_df)
        if df is not None:
            self._create_table_from_df(table_name, df, columns_to_types, exists, **kwargs)
        else:
            query_or_df = t.cast("Query", query_or_df)
            self._create_table_from_query(table_name, query_or_df, exists, **kwargs)

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
        self._create_table(schema, None, exists=exists, **kwargs)

    def _create_table_from_query(
        self,
        table_name: TableName,
        query: Query,
        exists: bool = True,
        replace: bool = False,
        **kwargs: t.Any,
    ) -> None:
        table = exp.to_table(table_name)
        self._create_table(table, query, exists=exists, replace=replace, **kwargs)

    def _create_table_from_df(
        self,
        table_name: TableName,
        df: DF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        exists: bool = True,
        replace: bool = False,
        **kwargs: t.Any,
    ) -> None:
        if not isinstance(df, pd.DataFrame):
            raise ValueError("df must be a pandas DataFrame")
        columns_to_types = columns_to_types or columns_to_types_from_df(df)
        with self.transaction():
            if replace:
                self.drop_table(table_name)
            self._create_table_from_columns(table_name, columns_to_types, **kwargs)
            self._insert_append_pandas_df(table_name, df, columns_to_types)

    def _create_table(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expression],
        exists: bool = True,
        replace: bool = False,
        **kwargs: t.Any,
    ) -> None:
        exists = False if replace else exists
        if not isinstance(table_name_or_schema, exp.Schema):
            table_name_or_schema = exp.to_table(table_name_or_schema)
        properties = self._create_table_properties(**kwargs) if kwargs else None
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
        with self.transaction(TransactionType.DDL):
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
        schema: t.Optional[exp.Table | exp.Schema] = exp.to_table(view_name)
        df = self.try_get_pandas_df(query_or_df)
        if df is not None:
            if columns_to_types is None:
                columns_to_types = columns_to_types_from_df(df)

            schema = exp.Schema(
                this=schema,
                expressions=[exp.column(column) for column in columns_to_types],
            )
            query_or_df = next(self._pandas_to_sql(df, columns_to_types=columns_to_types))

        properties = create_kwargs.pop("properties", None)
        if not properties:
            properties = exp.Properties(expressions=[])

        if materialized and self.SUPPORTS_MATERIALIZED_VIEWS:
            properties.append("expressions", exp.MaterializedProperty())

        if properties.expressions:
            create_kwargs["properties"] = properties

        self.execute(
            exp.Create(
                this=schema,
                kind="VIEW",
                replace=replace,
                expression=query_or_df,
                **create_kwargs,
            )
        )

    def create_schema(self, schema_name: str, ignore_if_exists: bool = True) -> None:
        """Create a schema from a name or qualified table name."""
        self.execute(
            exp.Create(
                this=exp.to_identifier(schema_name.split(".")[0]),
                kind="SCHEMA",
                exists=ignore_if_exists,
            )
        )

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

    def drop_view(self, view_name: TableName, ignore_if_not_exists: bool = True) -> None:
        """Drop a view."""
        self.execute(
            exp.Drop(this=exp.to_table(view_name), exists=ignore_if_not_exists, kind="VIEW")
        )

    def columns(
        self, table_name: TableName, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        """Fetches column names and types for the target table."""
        self.execute(exp.Describe(this=exp.to_table(table_name), kind="TABLE"))
        describe_output = self.cursor.fetchall()
        return {
            column_name: exp.DataType.build(column_type, dialect=self.dialect)
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
        df = self.try_get_pandas_df(query_or_df)
        if df is not None:
            self._insert_append_pandas_df(
                table_name, df, columns_to_types, contains_json=contains_json
            )
        else:
            query = t.cast("Query", query_or_df)
            if contains_json:
                query = self._escape_json(query)
            self._insert_append_query(table_name, query, columns_to_types)

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

    def _insert_append_query(
        self,
        table_name: TableName,
        query: Query,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        column_names = list(columns_to_types or [])
        self.execute(exp.insert(query, table_name, columns=column_names))

    def _insert_append_pandas_df(
        self,
        table_name: TableName,
        df: pd.DataFrame,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        contains_json: bool = False,
    ) -> None:
        connection = self._connection_pool.get()
        table = exp.to_table(table_name)

        sqlalchemy = optional_import("sqlalchemy")
        # pandas to_sql doesn't support insert overwrite, it only supports deleting the table or appending
        if sqlalchemy and isinstance(connection, sqlalchemy.engine.Connectable):
            df.to_sql(
                table.sql(dialect=self.dialect),
                connection,
                if_exists="append",
                index=False,
                chunksize=self.DEFAULT_BATCH_SIZE,
                method="multi",
            )
        else:
            column_names = list(columns_to_types or [])
            with self.transaction():
                for i, expression in enumerate(
                    self._pandas_to_sql(
                        df, columns_to_types, self.DEFAULT_BATCH_SIZE, contains_json=contains_json
                    )
                ):
                    self.execute(exp.insert(expression, table_name, columns=column_names))

    def insert_overwrite_by_partition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        partitioned_by: t.List[exp.Expression],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        self._insert_overwrite_by_condition(
            table_name, query_or_df, columns_to_types=columns_to_types
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
    ) -> None:
        if columns_to_types is None:
            columns_to_types = self.columns(table_name)

        low, high = [time_formatter(dt, columns_to_types) for dt in make_inclusive(start, end)]
        if isinstance(time_column, TimeColumn):
            time_column = time_column.column
        where = exp.Between(
            this=exp.to_column(time_column),
            low=low,
            high=high,
        )
        return self._insert_overwrite_by_condition(table_name, query_or_df, where, columns_to_types)

    @classmethod
    def _pandas_to_sql(
        cls,
        df: pd.DataFrame,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        batch_size: int = 0,
        alias: str = "t",
        contains_json: bool = False,
    ) -> t.Generator[exp.Select, None, None]:
        for expression in pandas_to_sql(df, columns_to_types, batch_size, alias):
            yield expression if not contains_json else t.cast(
                exp.Select, cls._escape_json(expression)
            )

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        where: t.Optional[exp.Condition] = None,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        table = exp.to_table(table_name)
        if (
            self.INSERT_OVERWRITE_STRATEGY
            in (InsertOverwriteStrategy.DELETE_INSERT, InsertOverwriteStrategy.REPLACE_WHERE)
            and not where
        ):
            raise SQLMeshError(
                "Where condition is required when doing a delete/insert or replace/where for insert/overwrite"
            )
        if self.INSERT_OVERWRITE_STRATEGY.is_delete_insert:
            with self.transaction():
                assert where is not None
                self.delete_from(table_name, where=where)
                self.insert_append(table_name, query_or_df, columns_to_types=columns_to_types)
        else:
            df = self.try_get_pandas_df(query_or_df)
            if df is not None:
                query_or_df = next(
                    pandas_to_sql(
                        df,
                        alias=table.alias_or_name,
                        columns_to_types=columns_to_types,
                    )
                )

            query = self._add_where_to_query(t.cast("Query", query_or_df), where)

            insert_exp = exp.insert(
                query,
                table,
                columns=list(columns_to_types or []),
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
        source_table: QueryOrDF,
        on: exp.Expression,
        match_expressions: t.List[exp.When],
    ) -> None:
        this = exp.alias_(exp.to_table(target_table), alias=MERGE_TARGET_ALIAS, table=True)
        using = exp.alias_(
            exp.Subquery(this=source_table), alias=MERGE_SOURCE_ALIAS, copy=False, table=True
        )
        self.execute(
            exp.Merge(
                this=this,
                using=using,
                on=on,
                expressions=match_expressions,
            )
        )

    def merge(
        self,
        target_table: TableName,
        source_table: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        unique_key: t.Sequence[str],
    ) -> None:
        if columns_to_types is None:
            columns_to_types = self.columns(target_table)

        df = self.try_get_pandas_df(source_table)
        if df is not None:
            source_table = next(
                pandas_to_sql(
                    df,
                    columns_to_types=columns_to_types,
                )
            )

        column_names = list(columns_to_types or [])
        on = exp.and_(
            *(
                exp.EQ(
                    this=exp.column(part, MERGE_TARGET_ALIAS),
                    expression=exp.column(part, MERGE_SOURCE_ALIAS),
                )
                for part in unique_key
            )
        )
        when_matched = exp.When(
            matched=True,
            source=False,
            then=exp.Update(
                expressions=[
                    exp.EQ(
                        this=exp.column(col, MERGE_TARGET_ALIAS),
                        expression=exp.column(col, MERGE_SOURCE_ALIAS),
                    )
                    for col in column_names
                ],
            ),
        )
        when_not_matched = exp.When(
            matched=False,
            source=False,
            then=exp.Insert(
                this=exp.Tuple(expressions=[exp.column(col) for col in column_names]),
                expression=exp.Tuple(
                    expressions=[exp.column(col, MERGE_SOURCE_ALIAS) for col in column_names]
                ),
            ),
        )
        return self._merge(
            target_table=target_table,
            source_table=source_table,
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
        self, transaction_type: TransactionType = TransactionType.DML
    ) -> t.Iterator[None]:
        """A transaction context manager."""
        if self._connection_pool.is_transaction_active or not self.supports_transactions(
            transaction_type
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

    def supports_transactions(self, transaction_type: TransactionType) -> bool:
        """Whether or not the engine adapter supports transactions for the given transaction type."""
        return True

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

        for e in ensure_list(expressions):
            sql = (
                self._to_sql(e, quote=quote_identifiers, **to_sql_kwargs)
                if isinstance(e, exp.Expression)
                else e
            )
            logger.debug(f"Executing SQL:\n{sql}")
            self.cursor.execute(sql, **kwargs)

    @contextlib.contextmanager
    def temp_table(self, query_or_df: QueryOrDF, name: TableName = "diff") -> t.Iterator[exp.Table]:
        """A context manager for working a temp table.

        The table will be created with a random guid and cleaned up after the block.

        Args:
            query_or_df: The query or df to create a temp table for.
            name: The base name of the temp table.

        Yields:
            The table expression
        """

        with self.transaction(TransactionType.DDL):
            table = self._get_temp_table(name)
            if table.db:
                self.create_schema(table.db)
            self.ctas(table, query_or_df)

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
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for ddl."""
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

    def _add_where_to_query(self, query: Query, where: t.Optional[exp.Expression]) -> Query:
        if not where or not isinstance(query, exp.Subqueryable):
            return query

        query = t.cast(exp.Subqueryable, query.copy())
        with_ = query.args.pop("with", None)
        query = (
            exp.select("*", copy=False)
            .from_(query.subquery("_subquery", copy=False), copy=False)
            .where(where, copy=False)
        )

        if with_:
            query.set("with", with_)

        return query


class EngineAdapterWithIndexSupport(EngineAdapter):
    SUPPORTS_INDEXES = True
