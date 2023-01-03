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

import duckdb
import pandas as pd
from sqlglot import exp, parse_one

from sqlmesh.utils import optional_import
from sqlmesh.utils.connection_pool import create_connection_pool
from sqlmesh.utils.df import pandas_to_sql
from sqlmesh.utils.errors import SQLMeshError

SOURCE_ALIAS = "__MERGE_SOURCE__"
DF_TYPES: t.Tuple = (pd.DataFrame,)

if t.TYPE_CHECKING:
    import pyspark

    PySparkDataFrame = pyspark.sql.DataFrame
    DF = t.Union[pd.DataFrame, PySparkDataFrame]
    Query = t.Union[exp.Subqueryable, exp.DerivedTable]
    QueryOrDF = t.Union[Query, DF]
else:
    try:
        import pyspark

        PySparkDataFrame = pyspark.sql.DataFrame
        DF_TYPES += (PySparkDataFrame,)
    except ImportError:
        PySparkDataFrame = None


logger = logging.getLogger(__name__)


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

    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        dialect: str,
        multithreaded: bool = False,
    ):
        self.dialect = dialect.lower()
        self._connection_pool = create_connection_pool(
            connection_factory, multithreaded
        )
        self._transaction = False

    @property
    def cursor(self) -> t.Any:
        return self._connection_pool.get_cursor()

    @property
    def spark(self) -> t.Optional[pyspark.sql.SparkSession]:
        spark_session = getattr(self._connection_pool.get(), "spark", None)
        if spark_session:
            spark_session = t.cast(pyspark.sql.SparkSession, spark_session)
        return spark_session

    def recycle(self) -> t.Any:
        """Closes all open connections and releases all allocated resources associated with any thread
        except the calling one."""
        self._connection_pool.close_all(exclude_calling_thread=True)

    def close(self) -> t.Any:
        """Closes all open connections and releases all allocated resources."""
        self._connection_pool.close_all()

    def create_and_insert(
        self,
        table_name: str,
        columns_to_types: t.Dict[str, exp.DataType],
        query_or_df: QueryOrDF,
        **kwargs,
    ):
        """Inserts query into table and creates it if missing.

        Args:
            table_name: The name of the table (eg. prod.table)
            columns_to_types: A mapping between the column name and its data type
            query_or_df: The SQL query or dataframe to insert.
            kwargs: Additional kwargs for creating the table or updating the query
        """
        self.create_table(table_name, columns_to_types, **kwargs)
        self.insert_append(table_name, query_or_df, columns_to_types=columns_to_types)

    def replace_query(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        """Replaces an existing table with a query.

        For partition based engines (hive, spark), insert override is used. For other systems, create or replace is used.

        Args:
            table_name: The name of the table (eg. prod.table)
            query_or_df: The SQL query to run or a dataframe.
            columns_to_types: Only used if a dataframe is provided. A mapping between the column name and its data type.
                Expected to be ordered to match the order of values in the dataframe.
        """
        if self.supports_partitions:
            self.insert_overwrite(table_name, query_or_df, columns_to_types)
        else:
            table = exp.to_table(table_name)
            if isinstance(query_or_df, pd.DataFrame):
                if not columns_to_types:
                    raise ValueError("columns_to_types must be provided for dataframes")
                expression = next(
                    pandas_to_sql(
                        query_or_df,
                        alias=table_name.split(".")[-1],
                        columns_to_types=columns_to_types,
                    )
                )
                create = exp.Create(
                    this=table,
                    kind="TABLE",
                    replace=True,
                    expression=expression,
                )
            else:
                create = exp.Create(
                    this=table,
                    kind="TABLE",
                    replace=True,
                    expression=query_or_df,
                )
            self.execute(create)

    def create_table(
        self,
        table_name: str,
        query_or_columns_to_types: Query | t.Dict[str, exp.DataType],
        exists: bool = True,
        **kwargs,
    ) -> None:
        """Create a table using a DDL statement or a CTAS.

        If a query is passed in instead of column type map, CREATE TABLE AS will be used.

        Args:
            table_name: The name of the table to create. Can be fully qualified or just table name.
            query_or_columns_to_types: A query or mapping between the column name and its data type.
            exists: Indicates whether to include the IF NOT EXISTS check.
            kwargs: Optional create table properties.
        """
        properties = self._create_table_properties(**kwargs)

        query = None
        schema: t.Optional[exp.Schema | exp.Table] = exp.to_table(table_name)

        if isinstance(query_or_columns_to_types, dict):
            schema = exp.Schema(
                this=schema,
                expressions=[
                    exp.ColumnDef(this=exp.to_identifier(column), kind=kind)
                    for column, kind in query_or_columns_to_types.items()
                ],
            )
        else:
            query = query_or_columns_to_types

        create_expression = exp.Create(
            this=schema,
            kind="TABLE",
            exists=exists,
            properties=properties,
            expression=query,
        )
        self.execute(create_expression)

    def drop_table(self, table_name: str, exists: bool = True) -> None:
        """Drops a table.

        Args:
            table_name: The name of the table to drop.
            exists: If exists, defaults to True.
        """
        drop_expression = exp.Drop(this=table_name, kind="TABLE", exists=exists)
        self.execute(drop_expression)

    def alter_table(
        self,
        table_name: str,
        added_columns: t.Dict[str, str],
        dropped_columns: t.Sequence[str],
    ) -> None:
        with self.transaction():
            alter_table = exp.AlterTable(this=exp.to_table(table_name))

            for column_name, column_type in added_columns.items():
                add_column = exp.ColumnDef(
                    this=exp.to_identifier(column_name),
                    kind=parse_one(column_type, into=exp.DataType),  # type: ignore
                )
                alter_table.set("actions", [add_column])

                self.execute(alter_table)

            for column_name in dropped_columns:
                drop_column = exp.Drop(
                    this=exp.column(column_name, quoted=True), kind="COLUMN"
                )
                alter_table.set("actions", [drop_column])

                self.execute(alter_table)

    def create_view(
        self,
        view_name: str,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
    ) -> None:
        """Create a view with a query or dataframe.

        If a dataframe is passed in, it will be converted into a literal values statement.
        This should only be done if the dataframe is very small!

        Args:
            view_name: The view name.
            query_or_df: A query or dataframe.
            columns_to_types: Columns to use in the view statement.
            replace: Whether or not to replace an existing view defaults to True.
        """
        schema: t.Optional[exp.Table | exp.Schema] = exp.to_table(view_name)

        if isinstance(query_or_df, DF_TYPES):
            if PySparkDataFrame and isinstance(query_or_df, PySparkDataFrame):
                query_or_df = query_or_df.toPandas()

            if not isinstance(query_or_df, pd.DataFrame):
                raise SQLMeshError("Can only create views with pandas dataframes.")

            if not columns_to_types:
                raise SQLMeshError(
                    "Creating a view with a dataframe requires passing in columns_to_types."
                )
            schema = exp.Schema(
                this=schema,
                expressions=[
                    exp.column(column, quoted=True) for column in columns_to_types
                ],
            )
            query_or_df = next(
                pandas_to_sql(query_or_df, columns_to_types=columns_to_types)
            )

        self.execute(
            exp.Create(
                this=schema,
                kind="VIEW",
                replace=replace,
                expression=query_or_df,
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

    def drop_view(self, view_name: str, ignore_if_not_exists: bool = True) -> None:
        """Drop a view."""
        if_exists = " IF EXISTS" if ignore_if_not_exists else ""
        self.execute(f"DROP VIEW{if_exists} {view_name}")

    def columns(self, table_name: str) -> t.Dict[str, str]:
        """Fetches column names and types for the target table."""
        self.execute(f"DESCRIBE TABLE {table_name}")
        describe_output = self.cursor.fetchall()
        return {
            t[0]: t[1].upper()
            for t in itertools.takewhile(
                lambda t: not t[0].startswith("#"),
                describe_output,
            )
        }

    def table_exists(self, table_name: str) -> bool:
        try:
            self.execute(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            return False

    def delete_from(self, table_name: str, where: t.Union[str, exp.Expression]) -> None:
        self.execute(exp.delete(table_name, where))

    def insert_append(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        self._insert(table_name, query_or_df, columns_to_types, overwrite=False)

    def insert_overwrite(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        self._insert(table_name, query_or_df, columns_to_types, overwrite=True)

    def delete_insert_query(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        where: exp.Condition,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> None:
        with self.transaction():
            self.delete_from(table_name, where=where)
            self.insert_append(
                table_name, query_or_df, columns_to_types=columns_to_types
            )

    def update_table(
        self,
        table_name: str,
        properties: t.Optional[t.Dict[str, t.Any]] = None,
        where: t.Optional[str | exp.Condition] = None,
    ) -> None:
        self.execute(exp.update(table_name, properties, where=where))

    def merge(
        self,
        target_table: str,
        source_table: QueryOrDF,
        column_names: t.Iterable[str],
        unique_key: t.Iterable[str],
    ):
        using = exp.Subquery(this=source_table, alias=SOURCE_ALIAS)
        on = exp.and_(
            *(
                exp.EQ(
                    this=exp.column(part, target_table),
                    expression=exp.column(part, SOURCE_ALIAS),
                )
                for part in unique_key
            )
        )
        when_matched = exp.When(
            this="MATCHED",
            then=exp.update(
                None,
                properties={
                    exp.column(col, target_table): exp.column(col, SOURCE_ALIAS)
                    for col in column_names
                },
            ),
        )
        when_not_matched = exp.When(
            this=exp.Not(this="MATCHED"),
            then=exp.Insert(
                this=exp.Tuple(expressions=[exp.column(col) for col in column_names]),
                expression=exp.Tuple(
                    expressions=[exp.column(col, SOURCE_ALIAS) for col in column_names]
                ),
            ),
        )
        self.execute(
            exp.Merge(
                this=target_table,
                using=using,
                on=on,
                expressions=[
                    when_matched,
                    when_not_matched,
                ],
            )
        )

    def fetchone(self, query: t.Union[exp.Expression, str]) -> t.Tuple:
        self.execute(query)
        return self.cursor.fetchone()

    def fetchall(self, query: t.Union[exp.Expression, str]) -> t.List[t.Tuple]:
        self.execute(query)
        return self.cursor.fetchall()

    def _fetchdf(self, query: t.Union[exp.Expression, str]) -> DF:
        """Fetches a DataFrame that can be either Pandas or PySpark from the cursor"""
        self.execute(query)
        if hasattr(self.cursor, "fetchdf"):
            return self.cursor.fetchdf()
        if hasattr(self.cursor, "fetchall_arrow"):
            return self.cursor.fetchall_arrow().to_pandas()
        raise NotImplementedError(
            "The cursor does not have a way to return a Pandas DataFrame"
        )

    def fetchdf(self, query: t.Union[exp.Expression, str]) -> pd.DataFrame:
        """Fetches a Pandas DataFrame from the cursor"""
        df = self._fetchdf(query)
        if not isinstance(df, pd.DataFrame):
            return df.toPandas()
        return df

    def fetch_pyspark_df(self, query: t.Union[exp.Expression, str]) -> PySparkDataFrame:
        """Fetches a PySpark DataFrame from the cursor"""
        df = self._fetchdf(query)
        if PySparkDataFrame and not isinstance(df, PySparkDataFrame):
            raise NotImplementedError(
                "The cursor does not have a way to return a PySpark DataFrame"
            )
        return df

    @contextlib.contextmanager
    def transaction(self) -> t.Generator[None, None, None]:
        """A transaction context manager."""
        if self._transaction or not self.supports_transactions:
            yield
            return
        self._transaction = True
        self.execute(exp.Transaction())
        try:
            yield
        except Exception as e:
            self.execute(exp.Rollback())
            raise e
        else:
            self.execute(exp.Commit())
        finally:
            self._transaction = False

    @property
    def supports_partitions(self) -> bool:
        """Whether or not the engine adapter supports partitions."""
        return self.dialect in ("hive", "spark")

    @property
    def supports_transactions(self) -> bool:
        """Whether or not the engine adapter supports transactions."""
        return self.dialect not in ("hive", "spark")

    def execute(self, sql: t.Union[str, exp.Expression]) -> None:
        """Execute a sql query."""
        sql = self._to_sql(sql) if isinstance(sql, exp.Expression) else sql
        logger.debug(f"Executing SQL:\n{sql}")
        self.cursor.execute(sql)

    def _insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]],
        overwrite: bool,
        batch_size: int = 10000,
    ) -> None:
        if not columns_to_types:
            into: t.Optional[exp.Expression] = exp.to_table(table_name)
        else:
            into = exp.Schema(
                this=exp.to_table(table_name),
                expressions=[exp.column(c, quoted=True) for c in columns_to_types],
            )

        connection = self._connection_pool.get()

        if (
            self.spark
            and PySparkDataFrame
            and isinstance(query_or_df, (PySparkDataFrame, pd.DataFrame))
        ):
            if not isinstance(query_or_df, PySparkDataFrame):
                query_or_df = self.spark.createDataFrame(query_or_df)
            query_or_df.select(*self.spark.table(table_name).columns).write.insertInto(  # type: ignore
                table_name, overwrite=overwrite
            )
        elif isinstance(query_or_df, pd.DataFrame):
            sqlalchemy = optional_import("sqlalchemy")
            # pandas to_sql doesn't support insert overwrite, it only supports deleting the table or appending
            if (
                not overwrite
                and sqlalchemy
                and isinstance(connection, sqlalchemy.engine.Connectable)
            ):
                query_or_df.to_sql(
                    table_name,
                    connection,
                    if_exists="append",
                    index=False,
                    chunksize=batch_size,
                    method="multi",
                )
            elif isinstance(connection, duckdb.DuckDBPyConnection):
                self.execute(
                    exp.Insert(
                        this=into,
                        expression="SELECT * FROM query_or_df",
                        overwrite=overwrite,
                    )
                )
            else:
                if not columns_to_types:
                    raise SQLMeshError(
                        "Column Mapping must be specified when using a DataFrame and not using SQLAlchemy or running on DuckDB"
                    )
                with self.transaction():
                    for i, expression in enumerate(
                        pandas_to_sql(query_or_df, columns_to_types, batch_size)
                    ):
                        self.execute(
                            exp.Insert(
                                this=into,
                                expression=expression,
                                overwrite=overwrite if i == 0 else False,
                            )
                        )
        elif isinstance(query_or_df, exp.Expression):
            self.execute(
                exp.Insert(
                    this=into,
                    expression=query_or_df,
                    overwrite=overwrite,
                )
            )
        else:
            raise SQLMeshError(f"Unsupported dataframe {query_or_df}")

    def _create_table_properties(
        self,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[str]] = None,
    ) -> t.Optional[exp.Properties]:
        if not self.supports_partitions:
            return None

        format_property = None
        partition_columns_property = None
        if storage_format:
            format_property = exp.TableFormatProperty(this=exp.Var(this=storage_format))
        if partitioned_by:
            partition_columns_property = exp.PartitionedByProperty(
                this=exp.Schema(
                    expressions=[exp.to_identifier(column) for column in partitioned_by]
                ),
            )
        return exp.Properties(
            expressions=[
                table_property
                for table_property in [format_property, partition_columns_property]
                if table_property
            ]
        )

    def _to_sql(self, e: exp.Expression, **kwargs) -> str:
        kwargs = {
            "dialect": self.dialect,
            "pretty": False,
            "comments": False,
            "identify": True,
            **kwargs,
        }
        return e.sql(**kwargs)


class SparkEngineAdapter(EngineAdapter):
    def __init__(
        self,
        connection_factory: t.Callable[[], t.Any],
        multithreaded: bool = False,
    ):
        super().__init__(connection_factory, "spark", multithreaded=multithreaded)

    def alter_table(
        self,
        table_name: str,
        added_columns: t.Dict[str, str],
        dropped_columns: t.Sequence[str],
    ) -> None:
        alter_table = exp.AlterTable(this=exp.to_table(table_name))

        if added_columns:
            add_columns = exp.Schema(
                expressions=[
                    exp.ColumnDef(this=exp.to_identifier(column_name), kind=column_type)
                    for column_name, column_type in added_columns.items()
                ],
            )
            alter_table.set("actions", [add_columns])
            self.execute(alter_table)

        if dropped_columns:
            drop_columns = exp.Drop(
                this=exp.Schema(
                    expressions=[
                        exp.to_identifier(column_name)
                        for column_name in dropped_columns
                    ]
                ),
                kind="COLUMNS",
            )
            alter_table.set("actions", [drop_columns])
            self.execute(alter_table)


def create_engine_adapter(
    connection_factory: t.Callable[[], t.Any], dialect: str, multithreaded: bool = False
) -> EngineAdapter:
    if dialect.lower() == "spark":
        return SparkEngineAdapter(connection_factory, multithreaded=multithreaded)
    return EngineAdapter(connection_factory, dialect, multithreaded=multithreaded)
