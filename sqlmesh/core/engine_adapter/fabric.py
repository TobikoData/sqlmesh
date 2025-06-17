from __future__ import annotations

import typing as t
from sqlglot import exp
from sqlmesh.core.engine_adapter.mssql import MSSQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    InsertOverwriteStrategy,
    SourceQuery,
    DataObject,
    DataObjectType,
)
import logging
from sqlmesh.core.dialect import to_schema

logger = logging.getLogger(__name__)
if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import QueryOrDF


class FabricAdapter(MSSQLEngineAdapter):
    """
    Adapter for Microsoft Fabric.
    """

    DIALECT = "fabric"
    SUPPORTS_INDEXES = False
    SUPPORTS_TRANSACTIONS = False

    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT

    def __init__(self, *args: t.Any, **kwargs: t.Any):
        self.database = kwargs.get("database")

        super().__init__(*args, **kwargs)

        if not self.database:
            raise ValueError(
                "The 'database' parameter is required in the connection config for the FabricWarehouseAdapter."
            )
        try:
            self.execute(f"USE [{self.database}]")
        except Exception as e:
            raise RuntimeError(f"Failed to set database context to '{self.database}'. Reason: {e}")

    def _get_schema_name(self, name: t.Union[str, exp.Table, exp.Identifier]) -> t.Optional[str]:
        """
        Safely extracts the schema name from a table or schema name, which can be
        a string or a sqlglot expression.

        Fabric requires database names to be explicitly specified in many contexts,
        including referencing schemas in INFORMATION_SCHEMA. This function helps
        in extracting the schema part correctly from potentially qualified names.
        """
        table = exp.to_table(name)

        if table.this and table.this.name.startswith("#"):
            return None

        schema_part = table.db

        if not schema_part:
            return None

        if isinstance(schema_part, exp.Identifier):
            return schema_part.name
        if isinstance(schema_part, str):
            return schema_part

        raise TypeError(f"Unexpected type for schema part: {type(schema_part)}")

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and database.

        Overridden to query `INFORMATION_SCHEMA.TABLES` with explicit database qualification
        and preserved casing using `quoted=True`.
        """
        import pandas as pd

        catalog = self.get_current_catalog()

        from_table = exp.Table(
            this=exp.to_identifier("TABLES", quoted=True),
            db=exp.to_identifier("INFORMATION_SCHEMA", quoted=True),
            catalog=exp.to_identifier(self.database),
        )

        query = (
            exp.select(
                exp.column("TABLE_NAME").as_("name"),
                exp.column("TABLE_SCHEMA").as_("schema_name"),
                exp.case()
                .when(exp.column("TABLE_TYPE").eq("BASE TABLE"), exp.Literal.string("TABLE"))
                .else_(exp.column("TABLE_TYPE"))
                .as_("type"),
            )
            .from_(from_table)
            .where(exp.column("TABLE_SCHEMA").eq(str(to_schema(schema_name).db).strip("[]")))
        )
        if object_names:
            query = query.where(
                exp.column("TABLE_NAME").isin(*(name.strip("[]") for name in object_names))
            )

        dataframe: pd.DataFrame = self.fetchdf(query)

        return [
            DataObject(
                catalog=catalog,
                schema=row.schema_name,
                name=row.name,
                type=DataObjectType.from_str(row.type),
            )
            for row in dataframe.itertuples()
        ]

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
        **kwargs: t.Any,
    ) -> None:
        """
        Creates a schema in a Microsoft Fabric Warehouse.

        Overridden to handle Fabric's specific T-SQL requirements.
        T-SQL's `CREATE SCHEMA` command does not support `IF NOT EXISTS` directly
        as part of the statement in all contexts, and error messages suggest
        issues with batching or preceding statements like USE.
        """
        if schema_name is None:
            return

        schema_name_str = (
            schema_name.name if isinstance(schema_name, exp.Identifier) else str(schema_name)
        )

        if not schema_name_str:
            logger.warning("Attempted to create a schema with an empty name. Skipping.")
            return

        schema_name_str = schema_name_str.strip('[]"').rstrip(".")

        if not schema_name_str:
            logger.warning(
                "Attempted to create a schema with an empty name after sanitization. Skipping."
            )
            return

        try:
            if self.schema_exists(schema_name_str):
                if ignore_if_exists:
                    return
                raise RuntimeError(f"Schema '{schema_name_str}' already exists.")
        except Exception as e:
            if warn_on_error:
                logger.warning(f"Failed to check for existence of schema '{schema_name_str}': {e}")
            else:
                raise

        try:
            create_sql = f"CREATE SCHEMA [{schema_name_str}]"
            self.execute(create_sql)
        except Exception as e:
            if "already exists" in str(e).lower() or "There is already an object named" in str(e):
                if ignore_if_exists:
                    return
                raise RuntimeError(f"Schema '{schema_name_str}' already exists.") from e
            else:
                if warn_on_error:
                    logger.warning(f"Failed to create schema {schema_name_str}. Reason: {e}")
                else:
                    raise RuntimeError(f"Failed to create schema {schema_name_str}.") from e

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
        Creates a table, ensuring the schema exists first and that all
        object names are fully qualified with the database.
        """
        table_exp = exp.to_table(table_name)
        schema_name = self._get_schema_name(table_name)

        self.create_schema(schema_name)

        fully_qualified_table_name = f"[{self.database}].[{schema_name}].[{table_exp.name}]"

        column_defs = ", ".join(
            f"[{col}] {kind.sql(dialect=self.dialect)}" for col, kind in columns_to_types.items()
        )

        create_table_sql = f"CREATE TABLE {fully_qualified_table_name} ({column_defs})"

        if not exists:
            self.execute(create_table_sql)
            return

        if not self.table_exists(table_name):
            self.execute(create_table_sql)

        if table_description and self.comments_enabled:
            qualified_table_for_comment = self._fully_qualify(table_name)
            self._create_table_comment(qualified_table_for_comment, table_description)
            if column_descriptions and self.comments_enabled:
                self._create_column_comments(qualified_table_for_comment, column_descriptions)

    def table_exists(self, table_name: TableName) -> bool:
        """
        Checks if a table exists.

        Overridden to query the uppercase `INFORMATION_SCHEMA` required
        by case-sensitive Fabric environments.
        """
        table = exp.to_table(table_name)
        schema = self._get_schema_name(table_name)

        sql = (
            exp.select("1")
            .from_("INFORMATION_SCHEMA.TABLES")
            .where(f"TABLE_NAME = '{table.alias_or_name}'")
            .where(f"TABLE_SCHEMA = '{schema}'")
        )

        result = self.fetchone(sql, quote_identifiers=True)

        return result[0] == 1 if result else False

    def _fully_qualify(self, name: t.Union[TableName, SchemaName]) -> exp.Table:
        """
        Ensures an object name is prefixed with the configured database and schema.

        Overridden to prevent qualification for temporary objects (starting with # or ##).
        Temporary objects should not be qualified with database or schema in T-SQL.
        """
        table = exp.to_table(name)

        if (
            table.this
            and isinstance(table.this, exp.Identifier)
            and (table.this.name.startswith("#"))
        ):
            temp_identifier = exp.Identifier(this=table.this.this, quoted=True)
            return exp.Table(this=temp_identifier)

        schema = self._get_schema_name(name)

        return exp.Table(
            this=table.this,
            db=exp.to_identifier(schema) if schema else None,
            catalog=exp.to_identifier(self.database),
        )

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
        """
        Creates a view from a query or DataFrame.

        Overridden to ensure that the view name and all tables referenced
        in the source query are fully qualified with the database name,
        as required by Fabric.
        """
        view_schema = self._get_schema_name(view_name)
        self.create_schema(view_schema)

        qualified_view_name = self._fully_qualify(view_name)

        if isinstance(query_or_df, exp.Expression):
            for table in query_or_df.find_all(exp.Table):
                if not table.catalog:
                    qualified_table = self._fully_qualify(table)
                    table.replace(qualified_table)

        return super().create_view(
            qualified_view_name,
            query_or_df,
            columns_to_types,
            replace,
            materialized,
            table_description=table_description,
            column_descriptions=column_descriptions,
            view_properties=view_properties,
            **create_kwargs,
        )

    def columns(
        self, table_name: TableName, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        table = exp.to_table(table_name)
        schema = self._get_schema_name(table_name)

        if (
            not schema
            and table.this
            and isinstance(table.this, exp.Identifier)
            and table.this.name.startswith("__temp_")
        ):
            schema = "dbo"

        if not schema:
            logger.warning(
                f"Cannot fetch columns for table '{table_name}' without a schema name in Fabric."
            )
            return {}

        from_table = exp.Table(
            this=exp.to_identifier("COLUMNS", quoted=True),
            db=exp.to_identifier("INFORMATION_SCHEMA", quoted=True),
            catalog=exp.to_identifier(self.database),
        )

        sql = (
            exp.select(
                "COLUMN_NAME",
                "DATA_TYPE",
                "CHARACTER_MAXIMUM_LENGTH",
                "NUMERIC_PRECISION",
                "NUMERIC_SCALE",
            )
            .from_(from_table)
            .where(f"TABLE_NAME = '{table.name.strip('[]')}'")
            .where(f"TABLE_SCHEMA = '{schema.strip('[]')}'")
            .order_by("ORDINAL_POSITION")
        )

        df = self.fetchdf(sql)

        def build_var_length_col(
            column_name: str,
            data_type: str,
            character_maximum_length: t.Optional[int] = None,
            numeric_precision: t.Optional[int] = None,
            numeric_scale: t.Optional[int] = None,
        ) -> t.Tuple[str, str]:
            data_type = data_type.lower()

            char_len_int = (
                int(character_maximum_length) if character_maximum_length is not None else None
            )
            prec_int = int(numeric_precision) if numeric_precision is not None else None
            scale_int = int(numeric_scale) if numeric_scale is not None else None

            if data_type in self.VARIABLE_LENGTH_DATA_TYPES and char_len_int is not None:
                if char_len_int > 0:
                    return (column_name, f"{data_type}({char_len_int})")
                if char_len_int == -1:
                    return (column_name, f"{data_type}(max)")
            if (
                data_type in ("decimal", "numeric")
                and prec_int is not None
                and scale_int is not None
            ):
                return (column_name, f"{data_type}({prec_int}, {scale_int})")
            if data_type == "float" and prec_int is not None:
                return (column_name, f"{data_type}({prec_int})")

            return (column_name, data_type)

        columns_raw = [
            (
                row.COLUMN_NAME,
                row.DATA_TYPE,
                getattr(row, "CHARACTER_MAXIMUM_LENGTH", None),
                getattr(row, "NUMERIC_PRECISION", None),
                getattr(row, "NUMERIC_SCALE", None),
            )
            for row in df.itertuples()
        ]

        columns_processed = [build_var_length_col(*row) for row in columns_raw]

        return {
            column_name: exp.DataType.build(data_type, dialect=self.dialect)
            for column_name, data_type in columns_processed
        }

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
        **kwargs: t.Any,
    ) -> None:
        if schema_name is None:
            return

        schema_exp = to_schema(schema_name)
        simple_schema_name_str = None
        if schema_exp.db:
            simple_schema_name_str = exp.to_identifier(schema_exp.db).name

        if not simple_schema_name_str:
            logger.warning(
                f"Could not determine simple schema name from '{schema_name}'. Skipping schema creation."
            )
            return

        if ignore_if_exists:
            try:
                if self.schema_exists(simple_schema_name_str):
                    return
            except Exception as e:
                if warn_on_error:
                    logger.warning(
                        f"Failed to check for existence of schema '{simple_schema_name_str}': {e}"
                    )
                else:
                    raise
        elif self.schema_exists(simple_schema_name_str):
            raise RuntimeError(f"Schema '{simple_schema_name_str}' already exists.")

        try:
            create_sql = f"CREATE SCHEMA [{simple_schema_name_str}]"
            self.execute(create_sql)
        except Exception as e:
            error_message = str(e).lower()
            if (
                "already exists" in error_message
                or "there is already an object named" in error_message
            ):
                if ignore_if_exists:
                    return
                raise RuntimeError(
                    f"Schema '{simple_schema_name_str}' already exists due to race condition."
                ) from e
            else:
                if warn_on_error:
                    logger.warning(f"Failed to create schema {simple_schema_name_str}. Reason: {e}")
                else:
                    raise RuntimeError(f"Failed to create schema {simple_schema_name_str}.") from e

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        """
        Implements the insert overwrite strategy for Fabric.

        Overridden to enforce a `DELETE`/`INSERT` strategy, as Fabric's
        `MERGE` statement has limitations.
        """

        columns_to_types = columns_to_types or self.columns(table_name)

        self.delete_from(table_name, where=where or exp.true())

        for source_query in source_queries:
            with source_query as query:
                query = self._order_projections_and_filter(query, columns_to_types)
                self._insert_append_query(
                    table_name,
                    query,
                    columns_to_types=columns_to_types,
                    order_projections=False,
                )
