from __future__ import annotations

import typing as t
import logging
import pandas as pd
from sqlglot import exp

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    DataObject,
    DataObjectType,
    SourceQuery,
)
from sqlmesh.core.schema_diff import SchemaDiffer

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query

    from sqlmesh.core.node import IntervalUnit

logger = logging.getLogger(__name__)


class ClickhouseEngineAdapter(EngineAdapter):
    DIALECT = "clickhouse"
    SUPPORTS_TRANSACTIONS = False
    # TODO: indexes are implicit for MergeTree table engines via PRIMARY KEY or ORDER BY
    # - Indexes can be manipulated with ALTER **AFTER** a table has been created, though
    SUPPORTS_INDEXES = False

    SCHEMA_DIFFER = SchemaDiffer()

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
    ) -> None:
        """Create a Clickhouse database from a name or qualified table name.

        Clickhouse has a two-level naming scheme [database].[table].
        """
        try:
            self.execute(
                exp.Create(
                    this=to_schema(schema_name),
                    kind="DATABASE",
                    exists=ignore_if_exists,
                )
            )
        except Exception as e:
            if not warn_on_error:
                raise
            logger.warning("Failed to create database '%s': %s", schema_name, e)

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
    ) -> None:
        self.execute(
            exp.Drop(
                this=to_schema(schema_name),
                kind="DATABASE",
                exists=ignore_if_not_exists,
                cascade=False,
            )
        )

    # TODO: `RENAME` is valid SQL, but `EXCHANGE` is an atomic swap
    # def _rename_table(
    #     self,
    #     old_table_name: TableName,
    #     new_table_name: TableName,
    # ) -> None:
    #     self.execute(f"EXCHANGE TABLES {old_table_name} AND {new_table_name}")

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> pd.DataFrame:
        """Fetches a Pandas DataFrame from the cursor"""
        return self.cursor.client.query_df(
            self._to_sql(query, quote=quote_identifiers)
            if isinstance(query, exp.Expression)
            else query
        )

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        temp_table = self._get_temp_table(target_table)

        def query_factory() -> Query:
            # TODO: should we pick a temp table engine ourselves or use the one passed to the model?
            # - If the latter, figure out how to pass it
            self.create_table(
                temp_table, columns_to_types, table_properties={"engine": exp.Var(this="Log")}
            )

            self.cursor.client.insert_df(temp_table.sql(dialect=self.dialect), df=df)

            return exp.select(*self._casted_columns(columns_to_types)).from_(temp_table)

        return [
            SourceQuery(
                query_factory=query_factory, cleanup_func=lambda: self.drop_table(temp_table)
            )
        ]

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given database.
        """
        query = (
            exp.select(
                exp.column("database").as_("schema_name"),
                exp.column("name"),
                exp.column("engine").as_("type"),
            )
            .from_("system.tables")
            .where(exp.column("database").eq(to_schema(schema_name).db))
        )
        if object_names:
            query = query.where(exp.column("name").isin(*object_names))
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=None,
                schema=row.schema_name,
                name=row.name,
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]

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

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        if partitioned_by:
            properties.append(
                exp.PartitionedByProperty(
                    this=exp.Schema(expressions=partitioned_by),
                )
            )

        if table_properties:
            # TODO ENGINE PROPERTY: address parsing/quoting of user input values
            # - bare values are parsed into column identifiers, so `quote_identifiers` incorrectly adds quotes
            # - single-quoted values are parsed into strings that include quotes
            table_properties_copy = table_properties.copy()
            engine_property = table_properties_copy.pop(
                "engine", None
            ) or table_properties_copy.pop("ENGINE", None)
            if engine_property:
                properties.append(exp.EngineProperty(this=exp.Var(this="Log")))

            table_properties_copy = {k.upper(): v for k, v in table_properties_copy.items()}

            properties.extend(self._table_or_view_properties_to_expressions(table_properties_copy))

        if properties:
            return exp.Properties(expressions=properties)

        return None
