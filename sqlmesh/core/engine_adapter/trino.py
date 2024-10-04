from __future__ import annotations
import typing as t
from functools import lru_cache
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype  # type: ignore
from sqlglot import exp
from sqlglot.helper import seq_get
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_result

from sqlmesh.core.dialect import schema_, to_schema
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    HiveMetastoreTablePropertiesMixin,
    PandasNativeFetchDFSupportMixin,
    RowDiffMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    InsertOverwriteStrategy,
    SourceQuery,
    set_catalog,
)
from sqlmesh.core.schema_diff import SchemaDiffer
from sqlmesh.utils.date import TimeLike

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF


@set_catalog()
class TrinoEngineAdapter(
    PandasNativeFetchDFSupportMixin,
    HiveMetastoreTablePropertiesMixin,
    GetCurrentCatalogFromFunctionMixin,
    RowDiffMixin,
):
    DIALECT = "trino"
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.INTO_IS_OVERWRITE
    CATALOG_SUPPORT = CatalogSupport.FULL_SUPPORT
    # Trino does technically support transactions but it doesn't work correctly with partition overwrite so we
    # disable transactions. If we need to get them enabled again then we would need to disable auto commit on the
    # connector and then figure out how to get insert/overwrite to work correctly without it.
    SUPPORTS_TRANSACTIONS = False
    CURRENT_CATALOG_EXPRESSION = exp.column("current_catalog")
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.COMMENT_COMMAND_ONLY
    SUPPORTS_REPLACE_TABLE = False
    DEFAULT_CATALOG_TYPE = "hive"
    QUOTE_IDENTIFIERS_IN_VIEWS = False
    SCHEMA_DIFFER = SchemaDiffer(
        parameterized_type_defaults={
            # default decimal precision varies across backends
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(), (0,)],
            exp.DataType.build("CHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("TIMESTAMP", dialect=DIALECT).this: [(3,)],
        },
    )
    # some catalogs support microsecond (precision 6) but it has to be specifically enabled (Hive) or just isnt available (Delta / TIMESTAMP WITH TIME ZONE)
    # and even if you have a TIMESTAMP(6) the date formatting functions still only support millisecond precision
    MAX_TIMESTAMP_PRECISION = 3

    def set_current_catalog(self, catalog: str) -> None:
        """Sets the catalog name of the current connection."""
        self.execute(exp.Use(this=schema_(db="information_schema", catalog=catalog)))

    @lru_cache()
    def get_catalog_type(self, catalog: t.Optional[str]) -> str:
        row: t.Tuple = tuple()
        if catalog:
            row = (
                self.fetchone(
                    f"select connector_name from system.metadata.catalogs where catalog_name='{catalog}'"
                )
                or ()
            )
        return seq_get(row, 0) or self.DEFAULT_CATALOG_TYPE

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        catalog = exp.to_table(table_name).catalog or self.get_current_catalog()

        if where and self.get_catalog_type(catalog) == "hive":
            # These session properties are only valid for the Trino Hive connector
            # Attempting to set them on an Iceberg catalog will throw an error:
            # "Session property 'catalog.insert_existing_partitions_behavior' does not exist"
            self.execute(f"SET SESSION {catalog}.insert_existing_partitions_behavior='OVERWRITE'")
            super()._insert_overwrite_by_condition(
                table_name, source_queries, columns_to_types, where
            )
            self.execute(f"SET SESSION {catalog}.insert_existing_partitions_behavior='APPEND'")
        else:
            super()._insert_overwrite_by_condition(
                table_name,
                source_queries,
                columns_to_types,
                where,
                insert_overwrite_strategy_override=InsertOverwriteStrategy.DELETE_INSERT,
            )

    def _truncate_table(self, table_name: TableName) -> None:
        table = exp.to_table(table_name)
        # Some trino connectors don't support truncate so we use delete.
        self.execute(f"DELETE FROM {table.sql(dialect=self.dialect, identify=True)}")

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        schema_name = to_schema(schema_name)
        schema = schema_name.db
        catalog = schema_name.catalog or self.get_current_catalog()
        query = (
            exp.select(
                exp.column("table_catalog", table="t").as_("catalog"),
                exp.column("table_schema", table="t").as_("schema"),
                exp.column("table_name", table="t").as_("name"),
                exp.case()
                .when(
                    exp.column("name", table="mv").is_(exp.null()).not_(),
                    exp.Literal.string("materialized_view"),
                )
                .when(
                    exp.column("table_type", table="t").eq("BASE TABLE"),
                    exp.Literal.string("table"),
                )
                .else_(exp.column("table_type", table="t"))
                .as_("type"),
            )
            .from_(exp.to_table(f"{catalog}.information_schema.tables", alias="t"))
            .join(
                exp.to_table("system.metadata.materialized_views", alias="mv"),
                on=exp.and_(
                    exp.column("catalog_name", table="mv").eq(
                        exp.column("table_catalog", table="t")
                    ),
                    exp.column("schema_name", table="mv").eq(exp.column("table_schema", table="t")),
                    exp.column("name", table="mv").eq(exp.column("table_name", table="t")),
                ),
                join_type="left",
            )
            .where(
                exp.and_(
                    exp.column("table_schema", table="t").eq(schema),
                    exp.or_(
                        exp.column("catalog_name", table="mv").is_(exp.null()),
                        exp.column("catalog_name", table="mv").eq(catalog),
                    ),
                    exp.or_(
                        exp.column("schema_name", table="mv").is_(exp.null()),
                        exp.column("schema_name", table="mv").eq(schema),
                    ),
                )
            )
        )
        if object_names:
            query = query.where(exp.column("table_name", table="t").isin(*object_names))
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=row.catalog,  # type: ignore
                schema=row.schema,  # type: ignore
                name=row.name,  # type: ignore
                type=DataObjectType.from_str(row.type),  # type: ignore
            )
            for row in df.itertuples()
        ]

    def _df_to_source_queries(
        self,
        df: DF,
        columns_to_types: t.Dict[str, exp.DataType],
        batch_size: int,
        target_table: TableName,
    ) -> t.List[SourceQuery]:
        assert isinstance(df, pd.DataFrame)

        # Trino does not accept timestamps in ISOFORMAT that include the "T". `execution_time` is stored in
        # Pandas with that format, so we convert the column to a string with the proper format and CAST to
        # timestamp in Trino.
        for column, kind in (columns_to_types or {}).items():
            dtype = df.dtypes[column]
            if is_datetime64_any_dtype(dtype) and getattr(dtype, "tz", None) is not None:
                df[column] = pd.to_datetime(df[column]).map(lambda x: x.isoformat(" "))

        return super()._df_to_source_queries(df, columns_to_types, batch_size, target_table)

    def _build_schema_exp(
        self,
        table: exp.Table,
        columns_to_types: t.Dict[str, exp.DataType],
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        expressions: t.Optional[t.List[exp.PrimaryKey]] = None,
        is_view: bool = False,
    ) -> exp.Schema:
        if self.current_catalog_type == "delta_lake":
            columns_to_types = self._to_delta_ts(columns_to_types)

        return super()._build_schema_exp(
            table, columns_to_types, column_descriptions, expressions, is_view
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
        **kwargs: t.Any,
    ) -> None:
        if columns_to_types and self.current_catalog_type == "delta_lake":
            columns_to_types = self._to_delta_ts(columns_to_types)

        return super()._scd_type_2(
            target_table,
            source_table,
            unique_key,
            valid_from_col,
            valid_to_col,
            execution_time,
            invalidate_hard_deletes,
            updated_at_col,
            check_columns,
            updated_at_as_valid_from,
            execution_time_as_valid_from,
            columns_to_types,
            table_description,
            column_descriptions,
            truncate,
            **kwargs,
        )

    # delta_lake only supports two timestamp data types. This method converts other
    # timestamp types to those two for use in DDL statements. Trino/delta automatically
    # converts the data values to the correct type on write, so we only need to handle
    # the column types in DDL.
    # - `timestamp(6)` for non-timezone-aware
    # - `timestamp(3) with time zone` for timezone-aware
    # https://trino.io/docs/current/connector/delta-lake.html#delta-lake-to-trino-type-mapping
    def _to_delta_ts(
        self, columns_to_types: t.Dict[str, exp.DataType]
    ) -> t.Dict[str, exp.DataType]:
        ts6 = exp.DataType.build("timestamp(6)")
        ts3_tz = exp.DataType.build("timestamp(3) with time zone")

        delta_columns_to_types = {
            k: ts6 if v.is_type(exp.DataType.Type.TIMESTAMP) else v
            for k, v in columns_to_types.items()
        }

        delta_columns_to_types = {
            k: ts3_tz if v.is_type(exp.DataType.Type.TIMESTAMPTZ) else v
            for k, v in delta_columns_to_types.items()
        }

        return delta_columns_to_types

    @retry(wait=wait_fixed(1), stop=stop_after_attempt(10), retry=retry_if_result(lambda v: not v))
    def _block_until_table_exists(self, table_name: TableName) -> bool:
        return self.table_exists(table_name)

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
        super()._create_table(
            table_name_or_schema=table_name_or_schema,
            expression=expression,
            exists=exists,
            replace=replace,
            columns_to_types=columns_to_types,
            table_description=table_description,
            column_descriptions=column_descriptions,
            table_kind=table_kind,
            **kwargs,
        )

        # extract the table name
        if isinstance(table_name_or_schema, exp.Schema):
            table_name = table_name_or_schema.this
            assert isinstance(table_name, exp.Table)
        else:
            table_name = table_name_or_schema

        if self.current_catalog_type == "hive":
            # the Trino Hive connector can take a few seconds for metadata changes to propagate to all internal threads
            # (even if metadata TTL is set to 0s)
            # Blocking until the table shows up means that subsequent code expecting it to exist immediately will not fail
            self._block_until_table_exists(table_name)
