from __future__ import annotations
from functools import lru_cache
import typing as t
import logging
from sqlglot import exp
from sqlmesh.core.dialect import to_schema
from sqlmesh.core.config.common import validate_s3_location
from sqlmesh.core.engine_adapter.mixins import PandasNativeFetchDFSupportMixin
from sqlmesh.core.engine_adapter.trino import TrinoEngineAdapter
from sqlmesh.core.node import IntervalUnit
import os
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    DataObject,
    DataObjectType,
    CommentCreationTable,
    CommentCreationView,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName

logger = logging.getLogger(__name__)


class AthenaEngineAdapter(PandasNativeFetchDFSupportMixin):
    DIALECT = "athena"
    SUPPORTS_TRANSACTIONS = False
    SUPPORTS_REPLACE_TABLE = False
    # Athena has the concept of catalogs but no notion of current_catalog or setting the current catalog
    CATALOG_SUPPORT = CatalogSupport.UNSUPPORTED
    # Athena's support for table and column comments is too patchy to consider "supported"
    # Hive tables: Table + Column comments are supported
    # Iceberg tables: Column comments only
    # CTAS, Views: No comment support at all
    COMMENT_CREATION_TABLE = CommentCreationTable.UNSUPPORTED
    COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED
    SCHEMA_DIFFER = TrinoEngineAdapter.SCHEMA_DIFFER

    def __init__(
        self, *args: t.Any, s3_warehouse_location: t.Optional[str] = None, **kwargs: t.Any
    ):
        # Need to pass s3_warehouse_location to the superclass so that it goes into _extra_config
        # which means that EngineAdapter.with_log_level() keeps this property when it makes a clone
        super().__init__(*args, s3_warehouse_location=s3_warehouse_location, **kwargs)
        self.s3_warehouse_location = s3_warehouse_location

    @property
    def s3_warehouse_location(self) -> t.Optional[str]:
        return self._s3_warehouse_location

    @s3_warehouse_location.setter
    def s3_warehouse_location(self, value: t.Optional[str]) -> None:
        if value:
            value = validate_s3_location(value)
        self._s3_warehouse_location = value

    def create_state_table(
        self,
        table_name: str,
        columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
    ) -> None:
        self.create_table(
            table_name,
            columns_to_types,
            primary_key=primary_key,
            table_properties={
                # it's painfully slow, but it works
                "table_type": exp.Literal.string("iceberg")
            },
        )

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """
        Returns all the data objects that exist in the given schema and optionally catalog.
        """
        schema_name = to_schema(schema_name)
        schema = schema_name.db
        query = (
            exp.select(
                exp.case()
                .when(
                    # calling code expects data objects in the default catalog to have their catalog set to None
                    exp.column("table_catalog", table="t").eq("awsdatacatalog"),
                    exp.Null(),
                )
                .else_(exp.column("table_catalog"))
                .as_("catalog"),
                exp.column("table_schema", table="t").as_("schema"),
                exp.column("table_name", table="t").as_("name"),
                exp.case()
                .when(
                    exp.column("table_type", table="t").eq("BASE TABLE"),
                    exp.Literal.string("table"),
                )
                .else_(exp.column("table_type", table="t"))
                .as_("type"),
            )
            .from_(exp.to_table("information_schema.tables", alias="t"))
            .where(exp.column("table_schema", table="t").eq(schema))
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

    def columns(
        self, table_name: TableName, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        table = exp.to_table(table_name)
        query = (
            exp.select("column_name", "data_type")
            .from_("information_schema.columns")
            .where(exp.column("table_schema").eq(table.db), exp.column("table_name").eq(table.name))
            .order_by("ordinal_position")
        )
        result = self.fetchdf(query, quote_identifiers=True)
        return {
            str(r.column_name): exp.DataType.build(str(r.data_type))
            for r in result.itertuples(index=False)
        }

    def _create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool,
        warn_on_error: bool,
        properties: t.List[exp.Expression],
        kind: str,
    ) -> None:
        if location := self._table_location(table_properties=None, table=exp.to_table(schema_name)):
            # don't add extra LocationProperty's if one already exists
            if not any(p for p in properties if isinstance(p, exp.LocationProperty)):
                properties.append(location)

        return super()._create_schema(
            schema_name=schema_name,
            ignore_if_exists=ignore_if_exists,
            warn_on_error=warn_on_error,
            properties=properties,
            kind=kind,
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
        partitioned_by: t.Optional[t.List[exp.Expression]] = None,
        table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        **kwargs: t.Any,
    ) -> exp.Create:
        exists = False if replace else exists

        table: exp.Table
        if isinstance(table_name_or_schema, str):
            table = exp.to_table(table_name_or_schema)
        elif isinstance(table_name_or_schema, exp.Schema):
            table = table_name_or_schema.this
        else:
            table = table_name_or_schema

        properties = self._build_table_properties_exp(
            table=table,
            expression=expression,
            columns_to_types=columns_to_types,
            partitioned_by=partitioned_by,
            table_properties=table_properties,
            table_description=table_description,
            table_kind=table_kind,
            **kwargs,
        )

        is_hive = self._table_type(table_properties) == "hive"

        # Filter any PARTITIONED BY properties from the main column list since they cant be specified in both places
        # ref: https://docs.aws.amazon.com/athena/latest/ug/partitions.html
        if is_hive and partitioned_by and isinstance(table_name_or_schema, exp.Schema):
            partitioned_by_column_names = {e.name for e in partitioned_by}
            filtered_expressions = [
                e
                for e in table_name_or_schema.expressions
                if isinstance(e, exp.ColumnDef) and e.this.name not in partitioned_by_column_names
            ]
            table_name_or_schema.args["expressions"] = filtered_expressions

        return exp.Create(
            this=table_name_or_schema,
            kind=table_kind or "TABLE",
            replace=replace,
            exists=exists,
            expression=expression,
            properties=properties,
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
        table: t.Optional[exp.Table] = None,
        expression: t.Optional[exp.Expression] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        properties: t.List[exp.Expression] = []
        table_properties = table_properties or {}

        is_hive = self._table_type(table_properties) == "hive"
        is_iceberg = not is_hive

        if is_hive and not expression:
            # Hive tables are CREATE EXTERNAL TABLE, Iceberg tables are CREATE TABLE
            # Unless it's a CTAS, those are always CREATE TABLE
            properties.append(exp.ExternalProperty())

        if table_description:
            properties.append(exp.SchemaCommentProperty(this=exp.Literal.string(table_description)))

        if partitioned_by:
            schema_expressions: t.List[exp.Expression] = []
            if is_hive and columns_to_types:
                # For Hive-style tables, you cannot include the partitioned by columns in the main set of columns
                # In the PARTITIONED BY expression, you also cant just include the column names, you need to include the data type as well
                # ref: https://docs.aws.amazon.com/athena/latest/ug/partitions.html
                for match_name, match_dtype in self._find_matching_columns(
                    partitioned_by, columns_to_types
                ):
                    column_def = exp.ColumnDef(this=exp.to_identifier(match_name), kind=match_dtype)
                    schema_expressions.append(column_def)
            else:
                schema_expressions = partitioned_by

            properties.append(
                exp.PartitionedByProperty(this=exp.Schema(expressions=schema_expressions))
            )

        if clustered_by:
            # Athena itself supports CLUSTERED BY, via the syntax CLUSTERED BY (col) INTO <n> BUCKETS
            # However, SQLMesh is more closely aligned with BigQuery's notion of clustering and
            # defines `clustered_by` as a List[str] with no way of indicating the number of buckets
            #
            # Athena's concept of CLUSTER BY is more like Iceberg's `bucket(<num_buckets>, col)` partition transform
            logging.warning("clustered_by is not supported in the Athena adapter at this time")

        if storage_format:
            if is_iceberg:
                # TBLPROPERTIES('format'='parquet')
                table_properties["format"] = exp.Literal.string(storage_format)
            else:
                # STORED AS PARQUET
                properties.append(exp.FileFormatProperty(this=storage_format))

        if table and (location := self._table_location_or_raise(table_properties, table)):
            properties.append(location)

            if is_iceberg and expression:
                # To make a CTAS expression persist as iceberg, alongside setting `table_type=iceberg`, you also need to set is_external=false
                # Note that SQLGlot does the right thing with LocationProperty and writes it as `location` (Iceberg) instead of `external_location` (Hive)
                # ref: https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties
                properties.append(exp.Property(this=exp.var("is_external"), value="false"))

        for name, value in table_properties.items():
            properties.append(exp.Property(this=exp.var(name), value=value))

        if properties:
            return exp.Properties(expressions=properties)

        return None

    def _truncate_table(self, table_name: TableName) -> None:
        table = exp.to_table(table_name)
        # Athena doesnt support TRUNCATE TABLE. The closest thing is DELETE FROM <table> but it only works on Iceberg
        self.execute(f"DELETE FROM {table.sql(dialect=self.dialect, identify=True)}")

    def _table_type(
        self, table_properties: t.Optional[t.Dict[str, exp.Expression]] = None
    ) -> t.Union[t.Literal["hive"], t.Literal["iceberg"]]:
        """
        Use the user-specified table_properties to figure out of this is a Hive or an Iceberg table
        """
        # if we cant detect any indication of Iceberg, this is a Hive table
        if table_properties and (table_type := table_properties.get("table_type", None)):
            if "iceberg" in table_type.sql(dialect=self.dialect).lower():
                return "iceberg"
        return "hive"

    @lru_cache()
    def _query_table_type(
        self, table_name: TableName
    ) -> t.Union[t.Literal["hive"], t.Literal["iceberg"]]:
        """
        Hit the DB to check if this is a Hive or an Iceberg table
        """
        table_name = exp.to_table(table_name)
        # Note: SHOW TBLPROPERTIES gets parsed by SQLGlot as an exp.Command anyway so we just use a string here
        # This also means we need to use dialect="hive" instead of dialect="athena" so that the identifiers get the correct quoting (backticks)
        for row in self.fetchall(f"SHOW TBLPROPERTIES {table_name.sql(dialect='hive')}"):
            # This query returns a single column with values like 'EXTERNAL\tTRUE'
            row_lower = row[0].lower()
            if "external" in row_lower and "true" in row_lower:
                return "hive"
        return "iceberg"

    def _table_location_or_raise(
        self, table_properties: t.Optional[t.Dict[str, exp.Expression]], table: exp.Table
    ) -> exp.LocationProperty:
        location = self._table_location(table_properties, table)
        if not location:
            raise SQLMeshError(
                f"Cannot figure out location for table {table}. Please either set `s3_base_location` in `physical_properties` or set `s3_warehouse_location` in the Athena connection config"
            )
        return location

    def _table_location(
        self,
        table_properties: t.Optional[t.Dict[str, exp.Expression]],
        table: exp.Table,
    ) -> t.Optional[exp.LocationProperty]:
        base_uri: str

        # If the user has manually specified a `s3_base_location`, use it
        if table_properties and "s3_base_location" in table_properties:
            s3_base_location_property = table_properties.pop(
                "s3_base_location"
            )  # pop because it's handled differently and we dont want it to end up in the TBLPROPERTIES clause
            if isinstance(s3_base_location_property, exp.Expression):
                base_uri = s3_base_location_property.name
            else:
                base_uri = s3_base_location_property

        elif self.s3_warehouse_location:
            # If the user has set `s3_warehouse_location` in the connection config, the base URI is <s3_warehouse_location>/<catalog>/<schema>/
            base_uri = os.path.join(self.s3_warehouse_location, table.catalog or "", table.db or "")
        else:
            return None

        full_uri = validate_s3_location(os.path.join(base_uri, table.text("this") or ""))
        return exp.LocationProperty(this=exp.Literal.string(full_uri))

    def _find_matching_columns(
        self, partitioned_by: t.List[exp.Expression], columns_to_types: t.Dict[str, exp.DataType]
    ) -> t.List[t.Tuple[str, exp.DataType]]:
        matches = []
        for col in partitioned_by:
            # TODO: do we care about normalization?
            key = col.name
            if isinstance(col, exp.Column) and (match_dtype := columns_to_types.get(key)):
                matches.append((key, match_dtype))
        return matches

    def delete_from(self, table_name: TableName, where: t.Union[str, exp.Expression]) -> None:
        table_type = self._query_table_type(table_name)

        # If Iceberg, DELETE operations work as expected
        if table_type == "iceberg":
            return super().delete_from(table_name, where)

        # If Hive, DELETE is an error
        if table_type == "hive":
            # However, if the table is empty, we can make DELETE a no-op
            # This simplifies a bunch of calling code that just assumes DELETE works (which to be fair is a reasonable assumption since it does for every other engine)
            empty_check = (
                exp.select("*").from_(table_name).limit(1)
            )  # deliberately not count(*) because we want the engine to stop as soon as it finds a record
            if len(self.fetchall(empty_check)) > 0:
                # TODO: in future, if SQLMesh adds support for explicit partition management, we may
                # be able to covert the DELETE query into an ALTER TABLE DROP PARTITION assuming the WHERE clause fully covers the partition bounds
                raise SQLMeshError("Cannot delete from non-empty Hive table")

        return None

    def _drop_object(
        self,
        name: TableName | SchemaName,
        exists: bool = True,
        kind: str = "TABLE",
        **drop_args: t.Any,
    ) -> None:
        return super()._drop_object(name, exists=exists, kind=kind, **drop_args)
