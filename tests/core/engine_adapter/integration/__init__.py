from __future__ import annotations

import os
import pathlib
import sys
import typing as t

import pandas as pd
import pytest
from sqlglot import exp, parse_one

from sqlmesh import Config, Context, EngineAdapter
from sqlmesh.core.config import load_config_from_paths
from sqlmesh.core.config.connection import AthenaConnectionConfig
from sqlmesh.core.dialect import normalize_model_name
import sqlmesh.core.dialect as d
from sqlmesh.core.engine_adapter import SparkEngineAdapter, TrinoEngineAdapter, AthenaEngineAdapter
from sqlmesh.core.engine_adapter.shared import DataObject
from sqlmesh.core.model.definition import SqlModel, load_sql_based_model
from sqlmesh.utils import random_id
from sqlmesh.utils.date import to_ds
from sqlmesh.utils.pydantic import PydanticModel
from tests.utils.pandas import compare_dataframes

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter._typing import Query

TEST_SCHEMA = "test_schema"


class MetadataResults(PydanticModel):
    tables: t.List[str] = []
    views: t.List[str] = []
    materialized_views: t.List[str] = []
    managed_tables: t.List[str] = []

    @classmethod
    def from_data_objects(cls, data_objects: t.List[DataObject]) -> MetadataResults:
        tables = []
        views = []
        materialized_views = []
        managed_tables = []
        for obj in data_objects:
            if obj.type.is_table:
                tables.append(obj.name)
            elif obj.type.is_view:
                views.append(obj.name)
            elif obj.type.is_materialized_view:
                materialized_views.append(obj.name)
            elif obj.type.is_managed_table:
                managed_tables.append(obj.name)
            else:
                raise ValueError(f"Unexpected object type: {obj.type}")
        return MetadataResults(
            tables=tables,
            views=views,
            materialized_views=materialized_views,
            managed_tables=managed_tables,
        )

    @property
    def non_temp_tables(self) -> t.List[str]:
        return [x for x in self.tables if not x.startswith("__temp") and not x.startswith("temp")]


class TestContext:
    __test__ = False  # prevent pytest trying to collect this as a test class

    def __init__(
        self,
        test_type: str,
        engine_adapter: EngineAdapter,
        mark: str,
        gateway: str,
        is_remote: bool = False,
        columns_to_types: t.Optional[t.Dict[str, t.Union[str, exp.DataType]]] = None,
    ):
        self.test_type = test_type
        self.engine_adapter = engine_adapter
        self.mark = mark
        self.gateway = gateway
        self._columns_to_types = columns_to_types
        self.test_id = random_id(short=True)
        self._context: t.Optional[Context] = None
        self.is_remote = is_remote

    @property
    def columns_to_types(self):
        if self._columns_to_types is None:
            self._columns_to_types = {
                "id": exp.DataType.build("int"),
                "ds": exp.DataType.build("string"),
            }
        return self._columns_to_types

    @columns_to_types.setter
    def columns_to_types(self, value: t.Dict[str, t.Union[str, exp.DataType]]):
        self._columns_to_types = {
            k: exp.DataType.build(v, dialect=self.dialect) for k, v in value.items()
        }

    @property
    def time_columns(self) -> t.List[str]:
        return [
            k
            for k, v in self.columns_to_types.items()
            if v.sql().lower().startswith("timestamp")
            or v.sql().lower().startswith("date")
            or k.lower() == "ds"
        ]

    @property
    def timestamp_columns(self) -> t.List[str]:
        return [
            k
            for k, v in self.columns_to_types.items()
            if v.sql().lower().startswith("timestamp")
            or (v.sql().lower() == "datetime" and self.dialect == "bigquery")
        ]

    @property
    def time_column(self) -> str:
        return self.time_columns[0]

    @property
    def time_formatter(self) -> t.Callable:
        return lambda x, _: exp.Literal.string(to_ds(x))

    @property
    def partitioned_by(self) -> t.List[exp.Expression]:
        return [parse_one(self.time_column)]

    @property
    def dialect(self) -> str:
        return self.engine_adapter.dialect

    @property
    def current_catalog_type(self) -> str:
        return self.engine_adapter.current_catalog_type

    @property
    def supports_merge(self) -> bool:
        if self.dialect == "spark":
            assert isinstance(self.engine_adapter, SparkEngineAdapter)
            # Spark supports MERGE on the Iceberg catalog (which is configured under "testing" in these integration tests)
            return self.engine_adapter.default_catalog == "testing"

        if self.dialect == "trino":
            assert isinstance(self.engine_adapter, TrinoEngineAdapter)
            # Trino supports MERGE on Delta and Iceberg but not Hive
            return (
                self.engine_adapter.get_catalog_type(self.engine_adapter.default_catalog) != "hive"
            )

        if self.dialect == "athena":
            return "hive" not in self.mark

        return True

    @property
    def default_table_format(self) -> t.Optional[str]:
        if self.dialect in {"athena", "trino"} and "_" in self.mark:
            return self.mark.split("_", 1)[-1]  # take eg 'athena_iceberg' and return 'iceberg'
        return None

    def add_test_suffix(self, value: str) -> str:
        return f"{value}_{self.test_id}"

    def get_metadata_results(self, schema: t.Optional[str] = None) -> MetadataResults:
        schema = schema if schema else self.schema(TEST_SCHEMA)
        return MetadataResults.from_data_objects(self.engine_adapter.get_data_objects(schema))

    def _init_engine_adapter(self) -> None:
        schema = self.schema(TEST_SCHEMA)
        self.engine_adapter.drop_schema(schema, ignore_if_not_exists=True, cascade=True)
        self.engine_adapter.create_schema(schema)

    def _format_df(self, data: pd.DataFrame, to_datetime: bool = True) -> pd.DataFrame:
        for timestamp_column in self.timestamp_columns:
            if timestamp_column in data.columns:
                value = data[timestamp_column]
                if to_datetime:
                    value = pd.to_datetime(value)
                data[timestamp_column] = value.astype("datetime64[ns]")
        return data

    def init(self):
        if self.test_type == "pyspark" and not hasattr(self.engine_adapter, "is_pyspark_df"):
            pytest.skip(f"Engine adapter {self.engine_adapter} doesn't support pyspark")
        self._init_engine_adapter()

    def input_data(
        self,
        data: pd.DataFrame,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    ) -> t.Union[Query, pd.DataFrame]:
        columns_to_types = columns_to_types or self.columns_to_types
        if self.test_type == "query":
            return self.engine_adapter._values_to_sql(
                list(data.itertuples(index=False, name=None)),
                batch_start=0,
                batch_end=sys.maxsize,
                columns_to_types=columns_to_types,
            )
        elif self.test_type == "pyspark":
            return self.engine_adapter.spark.createDataFrame(data)  # type: ignore
        return self._format_df(data, to_datetime=self.dialect != "trino")

    def output_data(self, data: pd.DataFrame) -> pd.DataFrame:
        return self._format_df(data)

    def table(self, table_name: str, schema: str = TEST_SCHEMA) -> exp.Table:
        schema = self.add_test_suffix(schema)
        return exp.to_table(
            normalize_model_name(
                ".".join([schema, table_name]),
                default_catalog=self.engine_adapter.default_catalog,
                dialect=self.dialect,
            )
        )

    def physical_properties(
        self, properties_for_dialect: t.Dict[str, t.Dict[str, str | exp.Expression]]
    ) -> t.Dict[str, exp.Expression]:
        if props := properties_for_dialect.get(self.dialect):
            return {k: exp.Literal.string(v) if isinstance(v, str) else v for k, v in props.items()}
        return {}

    def schema(self, schema_name: str = TEST_SCHEMA, catalog_name: t.Optional[str] = None) -> str:
        return exp.table_name(
            normalize_model_name(
                self.add_test_suffix(
                    ".".join(
                        p
                        for p in (catalog_name or self.engine_adapter.default_catalog, schema_name)
                        if p
                    )
                    if "." not in schema_name
                    else schema_name
                ),
                default_catalog=None,
                dialect=self.dialect,
            )
        )

    def get_current_data(self, table: exp.Table) -> pd.DataFrame:
        df = self.engine_adapter.fetchdf(exp.select("*").from_(table), quote_identifiers=True)
        if self.dialect == "snowflake" and "id" in df.columns:
            df["id"] = df["id"].apply(lambda x: x if pd.isna(x) else int(x))
        return self._format_df(df)

    def compare_with_current(self, table: exp.Table, expected: pd.DataFrame) -> None:
        compare_dataframes(
            self.get_current_data(table),
            self.output_data(expected),
            check_dtype=False,
            check_index_type=False,
        )

    def get_table_comment(
        self,
        schema_name: str,
        table_name: str,
        table_kind: str = "BASE TABLE",
        snowflake_capitalize_ids: bool = True,
    ) -> t.Optional[str]:
        if self.dialect in ["postgres", "redshift"]:
            query = f"""
                SELECT
                    pgc.relname,
                    pg_catalog.obj_description(pgc.oid, 'pg_class')
                FROM pg_catalog.pg_class pgc
                INNER JOIN pg_catalog.pg_namespace n
                ON pgc.relnamespace = n.oid
                WHERE
                    n.nspname = '{schema_name}'
                    AND pgc.relname = '{table_name}'
                    AND pgc.relkind = '{'v' if table_kind == "VIEW" else 'r'}'
                ;
            """
        elif self.dialect in ["mysql", "snowflake"]:
            # Snowflake treats all identifiers as uppercase unless they are lowercase and quoted.
            # They are lowercase and quoted in sushi but not in the inline tests.
            if self.dialect == "snowflake" and snowflake_capitalize_ids:
                schema_name = schema_name.upper()
                table_name = table_name.upper()

            comment_field_name = {
                "mysql": "table_comment",
                "snowflake": "comment",
            }

            query = f"""
                SELECT
                    table_name,
                    {comment_field_name[self.dialect]}
                FROM INFORMATION_SCHEMA.TABLES
                WHERE
                    table_schema='{schema_name}'
                    AND table_name='{table_name}'
                    AND table_type='{table_kind}'
            """
        elif self.dialect == "bigquery":
            query = f"""
                SELECT
                    table_name,
                    option_value
                FROM `region-us.INFORMATION_SCHEMA.TABLE_OPTIONS`
                WHERE
                    table_schema='{schema_name}'
                    AND table_name='{table_name}'
                    AND option_name = 'description'
            """
        elif self.dialect in ["spark", "databricks"]:
            query = f"DESCRIBE TABLE EXTENDED {schema_name}.{table_name}"
        elif self.dialect == "trino":
            query = f"""
                SELECT
                    table_name,
                    comment
                FROM system.metadata.table_comments
                WHERE
                    schema_name = '{schema_name}'
                    AND table_name = '{table_name}'
            """
        elif self.dialect == "duckdb":
            kind = "table" if table_kind == "BASE TABLE" else "view"
            query = f"""
                SELECT
                    {kind}_name,
                    comment
                FROM duckdb_{kind}s()
                WHERE
                    schema_name = '{schema_name}'
                    AND {kind}_name = '{table_name}'
            """
        elif self.dialect == "clickhouse":
            query = f"SELECT name, comment FROM system.tables WHERE database = '{schema_name}' AND name = '{table_name}'"

        result = self.engine_adapter.fetchall(query)

        if result:
            if self.dialect == "bigquery":
                comment = result[0][1].replace('"', "").replace("\\n", "\n")
            elif self.dialect in ["spark", "databricks"]:
                comment = [x for x in result if x[0] == "Comment"]
                comment = comment[0][1] if comment else None
            else:
                comment = result[0][1]

            return comment

        return None

    def get_column_comments(
        self,
        schema_name: str,
        table_name: str,
        table_kind: str = "BASE TABLE",
        snowflake_capitalize_ids: bool = True,
    ) -> t.Dict[str, str]:
        comment_index = 1
        if self.dialect in ["postgres", "redshift"]:
            query = f"""
                SELECT
                    cols.column_name,
                    pg_catalog.col_description(pgc.oid, cols.ordinal_position::int) AS column_comment
                FROM pg_catalog.pg_class pgc
                INNER JOIN pg_catalog.pg_namespace n
                ON
                    pgc.relnamespace = n.oid
                INNER JOIN information_schema.columns cols
                ON
                    pgc.relname = cols.table_name
                    AND n.nspname = cols.table_schema
                WHERE
                    n.nspname = '{schema_name}'
                    AND pgc.relname = '{table_name}'
                    AND pgc.relkind = '{'v' if table_kind == "VIEW" else 'r'}'
                ;
            """
        elif self.dialect in ["mysql", "snowflake"]:
            # Snowflake treats all identifiers as uppercase unless they are lowercase and quoted.
            # They are lowercase and quoted in sushi but not in the inline tests.
            if self.dialect == "snowflake" and snowflake_capitalize_ids:
                schema_name = schema_name.upper()
                table_name = table_name.upper()

            comment_field_name = {
                "mysql": "column_comment",
                "snowflake": "comment",
            }

            query = f"""
                SELECT
                    column_name,
                    {comment_field_name[self.dialect]}
                FROM
                    information_schema.columns
                WHERE
                    table_schema = '{schema_name}'
                    AND table_name = '{table_name}'
                ;
            """
        elif self.dialect == "bigquery":
            query = f"""
                SELECT
                    column_name,
                    description
                FROM
                    `region-us.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
                WHERE
                    table_schema = '{schema_name}'
                    AND table_name = '{table_name}'
                ;
            """
        elif self.dialect in ["spark", "databricks", "clickhouse"]:
            query = f"DESCRIBE TABLE {schema_name}.{table_name}"
            comment_index = 2 if self.dialect in ["spark", "databricks"] else 4
        elif self.dialect == "trino":
            query = f"SHOW COLUMNS FROM {schema_name}.{table_name}"
            comment_index = 3
        elif self.dialect == "duckdb":
            query = f"""
                SELECT
                    column_name,
                    comment
                FROM duckdb_columns()
                WHERE
                    schema_name = '{schema_name}'
                    AND table_name = '{table_name}'
            """

        result = self.engine_adapter.fetchall(query)

        comments = {}
        if result:
            if self.dialect in ["spark", "databricks"]:
                result = list(set([x for x in result if not x[0].startswith("#")]))

            comments = {
                x[0]: x[comment_index]
                for x in result
                if x[comment_index] is not None and x[comment_index].strip() != ""
            }

        return comments

    def create_context(
        self, config_mutator: t.Optional[t.Callable[[str, Config], None]] = None
    ) -> Context:
        private_sqlmesh_dir = pathlib.Path(pathlib.Path().home(), ".sqlmesh")
        config = load_config_from_paths(
            Config,
            project_paths=[
                pathlib.Path(os.path.join(os.path.dirname(__file__), "config.yaml")),
                private_sqlmesh_dir / "config.yml",
                private_sqlmesh_dir / "config.yaml",
            ],
        )
        if config_mutator:
            config_mutator(self.gateway, config)

        if "athena" in self.gateway:
            conn = config.gateways[self.gateway].connection
            assert isinstance(conn, AthenaConnectionConfig)
            assert isinstance(self.engine_adapter, AthenaEngineAdapter)
            # Ensure that s3_warehouse_location is propagated
            conn.s3_warehouse_location = self.engine_adapter.s3_warehouse_location

        self._context = Context(paths=".", config=config, gateway=self.gateway)
        return self._context

    def create_catalog(self, catalog_name: str):
        if self.dialect == "databricks":
            self.engine_adapter.execute(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        elif self.dialect == "tsql":
            self.engine_adapter.cursor.connection.autocommit(True)
            try:
                self.engine_adapter.cursor.execute(f"CREATE DATABASE {catalog_name}")
            except Exception:
                pass
            self.engine_adapter.cursor.connection.autocommit(False)
        elif self.dialect == "snowflake":
            self.engine_adapter.execute(f'CREATE DATABASE IF NOT EXISTS "{catalog_name}"')
        elif self.dialect == "duckdb":
            try:
                # Only applies to MotherDuck
                self.engine_adapter.execute(f'CREATE DATABASE IF NOT EXISTS "{catalog_name}"')
            except Exception:
                pass

    def drop_catalog(self, catalog_name: str):
        if self.dialect == "bigquery":
            return  # bigquery cannot create/drop catalogs
        elif self.dialect == "databricks":
            self.engine_adapter.execute(f"DROP CATALOG IF EXISTS {catalog_name} CASCADE")
        else:
            self.engine_adapter.execute(f'DROP DATABASE IF EXISTS "{catalog_name}"')

    def cleanup(self, ctx: t.Optional[Context] = None):
        schemas = [self.schema(TEST_SCHEMA)]

        ctx = ctx or self._context
        if ctx and ctx.models:
            for _, model in ctx.models.items():
                schemas.append(model.schema_name)
                schemas.append(model.physical_schema)

        for schema_name in set(schemas):
            self.engine_adapter.drop_schema(
                schema_name=schema_name, ignore_if_not_exists=True, cascade=True
            )

    def upsert_sql_model(self, model_definition: str) -> t.Tuple[Context, SqlModel]:
        if not self._context:
            self._context = self.create_context()

        model = load_sql_based_model(expressions=d.parse(model_definition))
        assert isinstance(model, SqlModel)
        self._context.upsert_model(model)
        return self._context, model
