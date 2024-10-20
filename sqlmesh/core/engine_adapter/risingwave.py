from __future__ import annotations

import logging
import re
import typing as t
from typing import List


from sqlglot import exp

from sqlmesh.core.engine_adapter.base_postgres import BasePostgresEngineAdapter
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    PandasNativeFetchDFSupportMixin,
)
from sqlmesh.core.engine_adapter.shared import set_catalog, CatalogSupport, CommentCreationView, CommentCreationTable
from sqlmesh.core.model.risingwavesink import RwSinkSettings
from sqlmesh.core.schema_diff import SchemaDiffer


if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import DF

logger = logging.getLogger(__name__)

def _increment_sink_version(sink_name: str) -> str:
    """
    Increments the version number in the sink name.
    Args:
        sink_name (str): The original sink name with a version suffix.
    Returns:
        str: The updated sink name with incremented version.
    """
    # Regular expression to match the version suffix (v followed by digits)
    match = re.search(r'(_v\d+)$', sink_name)

    if match:
        # Extract the version part
        version_part = match.group(0)  # e.g., _v1

        # Extract the version number
        version_number = int(version_part[2:])  # Convert "1" to 1

        # Increment the version number
        new_version_number = version_number + 1

        # Format the new version part (keep the same format without leading zero)
        new_version_part = f"_v{new_version_number}"  # e.g., _v2

        # Replace the old version part with the new version part
        new_sink_name = sink_name[:match.start()] + new_version_part
        return new_sink_name

    return sink_name  # Return original if no version suffix found


def _extract_sink_name(view_name: TableName) -> t.Optional[str]:
    """
    Extracts the sink name from the provided view name.
    This function handles both string and exp.Table types.
    For example, if the view name is 'sqlmesh__sqlmesh.sqlmesh__mv_sales_test01__3707292608__temp',
    it will return 'sqlmesh__sqlmesh.mv_sales_test01'.
    Args:
        view_name (TableName): The view name which can be a string or exp.Table object.
    Returns:
        str: The extracted sink name, or None if the structure is invalid.
    """

    # Convert the exp.Table object to a string representation (if needed)
    table_name = str(view_name)
    parts = table_name.split('__')

    if table_name.endswith("__temp"):
        return None

    if len(parts) > 2:
        return parts[2]
    return None



def _find_latest_sink(sink_names: List[str]) -> str:
    # Regular expression to extract the version number at the end of the sink name (e.g., _v1, _v2, etc.)
    version_pattern = re.compile(r"_v(\d+)$")

    latest_sink = sink_names[0]  # Initialize with the first element as a fallback
    max_version = -1  # Initialize to a value lower than any version

    for name in sink_names:
        # Search for version using the regular expression
        match = version_pattern.search(name)
        if match:
            version = int(match.group(1))  # Extract the version number as an integer

            # Update latest_sink if this version is higher than the current max_version
            if version > max_version:
                max_version = version
                latest_sink = name

    return latest_sink


@set_catalog()
class RisingwaveEngineAdapter(
    BasePostgresEngineAdapter,
    PandasNativeFetchDFSupportMixin,
    GetCurrentCatalogFromFunctionMixin,
):
    DIALECT = "risingwave"
    SUPPORTS_INDEXES = True
    HAS_VIEW_BINDING = True
    CURRENT_CATALOG_EXPRESSION = exp.column("current_catalog")
    SUPPORTS_REPLACE_TABLE = False
    DEFAULT_BATCH_SIZE = 400
    CATALOG_SUPPORT = CatalogSupport.SINGLE_CATALOG_ONLY
    COMMENT_CREATION_TABLE = CommentCreationTable.COMMENT_COMMAND_ONLY
    COMMENT_CREATION_VIEW = CommentCreationView.COMMENT_COMMAND_ONLY
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_RW_SINK = True

    SCHEMA_DIFFER = SchemaDiffer(
        parameterized_type_defaults={
            # DECIMAL without precision is "up to 131072 digits before the decimal point; up to 16383 digits after the decimal point"
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(131072 + 16383, 16383), (0,)],
            exp.DataType.build("CHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("TIME", dialect=DIALECT).this: [(6,)],
            exp.DataType.build("TIMESTAMP", dialect=DIALECT).this: [(6,)],
        },
        types_with_unlimited_length={
            # all can ALTER to `TEXT`
            exp.DataType.build("TEXT", dialect=DIALECT).this: {
                exp.DataType.build("VARCHAR", dialect=DIALECT).this,
                exp.DataType.build("CHAR", dialect=DIALECT).this,
                exp.DataType.build("BPCHAR", dialect=DIALECT).this,
            },
            # all can ALTER to unparameterized `VARCHAR`
            exp.DataType.build("VARCHAR", dialect=DIALECT).this: {
                exp.DataType.build("VARCHAR", dialect=DIALECT).this,
                exp.DataType.build("CHAR", dialect=DIALECT).this,
                exp.DataType.build("BPCHAR", dialect=DIALECT).this,
                exp.DataType.build("TEXT", dialect=DIALECT).this,
            },
            # parameterized `BPCHAR(n)` can ALTER to unparameterized `BPCHAR`
            exp.DataType.build("BPCHAR", dialect=DIALECT).this: {
                exp.DataType.build("BPCHAR", dialect=DIALECT).this
            },
        },
    )

    def _fetch_native_df(
        self, query: t.Union[exp.Expression, str], quote_identifiers: bool = False
    ) -> DF:
        """
        `read_sql_query` when using psycopg will result on a hanging transaction that must be committed
        https://github.com/pandas-dev/pandas/pull/42277
        """
        df = super()._fetch_native_df(query, quote_identifiers)
        if not self._connection_pool.is_transaction_active:
            self._connection_pool.commit()
        return df


    def create_view(
        self,
        view_name: TableName,
        query_or_df: DF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        sink: bool = False,
        connections_str: t.Optional[RwSinkSettings] = None,
        **create_kwargs: t.Any,
    ) -> None:
        """
        Postgres has very strict rules around view replacement. For example the new query must generate an identical setFormatter
        of columns, using the same column names and data types as the old one. We have to delete the old view instead of replacing it
        to work around these constraints.
        Reference: https://www.postgresql.org/docs/current/sql-createview.html
        """
        if materialized:
            replace = True

        with self.transaction():
            """check this why replace is always false incase of materialized is enabled to true"""
            if replace:
                self.drop_view(view_name, materialized=materialized)
            super().create_view(
                view_name,
                query_or_df,
                columns_to_types=columns_to_types,
                replace=replace,
                materialized=materialized,
                table_description=table_description,
                column_descriptions=column_descriptions,
                view_properties=view_properties,
                sink=sink,
                connections_str=connections_str,
                **create_kwargs,
            )
            if sink:
                self.create_rw_sink(view_name, connections_str)

    def drop_view(
        self,
        view_name: TableName,
        ignore_if_not_exists: bool = True,
        materialized: bool = False,
        **kwargs: t.Any,
    ) -> None:
        kwargs["cascade"] = kwargs.get("cascade", True)

        return super().drop_view(
            view_name,
            ignore_if_not_exists=ignore_if_not_exists,
            # may have errors since TableName could be str and has no attribute db
            # materialized=self._is_materialized(view_name),
            materialized=materialized,
            **kwargs,
        )

    def _is_materialized(self, view_name: TableName) -> bool:
        _is_materialized = False
        """
           Builds a SQL query to check for a table in information_schema.tables
           based on dbname.schema_name.table_name.
           """

        # Build the base query with WHERE conditions
        query = (
            exp.select("table_type")
            .from_("information_schema.tables")
            .where(exp.column("table_schema").eq(view_name.db))
            .where(exp.column("table_name").eq(view_name.name))
        )

        # Fetch the result as a DataFrame
        df = self.fetchdf(query)

        if not df.empty:
            # Access the first row to get table_type
            first_row = df.iloc[0]
            table_type = first_row['table_type']

            # Check if table_type is MATERIALIZED VIEW
            if table_type == 'MATERIALIZED VIEW':
                _is_materialized = True
                logger.debug("The object is a MATERIALIZED VIEW.")
            else:
                logger.debug(f"The object is of type: {table_type}")
        else:
            logger.debug("The DataFrame is empty.")

        # Return whether the object is materialized
        return _is_materialized

    def create_rw_sink(self, view_name: TableName,connections_str :RwSinkSettings) -> None:
        """
           Builds a SQL query to check for a table in information_schema.tables
           based on dbname.schema_name.table_name.
           """
        _is_sink_need_drop = False
        sink_name = _extract_sink_name(view_name)

        # Check if sink_name is None and log information
        if sink_name is None:
            logging.info(f"Sink name could not be extracted from the view name: {view_name}")
            return  # Exit the function without processing further

        sink_names = self._is_sink_exists(view_name, sink_name)

        topic = connections_str.properties.topic
        if not sink_names:  # Check if sink_names is None or an empty list
            sink_name = sink_name + '_v1'  # Set to v1 if no sinks exist
        else:
            if len(sink_names) > 1:
                logger.warning("Found more than one version of sinks in the model ! Please clear the other versions of not required !")
                logger.warning("This will use the compute power and might slow down the entire process !")
            # If sink_names is not empty, find the latest sink and increment its version
            sink_name_latest = _find_latest_sink(sink_names)
            sink_name = _increment_sink_version(sink_name_latest)

            if topic is not None:
                sink_name = sink_name_latest
                _is_sink_need_drop = True

        properties = connections_str.properties
        # Start finding topics
        if topic is None:
            properties = properties.copy(update={"topic": sink_name})
            connections_str = connections_str.copy(update={"properties": properties})

        self._create_rw_sink(sink_name,view_name,connections_str,_is_sink_need_drop)


    def _create_rw_sink(self, sink_name: str, view_name: TableName,connections_str :RwSinkSettings,_is_sink_need_drop:bool) -> None:

        '''sink_name = sink_name.split('__', 1)[1]'''
        if _is_sink_need_drop :
            logger.warning(f"no topic names are present in connection strings, drop current sink and create new one.")
            query = f"DROP SINK IF EXISTS {sink_name}"
            self._execute(query)
        # Start building the query
        query = f"CREATE SINK IF NOT EXISTS {sink_name} FROM {view_name} \nWITH (\n"

        # Iterate over the settings fields dynamically
        for field_name, value in connections_str.properties.model_dump().items():
            if value:
                setting_name = field_name.replace('_', '.')
                query += f"\t{setting_name}='{value}',\n"

        # Remove the last comma and newline if any "WITH" properties were added
        query = query.rstrip(',\n') + "\n)"
        # Add format and encode block if available
        if connections_str.format :
            query += " FORMAT"
            if connections_str.format.format:
                query += f" {connections_str.format.format}"
            if connections_str.format.encode:
                query += f" ENCODE {connections_str.format.encode} ( \n"

        # Add force_append_only under the FORMAT block if available
        excluded_attributes = {"format", "encode"}
        for field_name, value in connections_str.format.model_dump().items():
            if field_name not in excluded_attributes:
              setting_name = field_name # Remove the 'f_' prefix
              query += f" \t{setting_name}='{value}',\n"
        query = query.rstrip(',\n') + "\n)"
        self._execute(query)

    def _is_sink_exists(self, view_name: TableName, sink_name: str) -> t.Optional[List[str]]:

        """
           Builds a SQL query to check for a table in information_schema.tables
           based on dbname.schema_name.table_name.
           """
        # Build the base query with WHERE conditions
        query = (
            exp.select("table_name")
            .from_("information_schema.tables")
            .where(exp.column("table_type").eq("SINK"))
            .where(f"table_name LIKE '{sink_name}_v%'")

        )
        # Fetch the result as a DataFrame
        df = self.fetchdf(query)

        # Check if DataFrame is empty
        if df.empty:
            logger.info("The sink does not exist create new one !.")
            return None  # Return None if no rows
        else:
            # Return a list of table names (assuming the column is named 'table_name')
            table_list = df["table_name"].tolist()
            if len(table_list) > 1:
                logger.warning(f"multiple sink {table_list} found for {sink_name} !. Please drop it if not necessary !")
            return df["table_name"].tolist()