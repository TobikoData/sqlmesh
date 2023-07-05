from __future__ import annotations

import typing as t

from dbt.contracts.relation import RelationType
from pydantic import Field

from sqlmesh.core.config.base import UpdateStrategy
from sqlmesh.dbt.column import ColumnConfig
from sqlmesh.dbt.common import GeneralConfig, QuotingConfig
from sqlmesh.utils import AttributeDict


class SourceConfig(GeneralConfig):
    """
    Args:
        name: The name of the table
        source_name: The name of the source that defines the table
        database: Name of the database where the table is stored. By default, the project's target database is used.
        schema: The scehma name as stored in the database. If not specified, the source name is used.
        identifier: The table name as stored in the database. If not specified, the source table name is used
        loader: Describes the tool that loads the source into the warehouse
        overrides: Override a source defined in the specified package
        freshness: Dictionary specifying maximum time, since the most recent record, to consider the source fresh
        loaded_at_field: Column name or expression that returns a timestamp indicating freshness
        quoting: Define which components of the qualified name (database, schema, identifier) to quote when resolving the source() method
        external: Dictionary of metadata properties specific to sources that point to external tables
        columns: Columns within the source
    """

    # DBT configuration fields
    name: str = ""
    source_name_: str = Field("", alias="source_name")
    database: t.Optional[str] = None
    schema_: t.Optional[str] = Field(None, alias="schema")
    identifier: t.Optional[str] = None
    loader: t.Optional[str] = None
    overrides: t.Optional[str] = None
    freshness: t.Optional[t.Dict[str, t.Any]] = {}
    loaded_at_field: t.Optional[str] = None
    quoting: QuotingConfig = Field(default_factory=QuotingConfig)
    external: t.Optional[t.Dict[str, t.Any]] = {}
    columns: t.Dict[str, ColumnConfig] = {}

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        **GeneralConfig._FIELD_UPDATE_STRATEGY,
        **{"columns": UpdateStrategy.KEY_EXTEND},
    }

    @property
    def table_name(self) -> t.Optional[str]:
        return self.identifier or self.name

    @property
    def config_name(self) -> str:
        return f"{self.source_name_}.{self.name}"

    @property
    def sql_name(self) -> str:
        return ".".join(part for part in (self.database, self.schema_, self.table_name) if part)

    @property
    def relation_info(self) -> AttributeDict:
        return AttributeDict(
            {
                "database": self.database,
                "schema": self.schema_,
                "identifier": self.table_name,
                "type": RelationType.External.value,
                "quote_policy": AttributeDict(self.quoting.dict()),
            }
        )
