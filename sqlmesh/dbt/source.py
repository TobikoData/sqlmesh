from __future__ import annotations

import typing as t

from dbt.contracts.relation import RelationType
from pydantic import Field, validator

from sqlmesh.core.config.base import UpdateStrategy
from sqlmesh.dbt.column import ColumnConfig, yaml_to_columns
from sqlmesh.dbt.common import GeneralConfig
from sqlmesh.utils.conversions import ensure_bool


class SourceConfig(GeneralConfig):
    """
    Args:
        config_name: The schema.table_name names declared in source config
        name: The name of the source or table
        database: Name of the database where the table is stored. By default, the project's target database is used.
        schema: The scehma name as stored in the database. If not specified, the source name is used.
        identifier: The table name as stored in the database. If not specified, the source table name is used
        loader: Describes the tool that loads the source into the warehouse
        overrides: Override a source defined in the specified package
        freshness: Dictionary specifying maximum time, since the most recent record, to consider the source fresh
        loaded_at_field: Column name or expression that returns a timestamp indicating freshness
        quoting: Dictionary of what to quote (database, schema, identifier) when resolving the source() method
        external: Dictionary of metadata properties specific to sources that point to external tables
        columns: Columns within the source
    """

    # sqlmesh fields
    config_name: str = ""

    # DBT configuration fields
    name: t.Optional[str] = None
    database: t.Optional[str] = None
    schema_: t.Optional[str] = Field(None, alias="schema")
    identifier: t.Optional[str] = None
    loader: t.Optional[str] = None
    overrides: t.Optional[str] = None
    freshness: t.Optional[t.Dict[str, t.Any]] = {}
    loaded_at_field: t.Optional[str] = None
    quoting: t.Optional[t.Dict[str, bool]] = {}
    external: t.Optional[t.Dict[str, t.Any]] = {}
    columns: t.Dict[str, ColumnConfig] = {}

    @validator("quoting", pre=True)
    def _validate_quoting(cls, v: t.Dict[str, t.Any]) -> t.Dict[str, bool]:
        return {key: ensure_bool(val) for key, val in v.items()}

    @validator("columns", pre=True)
    def _validate_columns(cls, v: t.Any) -> t.Dict[str, ColumnConfig]:
        return yaml_to_columns(v)

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        **GeneralConfig._FIELD_UPDATE_STRATEGY,
        **{"columns": UpdateStrategy.KEY_EXTEND},
    }

    @property
    def table_name(self) -> t.Optional[str]:
        return self.identifier or self.name

    @property
    def source_name(self) -> str:
        return ".".join(part for part in (self.schema_, self.table_name) if part)

    @property
    def relation_info(self) -> t.Dict[str, t.Any]:
        return {
            "database": self.database,
            "schema": self.schema_,
            "identifier": self.table_name,
            "type": RelationType.External.value,
        }
