from __future__ import annotations

import typing as t

from pydantic import validator

from sqlmesh.dbt.column import ColumnConfig
from sqlmesh.dbt.common import parse_meta
from sqlmesh.dbt.update import UpdateStrategy
from sqlmesh.utils.datatype import ensure_bool, ensure_list
from sqlmesh.utils.pydantic import PydanticModel


class SourceConfig(PydanticModel):
    """
    Args:
        name: The name of the source or table
        description: User-defined description
        meta: Meta data associated with the source
        database: Name of the database where the table is stored. By default, the project's target database is used.
        schema: The scehma name as stored in the database. If not specified, the source name is used.
        identifier: The table name as stored in the database. If not specified, the source table name is used
        loader: Describes the tool that loads the source into the warehouse
        tests: Tests asociated with the source
        tags: Tags associated with the source
        overrides: Override a source defined in the specified package
        freshness: Dictionary specifying maximum time, since the most recent record, to consider the source fresh
        loaded_at_field: Column name or expression that returns a timestamp indicating freshness
        quoting: Dictionary of what to quote (database, schema, identifier) when resolving the source() method
        external: Dictionary of metadata properties specific to sources that point to external tables
        columns: Columns within the source
    """

    name: str
    description: t.Optional[str] = None
    meta: t.Optional[t.Dict[str, t.Any]] = {}
    database: t.Optional[str] = None
    schema: t.Optional[str] = None
    identifier: t.Optional[str] = None
    loader: t.Optional[str] = None
    tests: t.Optional[t.List[str]] = []
    tags: t.Optional[t.List[str]] = []
    overrides: t.Optional[str] = None
    freshness: t.Optional[t.Dict[str, t.Any]] = {}
    loaded_at_field: t.Optional[str] = None
    quoting: t.Optional[t.Dict[str, bool]] = {}
    external: t.Optional[t.Dict[str, t.Any]] = {}
    columns: t.Optional[t.List[ColumnConfig]] = []

    @validator(
        "tests",
        "tags",
        pre=True,
    )
    def _validate_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return ensure_list(v)

    @validator("meta", pre=True)
    def _validate_meta(cls, v: t.Dict[str, t.Union[str, t.Any]]) -> t.Dict[str, t.Any]:
        return parse_meta(v)

    @validator("quoting", pre=True)
    def _validate_quoting(cls, v: t.Dict[str, t.Any]) -> t.Dict[str, bool]:
        return {key: ensure_bool(val) for key, val in v.items()}

    @validator("columns", pre=True)
    def _validate_columns(cls, v: t.Any) -> t.List[ColumnConfig]:
        return [ColumnConfig()]

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        "meta": UpdateStrategy.KEY_UPDATE,
        "tags": UpdateStrategy.APPEND,
    }
