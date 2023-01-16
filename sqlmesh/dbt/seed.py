from __future__ import annotations

import typing as t
from pathlib import Path

from pydantic import Field, validator
from sqlglot.helper import ensure_list

from sqlmesh.core.model import Model, SeedKind, create_seed_model
from sqlmesh.dbt.column import ColumnConfig, yaml_to_columns
from sqlmesh.dbt.common import GeneralConfig, UpdateStrategy
from sqlmesh.utils.conversions import ensure_bool


class SeedConfig(GeneralConfig):
    """
    seedConfig contains all config parameters available to DBT seeds

    See https://docs.getdbt.com/reference/configs-and-properties for
    a more detailed description of each config parameter under the
    General propreties, General configs, and For seeds sections.

    Args:
        path: The path to the csv file
        target_schema: The schema for the profile target
        database: Database the seed is stored in
        schema: Custom schema name added to the seed schema name
        alias: Relation identifier for this seed instead of the seed filename
        pre-hook: List of SQL statements to run before the seed is built
        post-hook: List of SQL statements to run after the seed is built
        full_refresh: Forces the seed to always do a full refresh or never do a full refresh
        grants: Set or revoke permissions to the database object for this seed
        columns: Column information for the seed
    """

    # sqlmesh fields
    path: Path = Path()
    target_schema: str = ""

    # DBT configuration fields
    database: t.Optional[str] = None
    schema_: t.Optional[str] = Field(None, alias="schema")
    alias: t.Optional[str] = None
    pre_hook: t.List[str] = Field([], alias="pre-hook")
    post_hook: t.List[str] = Field([], alias="post-hook")
    full_refresh: t.Optional[bool] = None
    grants: t.Dict[str, t.List[str]] = {}
    columns: t.Dict[str, ColumnConfig] = {}

    @validator(
        "pre_hook",
        "post_hook",
        pre=True,
    )
    def _validate_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return ensure_list(v)

    @validator("full_refresh", pre=True)
    def _validate_bool(cls, v: str) -> bool:
        return ensure_bool(v)

    @validator("grants", pre=True)
    def _validate_grants(cls, v: t.Dict[str, str]) -> t.Dict[str, t.List[str]]:
        return {key: ensure_list(value) for key, value in v.items()}

    @validator("columns", pre=True)
    def _validate_columns(cls, v: t.Any) -> t.Dict[str, ColumnConfig]:
        return yaml_to_columns(v)

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        **GeneralConfig._FIELD_UPDATE_STRATEGY,
        **{
            "grants": UpdateStrategy.KEY_APPEND,
            "path": UpdateStrategy.IMMUTABLE,
            "pre-hook": UpdateStrategy.APPEND,
            "post-hook": UpdateStrategy.APPEND,
            "columns": UpdateStrategy.KEY_APPEND,
        },
    }

    def to_sqlmesh(self) -> Model:
        """Converts the dbt seed into a SQLMesh model."""
        return create_seed_model(
            self.seed_name, SeedKind(path=self.path.absolute()), path=self.path
        )

    @property
    def seed_name(self) -> str:
        """
        Get the sqlmesh seed name

        Returns:
            The sqlmesh seed name
        """
        schema = "_".join(part for part in (self.target_schema, self.schema_) if part)
        return ".".join(part for part in (schema, self.alias or self.path.stem) if part)
