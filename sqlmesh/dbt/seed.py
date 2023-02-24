from __future__ import annotations

import typing as t
from pathlib import Path

from pydantic import Field, validator
from sqlglot.helper import ensure_list

from sqlmesh.core.config.base import UpdateStrategy
from sqlmesh.core.model import Model, SeedKind, create_seed_model
from sqlmesh.dbt.builtin import builtin_jinja
from sqlmesh.dbt.column import (
    ColumnConfig,
    column_descriptions_to_sqlmesh,
    column_types_to_sqlmesh,
    yaml_to_columns,
)
from sqlmesh.dbt.common import GeneralConfig
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
        if not isinstance(v, dict) or all(isinstance(col, ColumnConfig) for col in v.values()):
            return v

        return yaml_to_columns(v)

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        **GeneralConfig._FIELD_UPDATE_STRATEGY,
        **{
            "grants": UpdateStrategy.KEY_EXTEND,
            "path": UpdateStrategy.IMMUTABLE,
            "pre-hook": UpdateStrategy.EXTEND,
            "post-hook": UpdateStrategy.EXTEND,
            "columns": UpdateStrategy.KEY_EXTEND,
        },
    }

    def to_sqlmesh(self, variables: t.Dict[str, t.Any]) -> Model:
        """Converts the dbt seed into a SQLMesh model."""
        rendered = self.render_non_sql_jinja(builtin_jinja(variables))

        return create_seed_model(
            rendered.seed_name,
            SeedKind(path=rendered.path.absolute()),
            path=rendered.path,
            columns=column_types_to_sqlmesh(rendered.columns) or None,
            column_descriptions_=column_descriptions_to_sqlmesh(rendered.columns) or None,
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
