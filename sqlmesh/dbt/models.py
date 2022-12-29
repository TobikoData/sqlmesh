from __future__ import annotations

import typing as t
from enum import Enum
from pathlib import Path

from pydantic import Field, validator
from sqlglot.helper import ensure_list

from sqlmesh.core import dialect as d
from sqlmesh.core.model import Model, ModelKindName
from sqlmesh.dbt.column import ColumnConfig, yaml_to_columns
from sqlmesh.dbt.common import GeneralConfig, UpdateStrategy
from sqlmesh.utils.conversions import ensure_bool
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.metaprogramming import Executable, ExecutableKind


class Materialization(str, Enum):
    """DBT model materializations"""

    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    EPHERMAL = "ephemeral"


class ModelConfig(GeneralConfig):
    """
    ModelConfig contains all config parameters available to DBT models

    See https://docs.getdbt.com/reference/configs-and-properties for
    a more detailed description of each config parameter under the
    General propreties, General configs, and For models sections.

    Args:
        path: The file path of the model
        sql: The model sql
        time_column: The name of the time column
        table_name: Table name as stored in the database instead of the model filename
        alias: Relation identifier for this model instead of the model filename
        cluster_by: Field(s) to use for clustering in data warehouses that support clustering
        database: Database the model is stored in
        full_refresh: Forces the model to always do a full refresh or never do a full refresh
        grants: Set or revoke permissions to the database object for this model
        incremental_strategy: Strategy used to build the incremental model
        materialized: How the model will be materialized in the database
        post-hook: List of SQL statements to run after the model is built
        pre-hook: List of SQL statements to run before the model is built
        schema: Custom schema name added to the model schema name
        sql_header: SQL statement to inject above create table/view as
        unique_key: List of columns that define row uniqueness for the model
        columns: Columns within the model
    """

    # sqlmesh fields
    path: Path = Path()
    sql: str = ""
    time_column: t.Optional[str] = None
    table_name: t.Optional[str] = None
    _depends_on: t.Set[str] = set()
    _calls: t.Set[str] = set()

    # DBT configuration fields
    alias: t.Optional[str] = None
    cluster_by: t.Optional[t.List[str]] = None
    database: t.Optional[str] = None
    full_refresh: t.Optional[bool] = None
    grants: t.Dict[str, t.List[str]] = {}
    incremental_strategy: t.Optional[str] = None
    materialized: Materialization = Materialization.VIEW
    post_hook: t.List[str] = Field([], alias="post-hook")
    pre_hook: t.List[str] = Field([], alias="pre-hook")
    schema_: t.Optional[str] = Field(None, alias="schema")
    sql_header: t.Optional[str] = None
    unique_key: t.Optional[t.List[str]] = None
    columns: t.Dict[str, ColumnConfig] = {}

    @validator(
        "pre_hook",
        "post_hook",
        "unique_key",
        "cluster_by",
        pre=True,
    )
    def _validate_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return ensure_list(v)

    @validator("full_refresh", pre=True)
    def _validate_bool(cls, v: str) -> bool:
        return ensure_bool(v)

    @validator("materialized", pre=True)
    def _validate_materialization(cls, v: str) -> Materialization:
        return Materialization(v.lower())

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
            "post-hook": UpdateStrategy.APPEND,
            "pre-hook": UpdateStrategy.APPEND,
            "sql": UpdateStrategy.IMMUTABLE,
            "time_column": UpdateStrategy.IMMUTABLE,
            "columns": UpdateStrategy.KEY_APPEND,
        },
    }

    def to_sqlmesh(self, mapping: t.Dict[str, ModelConfig]) -> Model:
        """Converts the dbt model into a SQLMesh model."""
        expressions = d.parse_model(
            f"""
            MODEL (
              name {self.model_name},
              kind {self.model_kind},
            );
            """
            + self.sql,
            default_dialect="",
        )

        for jinja in expressions[1:]:
            # find all the refs here and filter the python env?
            if isinstance(jinja, d.Jinja):
                pass

        def ref_code() -> str:
            deps = ", ".join(
                f"'{dep}': '{mapping[dep].model_name}'" for dep in self._depends_on
            )
            return f"{{{deps}}}[model_name]"

        python_env = {
            "source": Executable(
                payload="""def source(source_name, table_name):
    return ".".join((source_name, table_name))
""",
            ),
            "ref": Executable(
                payload=f"""def ref(package_name, model_name=None):
    if model_name:
        raise Exception("Package not supported.")
    model_name = package_name
    return {ref_code()}
""",
            ),
            "sqlmesh": Executable(
                kind=ExecutableKind.VALUE,
                payload=True,
            ),
            "is_incremental": Executable(
                payload="def is_incremental(): return False",
            ),
        }

        python_env = {k: v for k, v in python_env.items() if k in self._calls}

        return Model.load(
            expressions,
            path=self.path,
            python_env=python_env,
            depends_on=self._depends_on,
        )

    @property
    def model_name(self) -> str:
        """
        Get the sqlmesh model name

        Returns:
            The sqlmesh model name
        """
        return ".".join(
            part for part in (self.schema_, self.alias or self.table_name) if part
        )

    @property
    def model_kind(self) -> str:
        """
        Get the sqlmesh ModelKind

        Returns:
            The sqlmesh ModelKind
        """
        materialization = self.materialized
        if materialization == Materialization.TABLE:
            return ModelKindName.FULL.value
        if materialization == Materialization.VIEW:
            return ModelKindName.VIEW.value
        if materialization == Materialization.INCREMENTAL:
            if self.time_column:
                return f"{ModelKindName.INCREMENTAL_BY_TIME_RANGE.value} (TIME_COLUMN {self.time_column})"
            if self.unique_key:
                return f"{ModelKindName.INCREMENTAL_BY_UNIQUE_KEY.value} (UNIQUE_KEY ({','.join(self.unique_key)}))"
            raise ConfigError(f"Incremental needs either unique key or time column.")
        if materialization == Materialization.EPHERMAL:
            return ModelKindName.EMBEDDED.value
        raise ConfigError(f"{materialization.value} materialization not supported")
