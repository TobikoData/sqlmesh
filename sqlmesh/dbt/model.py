from __future__ import annotations

import re
import typing as t

from pydantic import validator
from sqlglot.helper import ensure_list

from sqlmesh.core import dialect as d
from sqlmesh.core.config.base import UpdateStrategy
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    Model,
    ModelKind,
    ModelKindName,
    create_sql_model,
)
from sqlmesh.dbt.basemodel import BaseModelConfig, Materialization
from sqlmesh.dbt.common import DbtContext, SqlStr
from sqlmesh.utils.errors import ConfigError


class ModelConfig(BaseModelConfig):
    """
    ModelConfig contains all config parameters available to DBT models

    See https://docs.getdbt.com/reference/configs-and-properties for
    a more detailed description of each config parameter under the
    General propreties, General configs, and For models sections.

    Args:
        sql: The model sql
        time_column: The name of the time column
        start: The earliest date that the model will be backfilled for
        cluster_by: Field(s) to use for clustering in data warehouses that support clustering
        incremental_strategy: Strategy used to build the incremental model
        materialized: How the model will be materialized in the database
        sql_header: SQL statement to inject above create table/view as
        unique_key: List of columns that define row uniqueness for the model
    """

    # sqlmesh fields
    sql: SqlStr = SqlStr("")
    time_column: t.Optional[str] = None

    # DBT configuration fields
    start: t.Optional[str] = None
    cluster_by: t.Optional[t.List[str]] = None
    incremental_strategy: t.Optional[str] = None
    materialized: Materialization = Materialization.VIEW
    sql_header: t.Optional[str] = None
    unique_key: t.Optional[t.List[str]] = None

    # redshift
    bind: t.Optional[bool] = None

    @validator(
        "unique_key",
        "cluster_by",
        pre=True,
    )
    def _validate_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return ensure_list(v)

    @validator("sql", pre=True)
    def _validate_sql(cls, v: t.Union[str, SqlStr]) -> SqlStr:
        return SqlStr(v)

    @validator("materialized", pre=True)
    def _validate_materialization(cls, v: str) -> Materialization:
        return Materialization(v.lower())

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        **BaseModelConfig._FIELD_UPDATE_STRATEGY,
        **{
            "sql": UpdateStrategy.IMMUTABLE,
            "time_column": UpdateStrategy.IMMUTABLE,
        },
    }

    @property
    def model_materialization(self) -> Materialization:
        return self.materialized

    @property
    def model_kind(self) -> ModelKind:
        """
        Get the sqlmesh ModelKind
        Returns:
            The sqlmesh ModelKind
        """
        materialization = self.materialized
        if materialization == Materialization.TABLE:
            return ModelKind(name=ModelKindName.FULL)
        if materialization == Materialization.VIEW:
            return ModelKind(name=ModelKindName.VIEW)
        if materialization == Materialization.INCREMENTAL:
            if self.time_column:
                return IncrementalByTimeRangeKind(time_column=self.time_column)
            if self.unique_key:
                return IncrementalByUniqueKeyKind(unique_key=self.unique_key)
            raise ConfigError(
                "SQLMesh ensures idempotent incremental loads and thus does not support append."
                " Add either an unique key (merge) or a time column (insert-overwrite)."
            )
        if materialization == Materialization.EPHEMERAL:
            return ModelKind(name=ModelKindName.EMBEDDED)
        raise ConfigError(f"{materialization.value} materialization not supported.")

    @property
    def sql_no_config(self) -> str:
        matches = re.findall(r"{{\s*config\(", self.sql)
        if matches:
            config_macro_start = self.sql.index(matches[0])
            cursor = config_macro_start
            quote = None
            while cursor < len(self.sql):
                if self.sql[cursor] in ('"', "'"):
                    if quote is None:
                        quote = self.sql[cursor]
                    elif quote == self.sql[cursor]:
                        quote = None
                if self.sql[cursor : cursor + 2] == "}}" and quote is None:
                    return "".join([self.sql[:config_macro_start], self.sql[cursor + 2 :]])
                cursor += 1
        return self.sql

    @property
    def all_sql(self) -> SqlStr:
        return SqlStr(";\n".join(self.pre_hook + [self.sql] + self.post_hook))

    def to_sqlmesh(self, context: DbtContext) -> Model:
        """Converts the dbt model into a SQLMesh model."""
        model_context = self._context_for_dependencies(context, self._dependencies)
        expressions = d.parse(self.sql_no_config)
        if not expressions:
            raise ConfigError(f"Model '{self.table_name}' must have a query.")

        return create_sql_model(
            self.model_name,
            expressions[-1],
            kind=self.model_kind,
            dialect=model_context.dialect,
            statements=expressions[0:-1],
            start=self.start,
            **self.sqlmesh_model_kwargs(model_context),
        )
