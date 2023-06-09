from __future__ import annotations

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
    ViewKind,
    create_sql_model,
)
from sqlmesh.dbt.basemodel import BaseModelConfig, Materialization
from sqlmesh.dbt.common import SqlStr, extract_jinja_config
from sqlmesh.dbt.target import TargetConfig
from sqlmesh.utils.errors import ConfigError

if t.TYPE_CHECKING:
    from sqlmesh.dbt.context import DbtContext

INCREMENTAL_BY_TIME_STRATEGIES = set(["delete+insert", "insert_overwrite"])
INCREMENTAL_BY_UNIQUE_KEY_STRATEGIES = set(["merge"])


def collection_to_str(collection: t.Iterable) -> str:
    return ", ".join(f"'{item}'" for item in collection)


class ModelConfig(BaseModelConfig):
    """
    ModelConfig contains all config parameters available to DBT models

    See https://docs.getdbt.com/reference/configs-and-properties for
    a more detailed description of each config parameter under the
    General propreties, General configs, and For models sections.

    Args:
        sql: The model sql
        time_column: The name of the time column
        partitioned_by: List of columns to partition by. time_column will automatically be
            included, if specified.
        cron: A cron string specifying how often the model should be refreshed, leveraging the
            [croniter](https://github.com/kiorky/croniter) library.
        dialect: The SQL dialect that the model's query is written in. By default,
            this is assumed to be the dialect of the context.
        batch_size: The maximum number of incremental intervals that can be run per backfill job. If this is None,
            then backfilling this model will do all of history in one job. If this is set, a model's backfill
            will be chunked such that each individual job will only contain jobs with max `batch_size` intervals.
        lookback: The number of previous incremental intervals in the lookback window.
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
    partitioned_by: t.Optional[t.Union[t.List[str], str]] = None
    cron: t.Optional[str] = None
    dialect: t.Optional[str] = None
    batch_size: t.Optional[int] = None
    lookback: t.Optional[int] = None

    # DBT configuration fields
    start: t.Optional[str] = None
    cluster_by: t.Optional[t.List[str]] = None
    incremental_strategy: t.Optional[str] = None
    materialized: str = Materialization.VIEW.value
    sql_header: t.Optional[str] = None
    unique_key: t.Optional[t.List[str]] = None

    # redshift
    bind: t.Optional[bool] = None

    # Private fields
    _sql_embedded_config: t.Optional[SqlStr] = None
    _sql_no_config: t.Optional[SqlStr] = None

    @validator(
        "unique_key",
        "cluster_by",
        "partitioned_by",
        pre=True,
    )
    def _validate_list(cls, v: t.Union[str, t.List[str]]) -> t.List[str]:
        return ensure_list(v)

    @validator("sql", pre=True)
    def _validate_sql(cls, v: t.Union[str, SqlStr]) -> SqlStr:
        return SqlStr(v)

    _FIELD_UPDATE_STRATEGY: t.ClassVar[t.Dict[str, UpdateStrategy]] = {
        **BaseModelConfig._FIELD_UPDATE_STRATEGY,
        **{
            "sql": UpdateStrategy.IMMUTABLE,
            "time_column": UpdateStrategy.IMMUTABLE,
        },
    }

    @property
    def model_dialect(self) -> t.Optional[str]:
        return self.dialect or self.meta.get("dialect", None)

    @property
    def model_materialization(self) -> Materialization:
        return Materialization(self.materialized.lower())

    def model_kind(self, target: TargetConfig) -> ModelKind:
        """
        Get the sqlmesh ModelKind
        Returns:
            The sqlmesh ModelKind
        """
        materialization = self.model_materialization
        if materialization == Materialization.TABLE:
            return ModelKind(name=ModelKindName.FULL)
        if materialization == Materialization.VIEW:
            return ViewKind()
        if materialization == Materialization.INCREMENTAL:
            incremental_kwargs = {}
            for field in ("batch_size", "lookback"):
                field_val = getattr(self, field, None) or self.meta.get(field, None)
                if field_val:
                    incremental_kwargs[field] = field_val

            if self.time_column:
                strategy = self.incremental_strategy or target.default_incremental_strategy(
                    IncrementalByTimeRangeKind
                )
                if strategy not in INCREMENTAL_BY_TIME_STRATEGIES:
                    raise ConfigError(
                        f"SQLMesh IncrementalByTime is not compatible with '{strategy}'"
                        f" incremental strategy. Supported strategies include {collection_to_str(INCREMENTAL_BY_TIME_STRATEGIES)}."
                    )
                return IncrementalByTimeRangeKind(
                    time_column=self.time_column, **incremental_kwargs
                )
            if self.unique_key:
                strategy = self.incremental_strategy or target.default_incremental_strategy(
                    IncrementalByUniqueKeyKind
                )
                if (
                    self.incremental_strategy
                    and strategy not in INCREMENTAL_BY_UNIQUE_KEY_STRATEGIES
                ):
                    raise ConfigError(
                        f"{self.sql_name}: SQLMesh IncrementalByUniqueKey is not compatible with '{strategy}'"
                        f" incremental strategy. Supported strategies include {collection_to_str(INCREMENTAL_BY_UNIQUE_KEY_STRATEGIES)}."
                    )
                return IncrementalByUniqueKeyKind(unique_key=self.unique_key, **incremental_kwargs)

            raise ConfigError(
                f"{self.sql_name}: Incremental materialization requires either a "
                f"time_column {collection_to_str(INCREMENTAL_BY_TIME_STRATEGIES)}) or a "
                f"unique_key ({collection_to_str(INCREMENTAL_BY_UNIQUE_KEY_STRATEGIES.union(['none']))}) configuration."
            )
        if materialization == Materialization.EPHEMERAL:
            return ModelKind(name=ModelKindName.EMBEDDED)
        raise ConfigError(f"{materialization.value} materialization not supported.")

    @property
    def sql_no_config(self) -> SqlStr:
        if self._sql_no_config is None:
            self._sql_no_config = SqlStr("")
            self._extract_sql_config()
        return self._sql_no_config

    @property
    def sql_embedded_config(self) -> SqlStr:
        if self._sql_embedded_config is None:
            self._sql_embedded_config = SqlStr("")
            self._extract_sql_config()
        return self._sql_embedded_config

    def _extract_sql_config(self) -> None:
        no_config, embedded_config = extract_jinja_config(self.sql)
        self._sql_no_config = SqlStr(no_config)
        self._sql_embedded_config = SqlStr(embedded_config)

    def to_sqlmesh(self, context: DbtContext) -> Model:
        """Converts the dbt model into a SQLMesh model."""
        dialect = self.model_dialect or context.dialect
        query = d.jinja_query(self.sql_no_config)

        optional_kwargs: t.Dict[str, t.Any] = {}
        if self.partitioned_by:
            optional_kwargs["partitioned_by"] = self.partitioned_by
        for field in ["cron"]:
            field_val = getattr(self, field, None) or self.meta.get(field, None)
            if field_val:
                optional_kwargs[field] = field_val

        if not context.target:
            raise ConfigError(f"Target required to load '{self.sql_name}' into SQLMesh.")

        return create_sql_model(
            self.sql_name,
            query,
            dialect=dialect,
            kind=self.model_kind(context.target),
            start=self.start,
            **optional_kwargs,
            **self.sqlmesh_model_kwargs(context),
        )
