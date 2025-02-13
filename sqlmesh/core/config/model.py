from __future__ import annotations

import typing as t

from sqlmesh.core.dialect import parse_one, extract_func_call
from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.model.kind import (
    ModelKind,
    OnDestructiveChange,
    model_kind_validator,
    on_destructive_change_validator,
)
from sqlmesh.core.node import IntervalUnit
from sqlmesh.utils.date import TimeLike
from sqlmesh.core.model.meta import FunctionCall
from sqlmesh.utils.pydantic import field_validator


class ModelDefaultsConfig(BaseConfig):
    """A config object for default values applied to model definitions.

    Args:
        kind: The model kind.
        dialect: The SQL dialect that the model's query is written in.
        cron: A cron string specifying how often the model should be refreshed, leveraging the
            [croniter](https://github.com/kiorky/croniter) library.
        owner: The owner of the model.
        start: The earliest date that the model will be backfilled for. If this is None,
            then the date is inferred by taking the most recent start date of its ancestors.
            The start date can be a static datetime or a relative datetime like "1 year ago"
        table_format: The table format used to manage the physical table files defined by `storage_format`, only applicable in certain engines.
            (eg, 'iceberg', 'delta', 'hudi')
        storage_format: The storage format used to store the physical table, only applicable in certain engines.
            (eg. 'parquet', 'orc')
        on_destructive_change: What should happen when a forward-only model requires a destructive schema change.
        physical_properties: A key-value mapping of arbitrary properties that are applied to the model table / view in the physical layer.
        virtual_properties: A key-value mapping of arbitrary properties that are applied to the model view in the virtual layer.
        session_properties: A key-value mapping of properties specific to the target engine that are applied to the engine session.
        audits: The audits to be applied globally to all models in the project.
        optimize_query: Whether the SQL models should be optimized.
        validate_query: Whether the SQL models should be validated at compile time.
        allow_partials: Whether the models can process partial (incomplete) data intervals.
        enabled: Whether the models are enabled.
        interval_unit: The temporal granularity of the models data intervals. By default computed from cron.
        column_descriptions: A key-value mapping of column names to their descriptions.
        description: Description of the models. Automatically registered in the SQL engine's table COMMENT field or equivalent.
        grains: The column(s) whose combination uniquely identifies each row in the models.
        references: The model column(s) used to join to models' grains.
        clustered_by: The column(s) used to cluster the models' physical table.
        partitioned_by: The column(s) used to define the models' partitioning key.
        tags: Arbitrary strings used to organize or classify the models.

    """

    kind: t.Optional[ModelKind] = None
    dialect: t.Optional[str] = None
    cron: t.Optional[str] = None
    owner: t.Optional[str] = None
    start: t.Optional[TimeLike] = None
    table_format: t.Optional[str] = None
    storage_format: t.Optional[str] = None
    on_destructive_change: t.Optional[OnDestructiveChange] = None
    physical_properties: t.Optional[t.Dict[str, t.Any]] = None
    virtual_properties: t.Optional[t.Dict[str, t.Any]] = None
    session_properties: t.Optional[t.Dict[str, t.Any]] = None
    audits: t.Optional[t.List[FunctionCall]] = None
    optimize_query: t.Optional[bool] = None
    validate_query: t.Optional[bool] = None
    allow_partials: t.Optional[bool] = None
    interval_unit: t.Optional[IntervalUnit] = None
    enabled: t.Optional[bool] = None
    description: t.Optional[str] = None
    column_descriptions: t.Optional[t.Dict[str, str]] = None
    grains: t.Optional[t.List[str]] = None
    references: t.Optional[t.List[str]] = None
    clustered_by: t.Optional[t.Any] = None
    partitioned_by: t.Optional[t.Any] = None
    tags: t.Optional[t.List[str]] = None

    _model_kind_validator = model_kind_validator
    _on_destructive_change_validator = on_destructive_change_validator

    @field_validator("audits", mode="before")
    def _audits_validator(cls, v: t.Any) -> t.Any:
        if isinstance(v, list):
            return [extract_func_call(parse_one(audit)) for audit in v]

        return v
