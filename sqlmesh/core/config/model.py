from __future__ import annotations

import typing as t

from sqlmesh.core.dialect import parse_one, extract_audit
from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.model.kind import (
    ModelKind,
    OnDestructiveChange,
    model_kind_validator,
    on_destructive_change_validator,
)
from sqlmesh.utils.date import TimeLike
from sqlmesh.core.model.meta import AuditReference
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
        storage_format: The storage format used to store the physical table, only applicable in certain engines.
            (eg. 'parquet')
        on_destructive_change: What should happen when a forward-only model requires a destructive schema change.
        audits: The audits to be applied globally to all models in the project.
    """

    kind: t.Optional[ModelKind] = None
    dialect: t.Optional[str] = None
    cron: t.Optional[str] = None
    owner: t.Optional[str] = None
    start: t.Optional[TimeLike] = None
    storage_format: t.Optional[str] = None
    on_destructive_change: t.Optional[OnDestructiveChange] = None
    session_properties: t.Optional[t.Dict[str, t.Any]] = None
    audits: t.Optional[t.List[AuditReference]] = None

    _model_kind_validator = model_kind_validator
    _on_destructive_change_validator = on_destructive_change_validator

    @field_validator("audits", mode="before")
    def _audits_validator(cls, v: t.Any) -> t.Any:
        if isinstance(v, list):
            return [extract_audit(parse_one(audit)) for audit in v]

        return v
