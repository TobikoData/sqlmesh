from __future__ import annotations

import typing as t

from sqlmesh.core.config.base import BaseConfig
from sqlmesh.core.model.kind import ModelKind, model_kind_validator
from sqlmesh.utils.date import TimeLike


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
    """

    kind: t.Optional[ModelKind] = None
    dialect: t.Optional[str] = None
    cron: t.Optional[str] = None
    owner: t.Optional[str] = None
    start: t.Optional[TimeLike] = None
    storage_format: t.Optional[str] = None
    session_properties: t.Optional[t.Dict[str, t.Any]] = None

    _model_kind_validator = model_kind_validator
