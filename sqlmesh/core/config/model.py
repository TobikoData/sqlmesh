from __future__ import annotations

import typing as t

from pydantic import Field

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
        batch_size: The maximum number of intervals that can be run per backfill job. If this is None,
            then backfilling this model will do all of history in one job. If this is set, a model's backfill
            will be chunked such that each individual job will only contain jobs with max `batch_size` intervals.
        storage_format: The storage format used to store the physical table, only applicable in certain engines.
            (eg. 'parquet')
        catalog: The default catalog to use if one is not provided (catalog.schema.table).
        schema: The default schema to use if one is not provided. (catalog.schema.table).
    """

    kind: t.Optional[ModelKind] = None
    dialect: t.Optional[str] = None
    cron: t.Optional[str] = None
    owner: t.Optional[str] = None
    start: t.Optional[TimeLike] = None
    batch_size: t.Optional[int] = None
    storage_format: t.Optional[str] = None
    catalog: t.Optional[str] = None
    schema_: t.Optional[str] = Field(default=None, alias="schema")

    _model_kind_validator = model_kind_validator
