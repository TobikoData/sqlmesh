from __future__ import annotations

import typing as t
from enum import Enum

from pydantic import Field, validator
from sqlglot import exp

from sqlmesh.utils.pydantic import PydanticModel


# switch to autoname with sqlglot is typed
class ModelKindEnum(str, Enum):
    """The kind of model, determining how this data is computed and stored in the warehouse."""

    INCREMENTAL_BY_TIME_RANGE = "incremental_by_time_range"
    INCREMENTAL_BY_UNIQUE_KEY = "incremental_by_unique_key"
    FULL = "full"
    SNAPSHOT = "snapshot"
    VIEW = "view"
    EMBEDDED = "embedded"


class ModelKind(PydanticModel):
    kind: ModelKindEnum

    @property
    def name(self):
        return self.kind.name

    @property
    def is_incremental_by_time_range(self):
        return self.kind == ModelKindEnum.INCREMENTAL_BY_TIME_RANGE

    @property
    def is_incremental_by_unique_key(self):
        return self.kind == ModelKindEnum.INCREMENTAL_BY_UNIQUE_KEY

    @property
    def is_full(self):
        return self.kind == ModelKindEnum.FULL

    @property
    def is_snapshot(self):
        return self.kind == ModelKindEnum.SNAPSHOT

    @property
    def is_view(self):
        return self.kind == ModelKindEnum.VIEW

    @property
    def is_embedded(self):
        return self.kind == ModelKindEnum.EMBEDDED

    @property
    def is_materialized(self):
        return self.kind not in (ModelKindEnum.VIEW, ModelKindEnum.EMBEDDED)

    @property
    def only_latest(self):
        """Whether or not this model only cares about latest date to render."""
        return self.kind in (ModelKindEnum.VIEW, ModelKindEnum.FULL)


class TimeColumn(PydanticModel):
    column: str
    format: t.Optional[str] = None

    @property
    def expression(self) -> exp.Column | exp.Tuple:
        """Convert this pydantic model into a time_column SQLGlot expression."""
        column = exp.to_column(self.column)
        if not self.format:
            return column

        return exp.Tuple(expressions=[column, exp.Literal.string(self.format)])


class IncrementalByTimeRange(ModelKind):
    kind: ModelKindEnum = Field(ModelKindEnum.INCREMENTAL_BY_TIME_RANGE, const=True)
    time_column: TimeColumn = TimeColumn(column="ds")

    @validator("time_column", pre=True)
    def _parse_time_column(cls, v: t.Any) -> TimeColumn:
        if isinstance(v, list):
            kwargs = {
                key: v[i].name for i, key in enumerate(("column", "format")[: len(v)])
            }
            return TimeColumn(**kwargs)

        if isinstance(v, exp.Identifier):
            return TimeColumn(column=v.name)
        return v


class IncrementalByUniqueKey(ModelKind):
    kind: ModelKindEnum = Field(ModelKindEnum.INCREMENTAL_BY_UNIQUE_KEY, const=True)
    unique_key: t.List[str]

    @validator("unique_key", pre=True)
    def _parse_unique_key(cls, v: t.Any) -> t.List[str]:
        if isinstance(v, exp.Identifier):
            return [v.this]
        return [i.this if isinstance(i, exp.Identifier) else str(i) for i in v]
