from __future__ import annotations

import typing as t
from enum import Enum

from pydantic import Field, validator
from sqlglot import exp
from sqlglot.time import format_time

from sqlmesh.core import dialect as d
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel


# TODO: switch to autoname when sqlglot is typed
class ModelKindName(str, Enum):
    """The kind of model, determining how this data is computed and stored in the warehouse."""

    INCREMENTAL_BY_TIME_RANGE = "INCREMENTAL_BY_TIME_RANGE"
    INCREMENTAL_BY_UNIQUE_KEY = "INCREMENTAL_BY_UNIQUE_KEY"
    FULL = "FULL"
    VIEW = "VIEW"
    EMBEDDED = "EMBEDDED"
    SEED = "SEED"
    # TODO: Add support for snapshots
    # SNAPSHOT = "SNAPSHOT"


class ModelKind(PydanticModel):
    name: ModelKindName

    @property
    def is_incremental_by_time_range(self) -> bool:
        return self.name == ModelKindName.INCREMENTAL_BY_TIME_RANGE

    @property
    def is_incremental_by_unique_key(self) -> bool:
        return self.name == ModelKindName.INCREMENTAL_BY_UNIQUE_KEY

    @property
    def is_full(self) -> bool:
        return self.name == ModelKindName.FULL

    # @property
    # def is_snapshot(self) -> bool:
    #     return self.name == ModelKindName.SNAPSHOT

    @property
    def is_view(self) -> bool:
        return self.name == ModelKindName.VIEW

    @property
    def is_embedded(self) -> bool:
        return self.name == ModelKindName.EMBEDDED

    @property
    def is_seed(self) -> bool:
        return self.name == ModelKindName.SEED

    @property
    def is_materialized(self) -> bool:
        return self.name not in (ModelKindName.VIEW, ModelKindName.EMBEDDED)

    @property
    def only_latest(self) -> bool:
        """Whether or not this model only cares about latest date to render."""
        return self.name in (ModelKindName.VIEW, ModelKindName.FULL)

    def to_expression(self, **kwargs: t.Any) -> d.ModelKind:
        return d.ModelKind(this=self.name.value.upper(), **kwargs)


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

    def to_expression(self, dialect: str) -> exp.Column | exp.Tuple:
        """Convert this pydantic model into a time_column SQLGlot expression."""
        column = exp.to_column(self.column)
        if not self.format:
            return column

        return exp.Tuple(
            expressions=[
                column,
                exp.Literal.string(
                    format_time(
                        self.format,
                        d.Dialect.get_or_raise(dialect).inverse_time_mapping,  # type: ignore
                    )
                ),
            ]
        )


class IncrementalByTimeRangeKind(ModelKind):
    name: ModelKindName = Field(ModelKindName.INCREMENTAL_BY_TIME_RANGE, const=True)
    time_column: TimeColumn

    @validator("time_column", pre=True)
    def _parse_time_column(cls, v: t.Any) -> TimeColumn:
        if isinstance(v, exp.Tuple):
            kwargs = {
                key: v.expressions[i].name
                for i, key in enumerate(("column", "format")[: len(v.expressions)])
            }
            return TimeColumn(**kwargs)

        if isinstance(v, exp.Identifier):
            return TimeColumn(column=v.name)

        if isinstance(v, exp.Expression):
            return TimeColumn(column=v.name)

        if isinstance(v, str):
            return TimeColumn(column=v)
        return v

    def to_expression(self, dialect: str = "", **kwargs: t.Any) -> d.ModelKind:
        return super().to_expression(
            expressions=[
                exp.Property(this="time_column", value=self.time_column.to_expression(dialect))
            ],
        )


class IncrementalByUniqueKeyKind(ModelKind):
    name: ModelKindName = Field(ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, const=True)
    unique_key: t.List[str]

    @validator("unique_key", pre=True)
    def _parse_unique_key(cls, v: t.Any) -> t.List[str]:
        if isinstance(v, exp.Identifier):
            return [v.this]
        if isinstance(v, exp.Tuple):
            return [e.this for e in v.expressions]
        return [i.this if isinstance(i, exp.Identifier) else str(i) for i in v]


class SeedKind(ModelKind):
    name: ModelKindName = Field(ModelKindName.SEED, const=True)
    path: str
    batch_size: int = 1000

    @validator("batch_size", pre=True)
    def _parse_batch_size(cls, v: t.Any) -> int:
        if isinstance(v, exp.Expression) and v.is_int:
            v = int(v.name)
        if not isinstance(v, int):
            raise ValueError("Seed batch size must be an integer value")
        if v <= 0:
            raise ValueError("Seed batch size must be a positive integer")
        return v

    @validator("path", pre=True)
    def _parse_path(cls, v: t.Any) -> str:
        if isinstance(v, exp.Literal):
            return v.this
        return str(v)

    def to_expression(self, **kwargs: t.Any) -> d.ModelKind:
        """Convert the seed kind into a SQLGlot expression."""
        return super().to_expression(
            expressions=[
                exp.Property(this=exp.Var(this="path"), value=exp.Literal.string(self.path)),
                exp.Property(
                    this=exp.Var(this="batch_size"),
                    value=exp.Literal.number(self.batch_size),
                ),
            ],
        )


def _model_kind_validator(v: t.Any) -> ModelKind:
    if isinstance(v, ModelKind):
        return v

    if isinstance(v, d.ModelKind):
        name = v.this
        props = {prop.name: prop.args.get("value") for prop in v.expressions}
        klass: t.Type[ModelKind] = ModelKind
        if name == ModelKindName.INCREMENTAL_BY_TIME_RANGE:
            klass = IncrementalByTimeRangeKind
        elif name == ModelKindName.INCREMENTAL_BY_UNIQUE_KEY:
            klass = IncrementalByUniqueKeyKind
        elif name == ModelKindName.SEED:
            klass = SeedKind
        else:
            props["name"] = ModelKindName(name)
        return klass(**props)

    if isinstance(v, dict):
        if v.get("name") == ModelKindName.INCREMENTAL_BY_TIME_RANGE:
            klass = IncrementalByTimeRangeKind
        elif v.get("name") == ModelKindName.INCREMENTAL_BY_UNIQUE_KEY:
            klass = IncrementalByUniqueKeyKind
        elif v.get("name") == ModelKindName.SEED:
            klass = SeedKind
        else:
            klass = ModelKind
        return klass(**v)

    name = (v.name if isinstance(v, exp.Expression) else str(v)).upper()

    try:
        return ModelKind(name=ModelKindName(name))
    except ValueError:
        raise ConfigError(f"Invalid model kind '{name}'")


model_kind_validator = validator("kind", pre=True, allow_reuse=True)(_model_kind_validator)
