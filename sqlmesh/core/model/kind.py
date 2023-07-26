from __future__ import annotations

import sys
import typing as t
from enum import Enum

from pydantic import Field, root_validator, validator
from pydantic.fields import ModelField
from sqlglot import exp
from sqlglot.time import format_time

from sqlmesh.core import dialect as d
from sqlmesh.core.model.common import bool_validator
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import PydanticModel

if sys.version_info >= (3, 9):
    from typing import Literal
else:
    from typing_extensions import Literal


class ModelKindMixin:
    @property
    def model_kind_name(self) -> ModelKindName:
        """Returns the model kind name."""
        raise NotImplementedError

    @property
    def is_incremental_by_time_range(self) -> bool:
        return self.model_kind_name == ModelKindName.INCREMENTAL_BY_TIME_RANGE

    @property
    def is_incremental_by_unique_key(self) -> bool:
        return self.model_kind_name == ModelKindName.INCREMENTAL_BY_UNIQUE_KEY

    @property
    def is_incremental_unmanaged(self) -> bool:
        return self.model_kind_name == ModelKindName.INCREMENTAL_UNMANAGED

    @property
    def is_full(self) -> bool:
        return self.model_kind_name == ModelKindName.FULL

    @property
    def is_view(self) -> bool:
        return self.model_kind_name == ModelKindName.VIEW

    @property
    def is_embedded(self) -> bool:
        return self.model_kind_name == ModelKindName.EMBEDDED

    @property
    def is_seed(self) -> bool:
        return self.model_kind_name == ModelKindName.SEED

    @property
    def is_external(self) -> bool:
        return self.model_kind_name == ModelKindName.EXTERNAL

    @property
    def is_symbolic(self) -> bool:
        """A symbolic model is one that doesn't execute at all."""
        return self.model_kind_name in (ModelKindName.EMBEDDED, ModelKindName.EXTERNAL)

    @property
    def is_materialized(self) -> bool:
        return not (self.is_symbolic or self.is_view)

    @property
    def only_execution_time(self) -> bool:
        """Whether or not this model only cares about execution time to render."""
        return self.is_view or self.is_full or self.is_incremental_unmanaged


class ModelKindName(str, ModelKindMixin, Enum):
    """The kind of model, determining how this data is computed and stored in the warehouse."""

    INCREMENTAL_BY_TIME_RANGE = "INCREMENTAL_BY_TIME_RANGE"
    INCREMENTAL_BY_UNIQUE_KEY = "INCREMENTAL_BY_UNIQUE_KEY"
    INCREMENTAL_UNMANAGED = "INCREMENTAL_UNMANAGED"
    FULL = "FULL"
    VIEW = "VIEW"
    EMBEDDED = "EMBEDDED"
    SEED = "SEED"
    EXTERNAL = "EXTERNAL"

    @property
    def model_kind_name(self) -> ModelKindName:
        return self


class ModelKind(PydanticModel, ModelKindMixin):
    name: ModelKindName

    @classmethod
    def field_validator(cls) -> classmethod:
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
                elif name == ModelKindName.VIEW:
                    klass = ViewKind
                else:
                    props["name"] = ModelKindName(name)
                return klass(**props)

            if isinstance(v, dict):
                if v.get("name") == ModelKindName.INCREMENTAL_BY_TIME_RANGE:
                    klass = IncrementalByTimeRangeKind
                elif v.get("name") == ModelKindName.INCREMENTAL_BY_UNIQUE_KEY:
                    klass = IncrementalByUniqueKeyKind
                elif v.get("name") == ModelKindName.INCREMENTAL_UNMANAGED:
                    klass = IncrementalUnmanagedKind
                elif v.get("name") == ModelKindName.SEED:
                    klass = SeedKind
                elif v.get("name") == ModelKindName.VIEW:
                    klass = ViewKind
                else:
                    klass = ModelKind
                return klass(**v)

            name = (v.name if isinstance(v, exp.Expression) else str(v)).upper()

            try:
                return ModelKind(name=ModelKindName(name))
            except ValueError:
                raise ConfigError(f"Invalid model kind '{name}'")

        return validator("kind", pre=True, allow_reuse=True)(_model_kind_validator)

    @property
    def model_kind_name(self) -> ModelKindName:
        return self.name

    def to_expression(self, **kwargs: t.Any) -> d.ModelKind:
        return d.ModelKind(this=self.name.value.upper(), **kwargs)


class TimeColumn(PydanticModel):
    column: str
    format: t.Optional[str] = None

    @classmethod
    def field_validator(cls) -> classmethod:
        def _time_column_validator(v: t.Any) -> TimeColumn:
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

        return validator("time_column", pre=True, allow_reuse=True)(_time_column_validator)

    @validator("column", pre=True)
    def _column_validator(cls, v: str) -> str:
        if not v:
            raise ConfigError("Time Column cannot be empty.")
        return v

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
                    format_time(self.format, d.Dialect.get_or_raise(dialect).INVERSE_TIME_MAPPING)
                ),
            ]
        )

    def to_property(self, dialect: str = "") -> exp.Property:
        return exp.Property(this="time_column", value=self.to_expression(dialect))


class _Incremental(ModelKind):
    batch_size: t.Optional[int]
    lookback: t.Optional[int]
    forward_only: bool = False
    disable_restatement: bool = False

    _bool_validator = bool_validator

    @validator("batch_size", "lookback", pre=True)
    def _int_validator(cls, v: t.Any, field: ModelField) -> t.Optional[int]:
        if isinstance(v, exp.Expression):
            num = int(v.name)
        else:
            num = int(v)
        if num < 0:
            raise ConfigError(
                f"Invalid value {num} for {field.name}. The value should be greater than 0"
            )
        return num

    @root_validator
    def _kind_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        batch_size = values.get("batch_size")
        lookback = values.get("lookback") or 0
        if batch_size and batch_size < lookback:
            raise ValueError("batch_size cannot be less than lookback")
        return values


class IncrementalByTimeRangeKind(_Incremental):
    name: ModelKindName = Field(ModelKindName.INCREMENTAL_BY_TIME_RANGE, const=True)
    time_column: TimeColumn

    _time_column_validator = TimeColumn.field_validator()

    def to_expression(self, dialect: str = "", **kwargs: t.Any) -> d.ModelKind:
        return super().to_expression(expressions=[self.time_column.to_property(dialect)])


class IncrementalByUniqueKeyKind(_Incremental):
    name: ModelKindName = Field(ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, const=True)
    unique_key: t.List[str]

    @validator("unique_key", pre=True)
    def _parse_unique_key(cls, v: t.Any) -> t.List[str]:
        if isinstance(v, exp.Identifier):
            return [v.this]
        if isinstance(v, exp.Tuple):
            return [e.this for e in v.expressions]
        return [i.this if isinstance(i, exp.Identifier) else str(i) for i in v]


class IncrementalUnmanagedKind(ModelKind):
    name: ModelKindName = Field(ModelKindName.INCREMENTAL_UNMANAGED, const=True)
    insert_overwrite: bool = False
    forward_only: bool = True
    disable_restatement: Literal[True] = True

    _bool_validator = bool_validator


class ViewKind(ModelKind):
    name: ModelKindName = Field(ModelKindName.VIEW, const=True)
    materialized: bool = False

    @validator("materialized", pre=True)
    def _parse_materialized(cls, v: t.Any) -> bool:
        if isinstance(v, exp.Expression):
            return bool(v.this)
        return bool(v)


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
