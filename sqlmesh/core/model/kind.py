from __future__ import annotations

import sys
import typing as t
from enum import Enum

from pydantic import Field
from sqlglot import exp
from sqlglot.time import format_time

from sqlmesh.core import dialect as d
from sqlmesh.core.model.common import parse_expressions, parse_properties
from sqlmesh.core.model.seed import CsvSettings
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import (
    PydanticModel,
    SQLGlotBool,
    SQLGlotPositiveInt,
    SQLGlotString,
    field_validator,
    model_validator,
    model_validator_v1_args,
)

if sys.version_info >= (3, 9):
    from typing import Annotated, Literal
else:
    from typing_extensions import Annotated, Literal


class ModelKindMixin:
    @property
    def model_kind_name(self) -> t.Optional[ModelKindName]:
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
    def is_scd_type_2(self) -> bool:
        return self.model_kind_name == ModelKindName.SCD_TYPE_2

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
        return self.is_view or self.is_full or self.is_incremental_unmanaged or self.is_scd_type_2


class ModelKindName(str, ModelKindMixin, Enum):
    """The kind of model, determining how this data is computed and stored in the warehouse."""

    INCREMENTAL_BY_TIME_RANGE = "INCREMENTAL_BY_TIME_RANGE"
    INCREMENTAL_BY_UNIQUE_KEY = "INCREMENTAL_BY_UNIQUE_KEY"
    INCREMENTAL_UNMANAGED = "INCREMENTAL_UNMANAGED"
    FULL = "FULL"
    SCD_TYPE_2 = "SCD_TYPE_2"
    VIEW = "VIEW"
    EMBEDDED = "EMBEDDED"
    SEED = "SEED"
    EXTERNAL = "EXTERNAL"

    @property
    def model_kind_name(self) -> t.Optional[ModelKindName]:
        return self


_unique_key_validator = field_validator("unique_key", mode="before")(parse_expressions)


class _ModelKind(PydanticModel, ModelKindMixin):
    name: ModelKindName

    @property
    def model_kind_name(self) -> t.Optional[ModelKindName]:
        return self.name

    def to_expression(self, **kwargs: t.Any) -> d.ModelKind:
        return d.ModelKind(this=self.name.value.upper(), **kwargs)

    @property
    def data_hash_values(self) -> t.List[t.Optional[str]]:
        return [self.name.value]


class TimeColumn(PydanticModel):
    column: str
    format: t.Optional[str] = None

    @classmethod
    def validator(cls) -> classmethod:
        def _time_column_validator(v: t.Any) -> TimeColumn:
            if isinstance(v, exp.Tuple):
                kwargs = {
                    key: v.expressions[i].name
                    for i, key in enumerate(("column", "format")[: len(v.expressions)])
                }
                return TimeColumn(**kwargs)

            if isinstance(v, exp.Expression):
                return TimeColumn(column=v.name)

            if isinstance(v, str):
                return TimeColumn(column=v)
            return v

        return field_validator("time_column", mode="before")(_time_column_validator)

    @field_validator("column", mode="before")
    @classmethod
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


class _Incremental(_ModelKind):
    batch_size: t.Optional[SQLGlotPositiveInt] = None
    lookback: t.Optional[SQLGlotPositiveInt] = None
    forward_only: SQLGlotBool = False
    disable_restatement: SQLGlotBool = False

    @model_validator(mode="after")
    @model_validator_v1_args
    def _kind_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        batch_size = values.get("batch_size")
        lookback = values.get("lookback") or 0
        if batch_size and batch_size < lookback:
            raise ValueError("batch_size cannot be less than lookback")
        return values


class IncrementalByTimeRangeKind(_Incremental):
    name: Literal[ModelKindName.INCREMENTAL_BY_TIME_RANGE] = ModelKindName.INCREMENTAL_BY_TIME_RANGE
    time_column: TimeColumn

    _time_column_validator = TimeColumn.validator()

    def to_expression(self, dialect: str = "", **kwargs: t.Any) -> d.ModelKind:
        return super().to_expression(expressions=[self.time_column.to_property(dialect)])


class IncrementalByUniqueKeyKind(_Incremental):
    name: Literal[ModelKindName.INCREMENTAL_BY_UNIQUE_KEY] = ModelKindName.INCREMENTAL_BY_UNIQUE_KEY
    unique_key: t.List[exp.Expression]
    _unique_key_validator = _unique_key_validator


class IncrementalUnmanagedKind(_ModelKind):
    name: Literal[ModelKindName.INCREMENTAL_UNMANAGED] = ModelKindName.INCREMENTAL_UNMANAGED
    insert_overwrite: SQLGlotBool = False
    forward_only: SQLGlotBool = True
    disable_restatement: Literal[True] = True


class ViewKind(_ModelKind):
    name: Literal[ModelKindName.VIEW] = ModelKindName.VIEW
    materialized: SQLGlotBool = False

    @property
    def data_hash_values(self) -> t.List[t.Optional[str]]:
        return [*super().data_hash_values, str(self.materialized)]


class SeedKind(_ModelKind):
    name: Literal[ModelKindName.SEED] = ModelKindName.SEED
    path: SQLGlotString
    batch_size: SQLGlotPositiveInt = 1000
    csv_settings: t.Optional[CsvSettings] = None

    @field_validator("csv_settings", mode="before")
    @classmethod
    def _parse_csv_settings(cls, v: t.Any) -> t.Optional[CsvSettings]:
        if v is None or isinstance(v, CsvSettings):
            return v
        if isinstance(v, exp.Expression):
            tuple_exp = parse_properties(cls, v, {})
            if not tuple_exp:
                return None
            return CsvSettings(**{e.left.name: e.right for e in tuple_exp.expressions})
        if isinstance(v, dict):
            return CsvSettings(**v)
        return v

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

    @property
    def data_hash_values(self) -> t.List[t.Optional[str]]:
        return [
            *super().data_hash_values,
            *(self.csv_settings or CsvSettings()).dict().values(),
        ]


class FullKind(_ModelKind):
    name: Literal[ModelKindName.FULL] = ModelKindName.FULL


class SCDType2Kind(_ModelKind):
    name: Literal[ModelKindName.SCD_TYPE_2] = ModelKindName.SCD_TYPE_2
    unique_key: t.List[exp.Expression]
    valid_from_name: SQLGlotString = "valid_from"
    valid_to_name: SQLGlotString = "valid_to"
    updated_at_name: SQLGlotString = "updated_at"

    forward_only: SQLGlotBool = True
    disable_restatement: SQLGlotBool = True
    _unique_key_validator = _unique_key_validator

    @property
    def managed_columns(self) -> t.Dict[str, exp.DataType]:
        return {
            self.valid_from_name: exp.DataType.build("TIMESTAMP"),
            self.valid_to_name: exp.DataType.build("TIMESTAMP"),
        }


class EmbeddedKind(_ModelKind):
    name: Literal[ModelKindName.EMBEDDED] = ModelKindName.EMBEDDED


class ExternalKind(_ModelKind):
    name: Literal[ModelKindName.EXTERNAL] = ModelKindName.EXTERNAL


ModelKind = Annotated[
    t.Union[
        EmbeddedKind,
        ExternalKind,
        FullKind,
        IncrementalByTimeRangeKind,
        IncrementalByUniqueKeyKind,
        IncrementalUnmanagedKind,
        SeedKind,
        ViewKind,
        SCDType2Kind,
    ],
    Field(discriminator="name"),
]

MODEL_KIND_NAME_TO_TYPE: t.Dict[str, t.Type[ModelKind]] = {
    ModelKindName.EMBEDDED: EmbeddedKind,
    ModelKindName.EXTERNAL: ExternalKind,
    ModelKindName.FULL: FullKind,
    ModelKindName.INCREMENTAL_BY_TIME_RANGE: IncrementalByTimeRangeKind,
    ModelKindName.INCREMENTAL_BY_UNIQUE_KEY: IncrementalByUniqueKeyKind,
    ModelKindName.INCREMENTAL_UNMANAGED: IncrementalUnmanagedKind,
    ModelKindName.SEED: SeedKind,
    ModelKindName.VIEW: ViewKind,
    ModelKindName.SCD_TYPE_2: SCDType2Kind,
}


def model_kind_type_from_name(name: t.Optional[str]) -> t.Type[ModelKind]:
    klass = MODEL_KIND_NAME_TO_TYPE.get(name) if name else None
    if not klass:
        raise ConfigError(f"Invalid model kind '{name}'")
    return t.cast(t.Type[ModelKind], klass)


def _model_kind_validator(v: t.Any) -> ModelKind:
    if isinstance(v, _ModelKind):
        return t.cast(ModelKind, v)

    if isinstance(v, d.ModelKind):
        name = v.this
        props = {prop.name: prop.args.get("value") for prop in v.expressions}
        return model_kind_type_from_name(name)(**props)

    if isinstance(v, dict):
        return model_kind_type_from_name(v.get("name"))(**v)

    name = (v.name if isinstance(v, exp.Expression) else str(v)).upper()
    return model_kind_type_from_name(name)()  # type: ignore


model_kind_validator = field_validator("kind", mode="before")(_model_kind_validator)
