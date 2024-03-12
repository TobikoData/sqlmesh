from __future__ import annotations

import typing as t
from functools import cached_property

from pydantic import Field
from sqlglot import Dialect, exp
from sqlglot.helper import ensure_collection, ensure_list
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.core import dialect as d
from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.model.common import (
    bool_validator,
    default_catalog_validator,
    depends_on_validator,
    parse_properties,
    properties_validator,
)
from sqlmesh.core.model.kind import (
    IncrementalByUniqueKeyKind,
    ModelKind,
    SCDType2ByColumnKind,
    SCDType2ByTimeKind,
    TimeColumn,
    ViewKind,
    _Incremental,
    model_kind_validator,
)
from sqlmesh.core.node import _Node, str_or_exp_to_str
from sqlmesh.core.reference import Reference
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import (
    field_validator,
    field_validator_v1_args,
    list_of_fields_validator,
    model_validator,
    model_validator_v1_args,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SessionProperties

AuditReference = t.Tuple[str, t.Dict[str, exp.Expression]]


class ModelMeta(_Node):
    """Metadata for models which can be defined in SQL."""

    dialect: str = ""
    name: str
    kind: ModelKind = ViewKind()
    retention: t.Optional[int] = None  # not implemented yet
    storage_format: t.Optional[str] = None
    partitioned_by_: t.List[exp.Expression] = Field(default=[], alias="partitioned_by")
    clustered_by: t.List[str] = []
    default_catalog: t.Optional[str] = None
    depends_on_: t.Optional[t.Set[str]] = Field(default=None, alias="depends_on")
    columns_to_types_: t.Optional[t.Dict[str, exp.DataType]] = Field(default=None, alias="columns")
    column_descriptions_: t.Optional[t.Dict[str, str]] = None
    audits: t.List[AuditReference] = []
    grains: t.List[exp.Expression] = []
    references: t.List[exp.Expression] = []
    physical_schema_override: t.Optional[str] = None
    table_properties_: t.Optional[exp.Tuple] = Field(default=None, alias="table_properties")
    session_properties_: t.Optional[exp.Tuple] = Field(default=None, alias="session_properties")
    allow_partials: bool = False
    signals: t.List[exp.Tuple] = []

    _bool_validator = bool_validator
    _model_kind_validator = model_kind_validator
    _properties_validator = properties_validator
    _default_catalog_validator = default_catalog_validator
    _depends_on_validator = depends_on_validator

    @field_validator("audits", mode="before")
    def _audits_validator(cls, v: t.Any) -> t.Any:
        def extract(v: exp.Expression) -> t.Tuple[str, t.Dict[str, exp.Expression]]:
            kwargs = {}

            if isinstance(v, exp.Anonymous):
                func = v.name
                args = v.expressions
            elif isinstance(v, exp.Func):
                func = v.sql_name()
                args = list(v.args.values())
            else:
                return v.name.lower(), {}

            for arg in args:
                if not isinstance(arg, exp.EQ):
                    raise ConfigError(
                        f"Function '{func}' must be called with key-value arguments like {func}(arg=value)."
                    )
                kwargs[arg.left.name] = arg.right
            return func.lower(), kwargs

        if isinstance(v, (exp.Tuple, exp.Array)):
            return [extract(i) for i in v.expressions]
        if isinstance(v, exp.Paren):
            return [extract(v.this)]
        if isinstance(v, exp.Expression):
            return [extract(v)]
        if isinstance(v, list):
            return [
                (
                    entry[0].lower(),
                    {
                        key: d.parse(value)[0] if isinstance(value, str) else value
                        for key, value in entry[1].items()
                    },
                )
                for entry in v
            ]
        return v

    @field_validator("tags", mode="before")
    @field_validator_v1_args
    def _value_or_tuple_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> t.Any:
        return cls._validate_value_or_tuple(v, values)

    @field_validator("clustered_by", mode="before")
    @field_validator_v1_args
    def _normalized_value_or_tuple_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> t.Any:
        return cls._validate_value_or_tuple(v, values, normalize=True)

    @classmethod
    def _validate_value_or_tuple(
        cls, v: t.Dict[str, t.Any], values: t.Dict[str, t.Any], normalize: bool = False
    ) -> t.Any:
        dialect = values.get("dialect")
        _normalize = lambda v: normalize_identifiers(v, dialect=dialect) if normalize else v

        if isinstance(v, (exp.Tuple, exp.Array)):
            return [_normalize(e).name for e in v.expressions]
        if isinstance(v, exp.Expression):
            return [_normalize(v).name]
        if isinstance(v, str):
            value = _normalize(v)
            return value.name if isinstance(value, exp.Expression) else value

        return v

    @field_validator("dialect", "storage_format", mode="before")
    def _string_validator(cls, v: t.Any) -> t.Optional[str]:
        return str_or_exp_to_str(v)

    @field_validator("partitioned_by_", mode="before")
    @field_validator_v1_args
    def _partition_by_validator(
        cls, v: t.Any, values: t.Dict[str, t.Any]
    ) -> t.List[exp.Expression]:
        partitions = list_of_fields_validator(v, values)

        for partition in partitions:
            num_cols = len(list(partition.find_all(exp.Column)))

            error_msg: t.Optional[str] = None
            if num_cols == 0:
                error_msg = "does not contain a column"
            elif num_cols > 1:
                error_msg = "contains multiple columns"

            if error_msg:
                raise ConfigError(f"partitioned_by field '{partition}' {error_msg}")

        return partitions

    @field_validator("columns_to_types_", mode="before")
    @field_validator_v1_args
    def _columns_validator(
        cls, v: t.Any, values: t.Dict[str, t.Any]
    ) -> t.Optional[t.Dict[str, exp.DataType]]:
        columns_to_types = {}
        dialect = values.get("dialect")

        if isinstance(v, exp.Schema):
            for column in v.expressions:
                expr = column.args["kind"]
                expr.meta["dialect"] = dialect
                columns_to_types[normalize_identifiers(column, dialect=dialect).name] = expr

            return columns_to_types

        if isinstance(v, dict):
            udt = Dialect.get_or_raise(dialect).SUPPORTS_USER_DEFINED_TYPES
            for k, data_type in v.items():
                expr = exp.DataType.build(data_type, dialect=dialect, udt=udt)
                expr.meta["dialect"] = dialect
                columns_to_types[normalize_identifiers(k, dialect=dialect).name] = expr

            return columns_to_types

        return v

    @field_validator("grains", "references", mode="before")
    @field_validator_v1_args
    def _refs_validator(cls, vs: t.Any, values: t.Dict[str, t.Any]) -> t.List[exp.Expression]:
        dialect = values.get("dialect")

        if isinstance(vs, exp.Paren):
            vs = vs.unnest()

        if isinstance(vs, (exp.Tuple, exp.Array)):
            vs = vs.expressions
        else:
            vs = [
                d.parse_one(v, dialect=dialect) if isinstance(v, str) else v
                for v in ensure_collection(vs)
            ]

        refs = []

        for v in vs:
            v = exp.column(v) if isinstance(v, exp.Identifier) else v
            v.meta["dialect"] = dialect
            refs.append(v)

        return refs

    @field_validator("signals", mode="before")
    @field_validator_v1_args
    def _signals_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> t.Any:
        if v is None:
            return []

        if isinstance(v, str):
            dialect = values.get("dialect")
            v = d.parse_one(v, dialect=dialect)

        if isinstance(v, (exp.Array, exp.Paren, exp.Tuple)):
            tuples: t.List[exp.Expression] = (
                [v.unnest()] if isinstance(v, exp.Paren) else v.expressions
            )
            signals = [parse_properties(cls, t, values) for t in tuples]
        elif isinstance(v, list):
            signals = [parse_properties(cls, t, values) for t in v]
        else:
            raise ConfigError(f"Unexpected signals '{v}'")

        return signals

    @model_validator(mode="before")
    @model_validator_v1_args
    def _pre_root_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        grain = values.pop("grain", None)
        if grain:
            grains = values.get("grains")
            if grains:
                raise ConfigError(
                    f"Cannot use argument 'grain' ({grain}) with 'grains' ({grains}), use only grains"
                )
            values["grains"] = ensure_list(grain)
        return values

    @model_validator(mode="after")
    @model_validator_v1_args
    def _root_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        values = cls._kind_validator(values)
        return values

    @classmethod
    def _kind_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        kind = values.get("kind")
        if kind:
            for field in ("partitioned_by_", "clustered_by"):
                if values.get(field) and not kind.is_materialized:
                    raise ValueError(f"{field} field cannot be set for {kind} models")

            dialect = values.get("dialect")

            if hasattr(kind, "time_column"):
                kind.time_column.column = normalize_identifiers(
                    kind.time_column.column, dialect=dialect
                ).name

            if hasattr(kind, "unique_key"):
                kind.unique_key = [
                    normalize_identifiers(key, dialect=dialect) for key in kind.unique_key
                ]

        return values

    @property
    def time_column(self) -> t.Optional[TimeColumn]:
        """The time column for incremental models."""
        return getattr(self.kind, "time_column", None)

    @property
    def unique_key(self) -> t.List[exp.Expression]:
        if isinstance(
            self.kind, (SCDType2ByTimeKind, SCDType2ByColumnKind, IncrementalByUniqueKeyKind)
        ):
            return self.kind.unique_key
        return []

    @property
    def partitioned_by(self) -> t.List[exp.Expression]:
        if self.time_column and self.time_column.column not in [
            col.name for col in self._partition_by_columns
        ]:
            return [*[exp.to_column(self.time_column.column)], *self.partitioned_by_]
        return self.partitioned_by_

    @property
    def column_descriptions(self) -> t.Dict[str, str]:
        """A dictionary of column names to annotation comments."""
        return self.column_descriptions_ or {}

    @property
    def lookback(self) -> int:
        """The incremental lookback window."""
        return (self.kind.lookback if isinstance(self.kind, _Incremental) else 0) or 0

    def lookback_start(self, start: TimeLike) -> TimeLike:
        if self.lookback == 0:
            return start

        for _ in range(self.lookback):
            start = self.interval_unit.cron_prev(start)
        return start

    @property
    def batch_size(self) -> t.Optional[int]:
        """The maximal number of units in a single task for a backfill."""
        return getattr(self.kind, "batch_size", None)

    @cached_property
    def table_properties(self) -> t.Dict[str, exp.Expression]:
        """A dictionary of table properties."""
        if self.table_properties_:
            table_properties = {}
            for expression in self.table_properties_.expressions:
                table_properties[expression.this.name] = expression.expression
            return table_properties
        return {}

    @property
    def session_properties(self) -> SessionProperties:
        """A dictionary of session properties."""
        if not self.session_properties_:
            return {}

        def _interpret_expr(
            e: exp.Expression,
        ) -> t.Union[exp.Expression | str | int | float | bool]:
            if e.is_int:
                return int(e.this)
            if e.is_number:
                return float(e.this)
            if isinstance(e, (exp.Literal, exp.Boolean)):
                return e.this
            return e

        return {
            e.this.name: _interpret_expr(e.expression) for e in self.session_properties_.expressions
        }

    @property
    def all_references(self) -> t.List[Reference]:
        """All references including grains."""
        return [Reference(model_name=self.name, expression=e, unique=True) for e in self.grains] + [
            Reference(model_name=self.name, expression=e, unique=True) for e in self.references
        ]

    @property
    def _partition_by_columns(self) -> t.List[exp.Column]:
        return [col for expr in self.partitioned_by_ for col in expr.find_all(exp.Column)]

    @property
    def managed_columns(self) -> t.Dict[str, exp.DataType]:
        return getattr(self.kind, "managed_columns", {})

    @property
    def when_matched(self) -> t.Optional[exp.When]:
        if isinstance(self.kind, IncrementalByUniqueKeyKind):
            return self.kind.when_matched
        return None

    @property
    def catalog(self) -> t.Optional[str]:
        """Returns the catalog of a model."""
        return self.fully_qualified_table.catalog

    @cached_property
    def fully_qualified_table(self) -> exp.Table:
        return exp.to_table(self.fqn)

    @cached_property
    def fqn(self) -> str:
        return normalize_model_name(
            self.name, default_catalog=self.default_catalog, dialect=self.dialect
        )
