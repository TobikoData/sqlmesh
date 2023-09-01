from __future__ import annotations

import typing as t

from pydantic import Field
from sqlglot import exp
from sqlglot.helper import ensure_collection, ensure_list
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.core import dialect as d
from sqlmesh.core.model.kind import (
    IncrementalByUniqueKeyKind,
    ModelKind,
    SCDType2Kind,
    TimeColumn,
    ViewKind,
    _Incremental,
    model_kind_validator,
)
from sqlmesh.core.node import _Node, str_or_exp_to_str
from sqlmesh.core.reference import Reference
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import ConfigError, SQLMeshError
from sqlmesh.utils.pydantic import (
    field_validator,
    field_validator_v1_args,
    model_validator,
    model_validator_v1_args,
)

AuditReference = t.Tuple[str, t.Dict[str, exp.Expression]]


class ModelMeta(_Node, extra="allow"):
    """Metadata for models which can be defined in SQL."""

    dialect: str = ""
    name: str
    kind: ModelKind = ViewKind()
    retention: t.Optional[int] = None  # not implemented yet
    storage_format: t.Optional[str] = None
    partitioned_by_: t.List[exp.Expression] = Field(default=[], alias="partitioned_by")
    clustered_by: t.List[str] = []
    depends_on_: t.Optional[t.Set[str]] = Field(default=None, alias="depends_on")
    columns_to_types_: t.Optional[t.Dict[str, exp.DataType]] = Field(default=None, alias="columns")
    column_descriptions_: t.Optional[t.Dict[str, str]] = None
    audits: t.List[AuditReference] = []
    grains: t.List[exp.Expression] = []
    references: t.List[exp.Expression] = []
    hash_raw_query: bool = False
    physical_schema_override: t.Optional[str] = None
    table_properties_: t.Optional[exp.Tuple] = Field(default=None, alias="table_properties")
    _table_properties: t.Dict[str, exp.Expression] = {}

    _model_kind_validator = model_kind_validator

    @field_validator("audits", mode="before")
    @classmethod
    def _audits_validator(cls, v: t.Any) -> t.Any:
        def extract(v: exp.Expression) -> t.Tuple[str, t.Dict[str, str]]:
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
            return (func.lower(), kwargs)

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
    @classmethod
    def _string_validator(cls, v: t.Any) -> t.Optional[str]:
        return str_or_exp_to_str(v)

    @field_validator("partitioned_by_", mode="before")
    @field_validator_v1_args
    def _partition_by_validator(
        cls, v: t.Any, values: t.Dict[str, t.Any]
    ) -> t.List[exp.Expression]:
        dialect = values.get("dialect")

        if isinstance(v, (exp.Tuple, exp.Array)):
            partitions: t.List[exp.Expression] = v.expressions
        elif isinstance(v, exp.Expression):
            partitions = [v]
        else:
            partitions = [
                d.parse_one(entry, dialect=dialect) if isinstance(entry, str) else entry
                for entry in ensure_list(v)
            ]

        partitions = [
            normalize_identifiers(
                exp.to_column(expr.name) if isinstance(expr, exp.Identifier) else expr
            )
            for expr in partitions
        ]

        for partition in partitions:
            partition.meta["dialect"] = dialect
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
            for k, data_type in v.items():
                expr = exp.DataType.build(data_type, dialect=dialect)
                expr.meta["dialect"] = dialect
                columns_to_types[normalize_identifiers(k, dialect=dialect).name] = expr

            return columns_to_types

        return v

    @field_validator("table_properties_", mode="before")
    @field_validator_v1_args
    def _properties_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> t.Optional[exp.Tuple]:
        if v is None:
            return v
        dialect = values.get("dialect")
        if isinstance(v, str):
            v = d.parse_one(v, dialect=dialect)
        if isinstance(v, (exp.Array, exp.Paren, exp.Tuple)):
            eq_expressions: t.List[exp.Expression] = (
                [v.unnest()] if isinstance(v, exp.Paren) else v.expressions
            )
            for eq_expr in eq_expressions:
                if not isinstance(eq_expr, exp.EQ):
                    raise ConfigError(
                        f"Invalid table property '{eq_expr.sql(dialect=dialect)}'. "
                        "Table properties must be specified as key-value pairs <key> = <value>. "
                    )
            properties = (
                exp.Tuple(expressions=eq_expressions)
                if isinstance(v, (exp.Paren, exp.Array))
                else v
            )
        elif isinstance(v, dict):
            properties = exp.Tuple(
                expressions=[exp.Literal.string(key).eq(value) for key, value in v.items()]
            )
        else:
            raise SQLMeshError(f"Unexpected table properties '{v}'")
        properties.meta["dialect"] = dialect
        return properties

    @field_validator("depends_on_", mode="before")
    @field_validator_v1_args
    def _depends_on_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> t.Optional[t.Set[str]]:
        dialect = values.get("dialect")

        if isinstance(v, (exp.Array, exp.Tuple)):
            return {
                d.normalize_model_name(
                    table.name if table.is_string else table.sql(dialect=dialect), dialect=dialect
                )
                for table in v.expressions
            }
        if isinstance(v, exp.Expression):
            return {d.normalize_model_name(v.sql(dialect=dialect), dialect=dialect)}
        if hasattr(v, "__iter__") and not isinstance(v, str):
            return {d.normalize_model_name(name, dialect=dialect) for name in v}

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

    @model_validator(mode="before")
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
        values = cls._normalize_name(values)
        return values

    @classmethod
    def _kind_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        kind = values.get("kind")
        if kind:
            for field in ("partitioned_by_", "clustered_by"):
                if values.get(field) and not kind.is_materialized:
                    raise ValueError(f"{field} field cannot be set for {kind} models")

            if hasattr(kind, "time_column"):
                kind.time_column.column = normalize_identifiers(
                    kind.time_column.column, dialect=values.get("dialect")
                ).name

        return values

    @classmethod
    def _normalize_name(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        values["name"] = d.normalize_model_name(values["name"], dialect=values.get("dialect"))
        return values

    @property
    def time_column(self) -> t.Optional[TimeColumn]:
        """The time column for incremental models."""
        return getattr(self.kind, "time_column", None)

    @property
    def unique_key(self) -> t.List[str]:
        if isinstance(self.kind, (IncrementalByUniqueKeyKind, SCDType2Kind)):
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

    @property
    def table_properties(self) -> t.Dict[str, exp.Expression]:
        """A dictionary of table properties."""
        if not self._table_properties and self.table_properties_:
            for expression in self.table_properties_.expressions:
                self._table_properties[expression.this.name] = expression.expression
        return self._table_properties

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
