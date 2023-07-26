from __future__ import annotations

import typing as t

from pydantic import Field, root_validator, validator
from sqlglot import exp
from sqlglot.helper import ensure_list

from sqlmesh.core import dialect as d
from sqlmesh.core.model.kind import (
    IncrementalByUniqueKeyKind,
    ModelKind,
    TimeColumn,
    ViewKind,
    _Incremental,
)
from sqlmesh.core.node import Node, str_or_exp_to_str
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import ConfigError

AuditReference = t.Tuple[str, t.Dict[str, exp.Expression]]


class ModelMeta(Node):
    """Metadata for models which can be defined in SQL."""

    dialect: str = ""
    name: str
    kind: ModelKind = ViewKind()
    retention: t.Optional[int]  # not implemented yet
    storage_format: t.Optional[str]
    partitioned_by_: t.List[exp.Expression] = Field(default=[], alias="partitioned_by")
    clustered_by: t.List[str] = []
    depends_on_: t.Optional[t.Set[str]] = Field(default=None, alias="depends_on")
    columns_to_types_: t.Optional[t.Dict[str, exp.DataType]] = Field(default=None, alias="columns")
    column_descriptions_: t.Optional[t.Dict[str, str]]
    audits: t.List[AuditReference] = []
    tags: t.List[str] = []
    grain: t.List[str] = []
    hash_raw_query: bool = False
    physical_schema_override: t.Optional[str] = None
    table_properties_: t.Optional[t.Dict[str, exp.Expression]] = Field(
        default=None, alias="table_properties"
    )

    _model_kind_validator = ModelKind.field_validator()

    class Config(Node.Config):
        extra = "allow"

    @validator("audits", pre=True)
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

    @validator("clustered_by", "tags", "grain", pre=True)
    def _value_or_tuple_validator(cls, v: t.Any) -> t.Any:
        if isinstance(v, (exp.Tuple, exp.Array)):
            return [e.name for e in v.expressions]
        if isinstance(v, exp.Expression):
            return [v.name]
        return v

    @validator("dialect", "storage_format", pre=True)
    def _string_validator(cls, v: t.Any) -> t.Optional[str]:
        return str_or_exp_to_str(v)

    @validator("partitioned_by_", pre=True)
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
            exp.to_column(expr.name) if isinstance(expr, exp.Identifier) else expr
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

    @validator("columns_to_types_", pre=True)
    def _columns_validator(
        cls, v: t.Any, values: t.Dict[str, t.Any]
    ) -> t.Optional[t.Dict[str, exp.DataType]]:
        dialect = values.get("dialect")
        columns_to_types = {}
        if isinstance(v, exp.Schema):
            for column in v.expressions:
                expr = column.args["kind"]
                expr.meta["dialect"] = dialect
                columns_to_types[column.name] = expr

            return columns_to_types

        if isinstance(v, dict):
            for k, data_type in v.items():
                expr = exp.DataType.build(data_type, dialect=dialect)
                expr.meta["dialect"] = dialect
                columns_to_types[k] = expr

            return columns_to_types

        return v

    @validator("table_properties_", pre=True)
    def _properties_validator(
        cls, v: t.Any, values: t.Dict[str, t.Any]
    ) -> t.Optional[t.Dict[str, exp.Expression]]:
        if v is None:
            return v

        dialect = values.get("dialect")

        table_properties = {}
        if isinstance(v, exp.Expression):
            if isinstance(v, (exp.Tuple, exp.Array)):
                eq_expressions: t.List[exp.Expression] = v.expressions
            elif isinstance(v, exp.Paren):
                eq_expressions = [v.unnest()]
            else:
                eq_expressions = [v]

            for eq_expr in eq_expressions:
                if not isinstance(eq_expr, exp.EQ):
                    raise ConfigError(
                        f"Invalid table property '{eq_expr.sql(dialect=dialect)}'. "
                        "Table properties must be specified as key-value pairs <key> = <value>. "
                    )

                value_expr = eq_expr.expression.copy()
                value_expr.meta["dialect"] = dialect
                table_properties[eq_expr.this.name] = value_expr
        elif isinstance(v, dict):
            for key, value in v.items():
                value_expr = exp.convert(value, copy=True)
                value_expr.meta["dialect"] = dialect
                table_properties[key] = value_expr
        else:
            raise ConfigError(f"Unexpected table properties '{v}'")

        return table_properties

    @validator("depends_on_", pre=True)
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

    @root_validator
    def _root_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        values = cls._kind_validator(values)
        values = cls._normalize_name(values)
        values = cls._set_physical_schema_override(values)
        return values

    @classmethod
    def _kind_validator(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        kind = values.get("kind")
        if kind:
            for field in ("partitioned_by_", "clustered_by"):
                if values.get(field) and not kind.is_materialized:
                    raise ValueError(f"{field} field cannot be set for {kind} models")

        return values

    @classmethod
    def _normalize_name(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        values["name"] = d.normalize_model_name(values["name"], dialect=values.get("dialect"))
        return values

    @classmethod
    def _set_physical_schema_override(cls, values: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        physical_schema_override_map = values.get("physical_schema_override_map") or {}
        values["physical_schema_override"] = physical_schema_override_map.get(
            exp.to_table(values["name"]).db, values.get("physical_schema_override")
        )
        return values

    @property
    def time_column(self) -> t.Optional[TimeColumn]:
        """The time column for incremental models."""
        return getattr(self.kind, "time_column", None)

    @property
    def unique_key(self) -> t.List[str]:
        if isinstance(self.kind, IncrementalByUniqueKeyKind):
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
        return self.table_properties_ or {}

    @property
    def _partition_by_columns(self) -> t.List[exp.Column]:
        return [col for expr in self.partitioned_by_ for col in expr.find_all(exp.Column)]
