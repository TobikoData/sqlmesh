from __future__ import annotations

import logging
import typing as t
from functools import cached_property

from pydantic import Field
from sqlglot import Dialect, exp
from sqlglot.helper import ensure_collection, ensure_list
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.core import dialect as d
from sqlmesh.core.dialect import normalize_model_name, extract_audit
from sqlmesh.core.model.common import (
    bool_validator,
    default_catalog_validator,
    depends_on_validator,
    parse_properties,
    properties_validator,
)
from sqlmesh.core.model.kind import (
    CustomKind,
    IncrementalByUniqueKeyKind,
    ModelKind,
    OnDestructiveChange,
    SCDType2ByColumnKind,
    SCDType2ByTimeKind,
    TimeColumn,
    ViewKind,
    _IncrementalBy,
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
    from sqlmesh.core._typing import CustomMaterializationProperties, SessionProperties

AuditReference = t.Tuple[str, t.Dict[str, exp.Expression]]

logger = logging.getLogger(__name__)


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
    column_descriptions_: t.Optional[t.Dict[str, str]] = Field(
        default=None, alias="column_descriptions"
    )
    audits: t.List[AuditReference] = []
    grains: t.List[exp.Expression] = []
    references: t.List[exp.Expression] = []
    physical_schema_override: t.Optional[str] = None
    physical_properties_: t.Optional[exp.Tuple] = Field(default=None, alias="physical_properties")
    virtual_properties_: t.Optional[exp.Tuple] = Field(default=None, alias="virtual_properties")
    session_properties_: t.Optional[exp.Tuple] = Field(default=None, alias="session_properties")
    allow_partials: bool = False
    signals: t.List[exp.Tuple] = []
    enabled: bool = True

    _bool_validator = bool_validator
    _model_kind_validator = model_kind_validator
    _properties_validator = properties_validator
    _default_catalog_validator = default_catalog_validator
    _depends_on_validator = depends_on_validator

    @field_validator("audits", mode="before")
    def _audits_validator(cls, v: t.Any) -> t.Any:
        if isinstance(v, (exp.Tuple, exp.Array)):
            return [extract_audit(i) for i in v.expressions]
        if isinstance(v, exp.Paren):
            return [extract_audit(v.this)]
        if isinstance(v, exp.Expression):
            return [extract_audit(v)]
        if isinstance(v, list):
            audits = []

            for entry in v:
                if isinstance(entry, dict):
                    args = entry
                    name = entry.pop("name")
                elif isinstance(entry, (tuple, list)):
                    name, args = entry
                else:
                    raise ConfigError(f"Audit must be a dictionary or named tuple. Got {entry}.")

                audits.append(
                    (
                        name.lower(),
                        {
                            key: d.parse_one(value) if isinstance(value, str) else value
                            for key, value in args.items()
                        },
                    )
                )

            return audits

        return v

    @field_validator("tags", mode="before")
    @field_validator_v1_args
    def _value_or_tuple_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> t.Any:
        return ensure_list(cls._validate_value_or_tuple(v, values))

    @field_validator("clustered_by", mode="before")
    @field_validator_v1_args
    def _normalized_value_or_tuple_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> t.Any:
        return ensure_list(cls._validate_value_or_tuple(v, values, normalize=True))

    @classmethod
    def _validate_value_or_tuple(
        cls, v: t.Dict[str, t.Any], values: t.Dict[str, t.Any], normalize: bool = False
    ) -> t.Any:
        dialect = values.get("dialect")

        def _normalize(value: t.Any) -> t.Any:
            return normalize_identifiers(value, dialect=dialect) if normalize else value

        if isinstance(v, exp.Paren):
            v = [v.unnest()]

        if isinstance(v, (exp.Tuple, exp.Array)):
            return [_normalize(e).name for e in v.expressions]
        if isinstance(v, exp.Expression):
            return _normalize(v).name
        if isinstance(v, str):
            value = _normalize(v)
            return value.name if isinstance(value, exp.Expression) else value
        if isinstance(v, (list, tuple)):
            return [cls._validate_value_or_tuple(elm, values, normalize=normalize) for elm in v]

        return v

    @field_validator("storage_format", mode="before")
    @field_validator_v1_args
    def _storage_format_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> t.Optional[str]:
        if isinstance(v, exp.Expression) and not (isinstance(v, (exp.Literal, exp.Identifier))):
            return v.sql(values.get("dialect"))
        return str_or_exp_to_str(v)

    @field_validator("dialect", mode="before")
    def _dialect_validator(cls, v: t.Any) -> t.Optional[str]:
        # dialects are parsed as identifiers and may get normalized as uppercase,
        # so this ensures they'll be stored as lowercase
        dialect = str_or_exp_to_str(v)
        return dialect and dialect.lower()

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

    @field_validator(
        "columns_to_types_", "derived_columns_to_types", mode="before", check_fields=False
    )
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

    @field_validator("column_descriptions_", mode="before")
    @field_validator_v1_args
    def _column_descriptions_validator(
        cls, vs: t.Any, values: t.Dict[str, t.Any]
    ) -> t.Optional[t.Dict[str, str]]:
        dialect = values.get("dialect")

        if vs is None:
            return None

        if isinstance(vs, exp.Paren):
            vs = vs.flatten()

        if isinstance(vs, (exp.Tuple, exp.Array)):
            vs = vs.expressions

        raw_col_descriptions = (
            vs if isinstance(vs, dict) else {v.this.name: v.expression.name for v in vs}
        )

        col_descriptions = {
            normalize_identifiers(k, dialect=dialect).name: v
            for k, v in raw_col_descriptions.items()
        }

        columns_to_types = values.get("columns_to_types_")
        if columns_to_types:
            for column_name in col_descriptions:
                if column_name not in columns_to_types:
                    raise ConfigError(
                        f"In model '{values['name']}', a description is provided for column '{column_name}' but it is not a column in the model."
                    )

        return col_descriptions

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

        table_properties = values.pop("table_properties", None)
        if table_properties:
            if not isinstance(table_properties, str):
                # Do not warn when deserializing from the state.
                model_name = values["name"]
                logger.warning(
                    f"Model '{model_name}' is using the `table_properties` attribute which is deprecated. Please use `physical_properties` instead."
                )
            physical_properties = values.get("physical_properties")
            if physical_properties:
                raise ConfigError(
                    f"Cannot use argument 'table_properties' ({table_properties}) with 'physical_properties' ({physical_properties}), use only physical_properties."
                )
            values["physical_properties"] = table_properties
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
                if (
                    values.get(field)
                    and not kind.is_materialized
                    and not (kind.is_view and kind.materialized)
                ):
                    raise ValueError(f"{field} field cannot be set for {kind} models")
            if kind.is_incremental_by_partition and not values.get("partitioned_by_"):
                raise ValueError(f"partitioned_by field is required for {kind.name} models")
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
        """Columns to partition the model by, including the time column if it is not already included."""
        if (
            self.time_column
            and self.time_column.column not in [col for col in self._partition_by_columns]
            and self.dialect not in NO_PARTITIONED_TIME_COLUMN_DIALECTS
        ):
            return [self.time_column.column, *self.partitioned_by_]
        return self.partitioned_by_

    @property
    def column_descriptions(self) -> t.Dict[str, str]:
        """A dictionary of column names to annotation comments."""
        return self.column_descriptions_ or {}

    @property
    def lookback(self) -> int:
        """The incremental lookback window."""
        return (self.kind.lookback if isinstance(self.kind, _IncrementalBy) else 0) or 0

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
    def batch_concurrency(self) -> t.Optional[int]:
        """The maximal number of batches that can run concurrently for a backfill."""
        return getattr(self.kind, "batch_concurrency", None)

    @cached_property
    def physical_properties(self) -> t.Dict[str, exp.Expression]:
        """A dictionary of properties that will be applied to the physical layer. It replaces table_properties which is deprecated."""
        if self.physical_properties_:
            return {e.this.name: e.expression for e in self.physical_properties_.expressions}
        return {}

    @cached_property
    def virtual_properties(self) -> t.Dict[str, exp.Expression]:
        """A dictionary of properties that will be applied to the virtual layer."""
        if self.virtual_properties_:
            return {e.this.name: e.expression for e in self.virtual_properties_.expressions}
        return {}

    @property
    def session_properties(self) -> SessionProperties:
        """A dictionary of session properties."""
        if not self.session_properties_:
            return {}

        return d.interpret_key_value_pairs(self.session_properties_)

    @property
    def custom_materialization_properties(self) -> CustomMaterializationProperties:
        if isinstance(self.kind, CustomKind):
            return self.kind.materialization_properties
        return {}

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
    def when_matched(self) -> t.Optional[t.List[exp.When]]:
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

    @property
    def on_destructive_change(self) -> OnDestructiveChange:
        return getattr(self.kind, "on_destructive_change", OnDestructiveChange.ALLOW)


# dialects for which time_column should not automatically be added to partitioned_by
NO_PARTITIONED_TIME_COLUMN_DIALECTS = {"clickhouse"}
