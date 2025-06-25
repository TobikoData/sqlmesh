from __future__ import annotations

import typing as t
from functools import cached_property
from typing_extensions import Self

from pydantic import Field
from sqlglot import Dialect, exp, parse_one
from sqlglot.helper import ensure_collection, ensure_list
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.core import dialect as d
from sqlmesh.core.config.linter import LinterConfig
from sqlmesh.core.dialect import normalize_model_name, extract_func_call
from sqlmesh.core.model.common import (
    bool_validator,
    default_catalog_validator,
    depends_on_validator,
    properties_validator,
    parse_properties,
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
    ValidationInfo,
    field_validator,
    list_of_fields_validator,
    model_validator,
    get_dialect,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import CustomMaterializationProperties, SessionProperties

FunctionCall = t.Tuple[str, t.Dict[str, exp.Expression]]


class ModelMeta(_Node):
    """Metadata for models which can be defined in SQL."""

    dialect: str = ""
    name: str
    kind: ModelKind = ViewKind()
    retention: t.Optional[int] = None  # not implemented yet
    table_format: t.Optional[str] = None
    storage_format: t.Optional[str] = None
    partitioned_by_: t.List[exp.Expression] = Field(default=[], alias="partitioned_by")
    clustered_by: t.List[exp.Expression] = []
    default_catalog: t.Optional[str] = None
    depends_on_: t.Optional[t.Set[str]] = Field(default=None, alias="depends_on")
    columns_to_types_: t.Optional[t.Dict[str, exp.DataType]] = Field(default=None, alias="columns")
    column_descriptions_: t.Optional[t.Dict[str, str]] = Field(
        default=None, alias="column_descriptions"
    )
    audits: t.List[FunctionCall] = []
    grains: t.List[exp.Expression] = []
    references: t.List[exp.Expression] = []
    physical_schema_override: t.Optional[str] = None
    physical_properties_: t.Optional[exp.Tuple] = Field(default=None, alias="physical_properties")
    virtual_properties_: t.Optional[exp.Tuple] = Field(default=None, alias="virtual_properties")
    session_properties_: t.Optional[exp.Tuple] = Field(default=None, alias="session_properties")
    allow_partials: bool = False
    signals: t.List[FunctionCall] = []
    enabled: bool = True
    physical_version: t.Optional[str] = None
    gateway: t.Optional[str] = None
    optimize_query: t.Optional[bool] = None
    ignored_rules_: t.Optional[t.Set[str]] = Field(
        default=None, exclude=True, alias="ignored_rules"
    )
    formatting: t.Optional[bool] = Field(default=None, exclude=True)

    _bool_validator = bool_validator
    _model_kind_validator = model_kind_validator
    _properties_validator = properties_validator
    _default_catalog_validator = default_catalog_validator
    _depends_on_validator = depends_on_validator

    @field_validator("audits", "signals", mode="before")
    def _func_call_validator(cls, v: t.Any, field: t.Any) -> t.Any:
        is_signal = getattr(field, "name" if hasattr(field, "name") else "field_name") == "signals"

        if isinstance(v, (exp.Tuple, exp.Array)):
            return [extract_func_call(i, allow_tuples=is_signal) for i in v.expressions]
        if isinstance(v, exp.Paren):
            return [extract_func_call(v.this, allow_tuples=is_signal)]
        if isinstance(v, exp.Expression):
            return [extract_func_call(v, allow_tuples=is_signal)]
        if isinstance(v, list):
            audits = []

            for entry in v:
                if isinstance(entry, dict):
                    args = entry
                    name = "" if is_signal else entry.pop("name")
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

        return v or []

    @field_validator("tags", mode="before")
    def _value_or_tuple_validator(cls, v: t.Any, info: ValidationInfo) -> t.Any:
        return ensure_list(cls._validate_value_or_tuple(v, info.data))

    @classmethod
    def _validate_value_or_tuple(
        cls, v: t.Dict[str, t.Any], data: t.Dict[str, t.Any], normalize: bool = False
    ) -> t.Any:
        dialect = data.get("dialect")

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
            return [cls._validate_value_or_tuple(elm, data, normalize=normalize) for elm in v]

        return v

    @field_validator("table_format", "storage_format", mode="before")
    def _format_validator(cls, v: t.Any, info: ValidationInfo) -> t.Optional[str]:
        if isinstance(v, exp.Expression) and not (isinstance(v, (exp.Literal, exp.Identifier))):
            return v.sql(info.data.get("dialect"))
        return str_or_exp_to_str(v)

    @field_validator("dialect", mode="before")
    def _dialect_validator(cls, v: t.Any) -> t.Optional[str]:
        # dialects are parsed as identifiers and may get normalized as uppercase,
        # so this ensures they'll be stored as lowercase
        dialect = str_or_exp_to_str(v)
        return dialect and dialect.lower()

    @field_validator("physical_version", mode="before")
    def _physical_version_validator(cls, v: t.Any) -> t.Optional[str]:
        if v is None:
            return v
        return str_or_exp_to_str(v)

    @field_validator("gateway", mode="before")
    def _gateway_validator(cls, v: t.Any) -> t.Optional[str]:
        if v is None:
            return None
        gateway = str_or_exp_to_str(v)
        return gateway and gateway.lower()

    @field_validator("partitioned_by_", "clustered_by", mode="before")
    def _partition_and_cluster_validator(
        cls, v: t.Any, info: ValidationInfo
    ) -> t.List[exp.Expression]:
        if (
            isinstance(v, list)
            and all(isinstance(i, str) for i in v)
            and info.field_name == "partitioned_by_"
        ):
            # this branch gets hit when we are deserializing from json because `partitioned_by` is stored as a List[str]
            # however, we should only invoke this if the list contains strings because this validator is also
            # called by Python models which might pass a List[exp.Expression]
            string_to_parse = (
                f"({','.join(v)})"  # recreate the (a, b, c) part of "partitioned_by (a, b, c)"
            )
            parsed = parse_one(
                string_to_parse, into=exp.PartitionedByProperty, dialect=get_dialect(info)
            )
            v = parsed.this.expressions if isinstance(parsed.this, exp.Schema) else v

        expressions = list_of_fields_validator(v, info.data)

        for expression in expressions:
            num_cols = len(list(expression.find_all(exp.Column)))

            error_msg: t.Optional[str] = None
            if num_cols == 0:
                error_msg = "does not contain a column"
            elif num_cols > 1:
                error_msg = "contains multiple columns"

            if error_msg:
                raise ConfigError(f"Field '{expression}' {error_msg}")

        return expressions

    @field_validator(
        "columns_to_types_", "derived_columns_to_types", mode="before", check_fields=False
    )
    def _columns_validator(
        cls, v: t.Any, info: ValidationInfo
    ) -> t.Optional[t.Dict[str, exp.DataType]]:
        columns_to_types = {}
        dialect = info.data.get("dialect")

        if isinstance(v, exp.Schema):
            for column in v.expressions:
                expr = column.args.get("kind")
                if not isinstance(expr, exp.DataType):
                    raise ConfigError(f"Missing data type for column '{column.name}'.")

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
    def _column_descriptions_validator(
        cls, vs: t.Any, info: ValidationInfo
    ) -> t.Optional[t.Dict[str, str]]:
        dialect = info.data.get("dialect")

        if vs is None:
            return None

        if isinstance(vs, exp.Paren):
            vs = vs.flatten()

        if isinstance(vs, (exp.Tuple, exp.Array)):
            vs = vs.expressions

        raw_col_descriptions = (
            vs
            if isinstance(vs, dict)
            else {".".join([part.this for part in v.this.parts]): v.expression.name for v in vs}
        )

        col_descriptions = {
            normalize_identifiers(k, dialect=dialect).name: v
            for k, v in raw_col_descriptions.items()
        }

        columns_to_types = info.data.get("columns_to_types_")
        if columns_to_types:
            for column_name in col_descriptions:
                if column_name not in columns_to_types:
                    raise ConfigError(
                        f"In model '{info.data['name']}', a description is provided for column '{column_name}' but it is not a column in the model."
                    )

        return col_descriptions

    @field_validator("grains", "references", mode="before")
    def _refs_validator(cls, vs: t.Any, info: ValidationInfo) -> t.List[exp.Expression]:
        dialect = info.data.get("dialect")

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

    @field_validator("ignored_rules_", mode="before")
    def ignored_rules_validator(cls, vs: t.Any) -> t.Any:
        return LinterConfig._validate_rules(vs)

    @field_validator("session_properties_", mode="before")
    def session_properties_validator(cls, v: t.Any, info: ValidationInfo) -> t.Any:
        # use the generic properties validator to parse the session properties
        parsed_session_properties = parse_properties(type(cls), v, info)
        if not parsed_session_properties:
            return parsed_session_properties

        for eq in parsed_session_properties:
            prop_name = eq.left.name

            if prop_name == "query_label":
                query_label = eq.right
                if not isinstance(
                    query_label, (exp.Array, exp.Tuple, exp.Paren, d.MacroFunc, d.MacroVar)
                ):
                    raise ConfigError(
                        "Invalid value for `session_properties.query_label`. Must be an array or tuple."
                    )

                label_tuples: t.List[exp.Expression] = (
                    [query_label.unnest()]
                    if isinstance(query_label, exp.Paren)
                    else query_label.expressions
                )

                for label_tuple in label_tuples:
                    if not (
                        isinstance(label_tuple, exp.Tuple)
                        and len(label_tuple.expressions) == 2
                        and all(isinstance(label, exp.Literal) for label in label_tuple.expressions)
                    ):
                        raise ConfigError(
                            "Invalid entry in `session_properties.query_label`. Must be tuples of string literals with length 2."
                        )
            elif prop_name == "authorization":
                authorization = eq.right
                if not (
                    isinstance(authorization, exp.Literal) and authorization.is_string
                ) and not isinstance(authorization, (d.MacroFunc, d.MacroVar)):
                    raise ConfigError(
                        "Invalid value for `session_properties.authorization`. Must be a string literal."
                    )

        return parsed_session_properties

    @model_validator(mode="before")
    def _pre_root_validator(cls, data: t.Any) -> t.Any:
        if not isinstance(data, dict):
            return data

        grain = data.pop("grain", None)
        if grain:
            grains = data.get("grains")
            if grains:
                raise ConfigError(
                    f"Cannot use argument 'grain' ({grain}) with 'grains' ({grains}), use only grains"
                )
            data["grains"] = ensure_list(grain)

        table_properties = data.pop("table_properties", None)
        if table_properties:
            if not isinstance(table_properties, str):
                # Do not warn when deserializing from the state.
                model_name = data["name"]
                from sqlmesh.core.console import get_console

                get_console().log_warning(
                    f"Model '{model_name}' is using the `table_properties` attribute which is deprecated. Please use `physical_properties` instead."
                )
            physical_properties = data.get("physical_properties")
            if physical_properties:
                raise ConfigError(
                    f"Cannot use argument 'table_properties' ({table_properties}) with 'physical_properties' ({physical_properties}), use only physical_properties."
                )

            data["physical_properties"] = table_properties

        return data

    @model_validator(mode="after")
    def _root_validator(self) -> Self:
        kind: t.Any = self.kind

        for field in ("partitioned_by_", "clustered_by"):
            if (
                getattr(self, field, None)
                and not kind.is_materialized
                and not (kind.is_view and kind.materialized)
            ):
                name = field[:-1] if field.endswith("_") else field
                raise ValueError(f"{name} field cannot be set for {kind.name} models")
        if kind.is_incremental_by_partition and not getattr(self, "partitioned_by_", None):
            raise ValueError(f"partitioned_by field is required for {kind.name} models")

        # needs to be in a mode=after model validator so that the field validators have run to convert from Expression -> str
        if (storage_format := self.storage_format) and storage_format.lower() in {
            "iceberg",
            "hive",
            "hudi",
            "delta",
        }:
            from sqlmesh.core.console import get_console

            get_console().log_warning(
                f"Model {self.name} has `storage_format` set to a table format '{storage_format}' which is deprecated. Please use the `table_format` property instead."
            )

        return self

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
    def on(self) -> t.List[str]:
        """The grains to be used as join condition in table_diff."""

        on: t.List[str] = []
        for expr in [ref.expression for ref in self.all_references if ref.unique]:
            if isinstance(expr, exp.Tuple):
                on.extend([key.this.sql(dialect=self.dialect) for key in expr.expressions])
            else:
                # Handle a single Column or Paren expression
                on.append(expr.this.sql(dialect=self.dialect))

        return on

    @property
    def managed_columns(self) -> t.Dict[str, exp.DataType]:
        return getattr(self.kind, "managed_columns", {})

    @property
    def when_matched(self) -> t.Optional[exp.Whens]:
        if isinstance(self.kind, IncrementalByUniqueKeyKind):
            return self.kind.when_matched
        return None

    @property
    def merge_filter(self) -> t.Optional[exp.Expression]:
        if isinstance(self.kind, IncrementalByUniqueKeyKind):
            return self.kind.merge_filter
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

    @property
    def ignored_rules(self) -> t.Set[str]:
        return self.ignored_rules_ or set()
