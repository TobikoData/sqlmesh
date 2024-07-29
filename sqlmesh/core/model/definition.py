from __future__ import annotations

import ast
import json
import logging
import sys
import types
import typing as t
from functools import cached_property
from pathlib import Path

import pandas as pd
import numpy as np
from astor import to_source
from pydantic import Field
from sqlglot import diff, exp
from sqlglot.diff import Insert, Keep
from sqlglot.helper import ensure_list
from sqlglot.optimizer.simplify import gen
from sqlglot.schema import MappingSchema, nested_set
from sqlglot.time import format_time

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.macros import MacroRegistry, MacroStrTemplate, macro
from sqlmesh.core.model.common import expression_validator
from sqlmesh.core.model.kind import ModelKindName, SeedKind, ModelKind, FullKind, create_model_kind
from sqlmesh.core.model.meta import ModelMeta, AuditReference
from sqlmesh.core.model.seed import CsvSeedReader, Seed, create_seed
from sqlmesh.core.renderer import ExpressionRenderer, QueryRenderer
from sqlmesh.utils import columns_to_types_all_known, str_to_bool, UniqueKeyDict
from sqlmesh.utils.date import TimeLike, make_inclusive, to_datetime, to_time_column
from sqlmesh.utils.errors import ConfigError, SQLMeshError, raise_config_error
from sqlmesh.utils.hashing import hash_data
from sqlmesh.utils.jinja import (
    JinjaMacroRegistry,
    extract_macro_references_and_variables,
)
from sqlmesh.utils.pydantic import field_validator, field_validator_v1_args
from sqlmesh.utils.metaprogramming import (
    Executable,
    build_env,
    prepare_env,
    print_exception,
    serialize_env,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.audit import ModelAudit
    from sqlmesh.core.context import ExecutionContext
    from sqlmesh.core.engine_adapter import EngineAdapter
    from sqlmesh.core.engine_adapter._typing import QueryOrDF
    from sqlmesh.core.snapshot import DeployabilityIndex, Node, Snapshot
    from sqlmesh.utils.jinja import MacroReference

if sys.version_info >= (3, 9):
    from typing import Literal
else:
    from typing_extensions import Literal

logger = logging.getLogger(__name__)


class _Model(ModelMeta, frozen=True):
    """Model is the core abstraction for user defined datasets.

    A model consists of logic that fetches the data (a SQL query, a Python script or a seed) and metadata
    associated with it. Models can be run on arbitrary cadences and support incremental or full refreshes.
    Models can also be materialized into physical tables or shared across other models as temporary views.

    Example:
        MODEL (
            name           sushi.order_items,
            owner          jen,
            cron           '@daily',
            start          '2020-01-01',
            partitioned_by ds
        );

        @DEF(var, 'my_var');

        SELECT
          1 AS column_a # my first column,
          @var AS my_column # my second column,
        ;

    Args:
        name: The name of the model, which is of the form [catalog].[db].table.
            The catalog and db are optional.
        dialect: The SQL dialect that the model's query is written in. By default,
            this is assumed to be the dialect of the context.
        owner: The owner of the model.
        cron: A cron string specifying how often the model should be refreshed, leveraging the
            [croniter](https://github.com/kiorky/croniter) library.
        description: The optional model description.
        stamp: An optional arbitrary string sequence used to create new model versions without making
            changes to any of the functional components of the definition.
        start: The earliest date that the model will be backfilled for. If this is None,
            then the date is inferred by taking the most recent start date of its ancestors.
            The start date can be a static datetime or a relative datetime like "1 year ago"
        end: The date that the model will be backfilled up until. Follows the same syntax as 'start',
            should be omitted if there is no end date.
        lookback: The number of previous incremental intervals in the lookback window.
        storage_format: The storage format used to store the physical table, only applicable in certain engines.
            (eg. 'parquet')
        partitioned_by: The partition columns or engine specific expressions, only applicable in certain engines. (eg. (ds, hour))
        clustered_by: The cluster columns, only applicable in certain engines. (eg. (ds, hour))
        python_env: Dictionary containing all global variables needed to render the model's macros.
        mapping_schema: The schema of table names to column and types.
        physical_schema_override: The desired physical schema name override.
    """

    python_env_: t.Optional[t.Dict[str, Executable]] = Field(default=None, alias="python_env")
    jinja_macros: JinjaMacroRegistry = JinjaMacroRegistry()
    mapping_schema: t.Dict[str, t.Any] = {}

    _full_depends_on: t.Optional[t.Set[str]] = None

    _expressions_validator = expression_validator

    def render(
        self,
        *,
        context: ExecutionContext,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        **kwargs: t.Any,
    ) -> t.Iterator[QueryOrDF]:
        """Renders the content of this model in a form of either a SELECT query, executing which the data for this model can
        be fetched, or a dataframe object which contains the data itself.

        The type of the returned object (query or dataframe) depends on whether the model was sourced from a SQL query,
        a Python script or a pre-built dataset (seed).

        Args:
            context: The execution context used for fetching data.
            start: The start date/time of the run.
            end: The end date/time of the run.
            execution_time: The date/time time reference to use for execution time.

        Returns:
            A generator which yields either a query object or one of the supported dataframe objects.
        """
        yield self.render_query_or_raise(
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=context.snapshots,
            deployability_index=context.deployability_index,
            engine_adapter=context.engine_adapter,
            **kwargs,
        )

    def render_definition(
        self, include_python: bool = True, include_defaults: bool = False
    ) -> t.List[exp.Expression]:
        """Returns the original list of sql expressions comprising the model definition.

        Args:
            include_python: Whether or not to include Python code in the rendered definition.
        """
        expressions = []
        comment = None
        for field_name, field_info in ModelMeta.all_field_infos().items():
            field_value = getattr(self, field_name)

            if (include_defaults and field_value) or field_value != field_info.default:
                if field_name == "description":
                    comment = field_value
                elif field_name == "kind":
                    expressions.append(
                        exp.Property(
                            this="kind",
                            value=field_value.to_expression(dialect=self.dialect),
                        )
                    )
                elif field_name == "name":
                    expressions.append(
                        exp.Property(
                            this=field_name,
                            value=exp.to_table(field_value, dialect=self.dialect),
                        )
                    )
                elif field_name not in (
                    "column_descriptions_",
                    "default_catalog",
                    "enabled",
                    "inline_audits",
                ):
                    expressions.append(
                        exp.Property(
                            this=field_info.alias or field_name,
                            value=META_FIELD_CONVERTER.get(field_name, exp.to_identifier)(
                                field_value
                            ),
                        )
                    )

        model = d.Model(expressions=expressions)
        model.comments = [comment] if comment else None

        jinja_expressions = []
        python_expressions = []
        if include_python:
            python_env = d.PythonCode(
                expressions=[
                    v.payload if v.is_import or v.is_definition else f"{k} = {v.payload}"
                    for k, v in self.sorted_python_env
                ]
            )
            if python_env.expressions:
                python_expressions.append(python_env)

            jinja_expressions = self.jinja_macros.to_expressions()

        return [
            model,
            *python_expressions,
            *jinja_expressions,
        ]

    def render_query(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        expand: t.Iterable[str] = tuple(),
        deployability_index: t.Optional[DeployabilityIndex] = None,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Query]:
        """Renders a model's query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            execution_time: The date/time time reference to use for execution time.
            snapshots: All upstream snapshots (by name) to use for expansion and mapping of physical locations.
            table_mapping: Table mapping of physical locations. Takes precedence over snapshot mappings.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.
            deployability_index: Determines snapshots that are deployable in the context of this render.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        return exp.select(
            *(
                exp.cast(exp.Null(), column_type, copy=False).as_(name, copy=False, quoted=True)
                for name, column_type in (self.columns_to_types or {}).items()
            ),
            copy=False,
        ).from_(exp.values([tuple([1])], alias="t", columns=["dummy"]), copy=False)

    def render_query_or_raise(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        expand: t.Iterable[str] = tuple(),
        deployability_index: t.Optional[DeployabilityIndex] = None,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> exp.Query:
        """Same as `render_query()` but raises an exception if the query can't be rendered.

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            execution_time: The date/time time reference to use for execution time.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            table_mapping: Table mapping of physical locations. Takes precedence over snapshot mappings.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.
            deployability_index: Determines snapshots that are deployable in the context of this render.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        query = self.render_query(
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=snapshots,
            table_mapping=table_mapping,
            expand=expand,
            deployability_index=deployability_index,
            engine_adapter=engine_adapter,
            **kwargs,
        )
        if query is None:
            raise SQLMeshError(f"Failed to render query for model '{self.name}'.")
        return query

    def render_pre_statements(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Collection[Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        deployability_index: t.Optional[DeployabilityIndex] = None,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> t.List[exp.Expression]:
        """Renders pre-statements for a model.

        Pre-statements are statements that preceded the model's SELECT query.

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            execution_time: The date/time time reference to use for execution time.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.
            deployability_index: Determines snapshots that are deployable in the context of this render.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The list of rendered expressions.
        """
        return []

    def render_post_statements(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Collection[Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        deployability_index: t.Optional[DeployabilityIndex] = None,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> t.List[exp.Expression]:
        """Renders post-statements for a model.

        Post-statements are statements that follow after the model's SELECT query.

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            execution_time: The date/time time reference to use for execution time.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.
            deployability_index: Determines snapshots that are deployable in the context of this render.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The list of rendered expressions.
        """
        return []

    def render_signals(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
    ) -> t.List[t.Dict[str, str | int | float | bool]]:
        """Renders external; signals defined for this model.

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            execution_time: The date/time time reference to use for execution time.

        Returns:
            The list of rendered expressions.
        """

        def _create_renderer(expression: exp.Expression) -> ExpressionRenderer:
            return ExpressionRenderer(
                expression,
                self.dialect,
                [],
                path=self._path,
                jinja_macro_registry=self.jinja_macros,
                python_env=self.python_env,
                only_execution_time=False,
            )

        def _render(e: exp.Expression) -> str | int | float | bool:
            rendered_exprs = (
                _create_renderer(e).render(start=start, end=end, execution_time=execution_time)
                or []
            )
            if len(rendered_exprs) != 1:
                raise SQLMeshError(f"Expected one expression but got {len(rendered_exprs)}")

            rendered = rendered_exprs[0]
            if rendered.is_int:
                return int(rendered.this)
            if rendered.is_number:
                return float(rendered.this)
            if isinstance(rendered, (exp.Literal, exp.Boolean)):
                return rendered.this
            return rendered.sql(dialect=self.dialect)

        return [{t.this.name: _render(t.expression) for t in signal} for signal in self.signals]

    def ctas_query(self, **render_kwarg: t.Any) -> exp.Query:
        """Return a dummy query to do a CTAS.

        If a model's column types are unknown, the only way to create the table is to
        run the fully expanded query. This can be expensive so we add a WHERE FALSE to all
        SELECTS and hopefully the optimizer is smart enough to not do anything.

        Args:
            render_kwarg: Additional kwargs to pass to the renderer.
        Return:
            The mocked out ctas query.
        """
        query = self.render_query_or_raise(**render_kwarg).copy()

        for select_or_set_op in query.find_all(exp.Select, exp.SetOperation):
            skip_limit = False
            ancestor = select_or_set_op.parent
            while ancestor and not skip_limit:
                if isinstance(ancestor, exp.With) and ancestor.recursive:
                    skip_limit = True
                ancestor = ancestor.parent

            if isinstance(select_or_set_op, exp.Select) and select_or_set_op.args.get("from"):
                select_or_set_op.where(exp.false(), copy=False)
                if not skip_limit and not isinstance(select_or_set_op.parent, exp.SetOperation):
                    select_or_set_op.limit(0, copy=False)
            elif (
                not skip_limit
                and isinstance(select_or_set_op, exp.SetOperation)
                and not isinstance(select_or_set_op.parent, exp.SetOperation)
            ):
                select_or_set_op.set("limit", exp.Limit(expression=exp.Literal.number(0)))

        if self.managed_columns:
            query.select(
                *[
                    exp.alias_(exp.cast(exp.Null(), to=col_type), col)
                    for col, col_type in self.managed_columns.items()
                    if col not in query.named_selects
                ],
                append=True,
                copy=False,
            )
        return query

    def referenced_audits(
        self,
        audits: t.Dict[str, ModelAudit],
        default_audits: t.List[AuditReference] = [],
    ) -> t.List[ModelAudit]:
        """Returns audits referenced in this model.

        Args:
            audits: Available audits by name.
        """
        from sqlmesh.core.audit import BUILT_IN_AUDITS

        referenced_audits = []

        for audit_name, _ in self.audits + default_audits:
            if audit_name in self.inline_audits:
                referenced_audits.append(self.inline_audits[audit_name])
            elif audit_name in audits:
                referenced_audits.append(audits[audit_name])
            elif audit_name not in BUILT_IN_AUDITS:
                raise_config_error(
                    f"Unknown audit '{audit_name}' referenced in model '{self.name}'",
                    self._path,
                )
        return referenced_audits

    def text_diff(self, other: Node) -> str:
        """Produce a text diff against another node.

        Args:
            other: The node to diff against.

        Returns:
            A unified text diff showing additions and deletions.
        """
        if not isinstance(other, _Model):
            raise SQLMeshError(
                f"Cannot diff model '{self.name} against a non-model node '{other.name}'"
            )

        return d.text_diff(
            self.render_definition(), other.render_definition(), self.dialect, other.dialect
        ).strip()

    def set_time_format(self, default_time_format: str = c.DEFAULT_TIME_COLUMN_FORMAT) -> None:
        """Sets the default time format for a model.

        Args:
            default_time_format: A python time format used as the default format when none is provided.
        """
        if not self.time_column:
            return

        if self.time_column.format:
            # Transpile the time column format into the generic dialect
            formatted_time = format_time(
                self.time_column.format,
                d.Dialect.get_or_raise(self.dialect).TIME_MAPPING,
            )
            assert formatted_time is not None
            self.time_column.format = formatted_time
        else:
            self.time_column.format = default_time_format

    def convert_to_time_column(
        self, time: TimeLike, columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    ) -> exp.Expression:
        """Convert a TimeLike object to the same time format and type as the model's time column."""
        if self.time_column:
            if columns_to_types is None:
                columns_to_types = self.columns_to_types_or_raise

            if self.time_column.column.name not in columns_to_types:
                raise ConfigError(
                    f"Time column '{self.time_column.column.sql(dialect=self.dialect)}' not found in model '{self.name}'."
                )

            time_column_type = columns_to_types[self.time_column.column.name]

            return to_time_column(time, time_column_type, self.time_column.format)
        return exp.convert(time)

    def update_schema(
        self,
        schema: MappingSchema,
    ) -> None:
        """Updates the schema for this model's dependencies based on the given mapping schema."""
        for dep in self.depends_on:
            table = exp.to_table(dep)
            mapping_schema = schema.find(table)

            if mapping_schema:
                nested_set(
                    self.mapping_schema,
                    tuple(part.sql(copy=False) for part in table.parts),
                    {col: dtype.sql(dialect=self.dialect) for col, dtype in mapping_schema.items()},
                )

    @cached_property
    def depends_on(self) -> t.Set[str]:
        """All of the upstream dependencies referenced in the model's query, excluding self references.

        Returns:
            A list of all the upstream table names.
        """
        return self.full_depends_on - {self.fqn}

    @property
    def columns_to_types(self) -> t.Optional[t.Dict[str, exp.DataType]]:
        """Returns the mapping of column names to types of this model."""
        if self.columns_to_types_ is None:
            return None
        return {**self.columns_to_types_, **self.managed_columns}

    @property
    def columns_to_types_or_raise(self) -> t.Dict[str, exp.DataType]:
        """Returns the mapping of column names to types of this model or raise if not available."""
        columns_to_types = self.columns_to_types
        if columns_to_types is None:
            raise SQLMeshError(f"Column information is not available for model '{self.name}'")
        return columns_to_types

    @property
    def annotated(self) -> bool:
        """Checks if all column projection types of this model are known."""
        if self.columns_to_types is None:
            return False
        columns_to_types = {
            k: v for k, v in self.columns_to_types.items() if k not in self.managed_columns
        }
        if not columns_to_types:
            return False
        return columns_to_types_all_known(columns_to_types)

    @property
    def sorted_python_env(self) -> t.List[t.Tuple[str, Executable]]:
        """Returns the python env sorted by executable kind and then var name."""
        return sorted(self.python_env.items(), key=lambda x: (x[1].kind, x[0]))

    @property
    def view_name(self) -> str:
        return self.fully_qualified_table.name

    @property
    def python_env(self) -> t.Dict[str, Executable]:
        return self.python_env_ or {}

    @property
    def schema_name(self) -> str:
        return self.fully_qualified_table.db or c.DEFAULT_SCHEMA

    @property
    def physical_schema(self) -> str:
        return self.physical_schema_override or f"{c.SQLMESH}__{self.schema_name}"

    @property
    def is_sql(self) -> bool:
        return False

    @property
    def is_python(self) -> bool:
        return False

    @property
    def is_seed(self) -> bool:
        return False

    @cached_property
    def depends_on_self(self) -> bool:
        return self.fqn in self.full_depends_on

    @property
    def forward_only(self) -> bool:
        return getattr(self.kind, "forward_only", False)

    @property
    def disable_restatement(self) -> bool:
        return getattr(self.kind, "disable_restatement", False)

    @property
    def wap_supported(self) -> bool:
        return self.kind.is_materialized and (self.storage_format or "").lower() == "iceberg"

    @property
    def inline_audits(self) -> t.Dict[str, ModelAudit]:
        return {}

    def validate_definition(self) -> None:
        """Validates the model's definition.

        Raises:
            ConfigError
        """

        for field in ("partitioned_by", "clustered_by"):
            values = getattr(self, field)

            if values:
                values = [
                    col.name
                    for expr in values
                    for col in t.cast(
                        exp.Expression, exp.maybe_parse(expr, dialect=self.dialect)
                    ).find_all(exp.Column)
                ]

                unique_keys = set(values)

                if len(values) != len(unique_keys):
                    raise_config_error(
                        "All keys in '{field}' must be unique in the model definition",
                        self._path,
                    )

                columns_to_types = self.columns_to_types
                if columns_to_types is not None:
                    missing_keys = unique_keys - set(columns_to_types)
                    if missing_keys:
                        missing_keys_str = ", ".join(f"'{k}'" for k in sorted(missing_keys))
                        raise_config_error(
                            f"{field} keys [{missing_keys_str}] are missing in the model definition",
                            self._path,
                        )

        if self.kind.is_incremental_by_time_range and not self.time_column:
            raise_config_error(
                "Incremental by time range models must have a time_column field",
                self._path,
            )

        if (
            self.kind.is_incremental_unmanaged
            and getattr(self.kind, "insert_overwrite", False)
            and not self.partitioned_by_
        ):
            raise_config_error(
                "Unmanaged incremental models with insert / overwrite enabled must specify the partitioned_by field",
                self._path,
            )

        if self.kind.is_managed:
            # TODO: would this sort of logic be better off moved into the Kind?
            if self.dialect == "snowflake" and "target_lag" not in self.physical_properties:
                raise_config_error(
                    "Snowflake managed tables must specify the 'target_lag' physical property"
                )

    def is_breaking_change(self, previous: Model) -> t.Optional[bool]:
        """Determines whether this model is a breaking change in relation to the `previous` model.

        Args:
            previous: The previous model to compare against.

        Returns:
            True if this model instance represents a breaking change, False if it's a non-breaking change
            and None if the nature of the change can't be determined.
        """
        raise NotImplementedError

    @property
    def data_hash(self) -> str:
        """
        Computes the data hash for the node.

        Returns:
            The data hash for the node.
        """
        return hash_data(self._data_hash_values)

    @property
    def _data_hash_values(self) -> t.List[str]:
        data = [
            str(  # Exclude metadata only macro funcs
                [(k, v) for k, v in self.sorted_python_env if not v.is_metadata]
            ),
            *self.kind.data_hash_values,
            self.storage_format,
            str(self.lookback),
            *(gen(expr) for expr in (self.partitioned_by or [])),
            *(self.clustered_by or []),
            self.stamp,
            self.physical_schema,
            self.interval_unit.value if self.interval_unit is not None else None,
        ]

        for column_name, column_type in (self.columns_to_types_ or {}).items():
            data.append(column_name)
            data.append(column_type.sql())

        for key, value in (self.physical_properties or {}).items():
            data.append(key)
            data.append(gen(value))

        return data  # type: ignore

    def metadata_hash(self, audits: t.Dict[str, ModelAudit]) -> str:
        """
        Computes the metadata hash for the node.

        Args:
            audits: Available audits by name.

        Returns:
            The metadata hash for the node.
        """
        from sqlmesh.core.audit import BUILT_IN_AUDITS

        metadata = [
            self.dialect,
            self.owner,
            self.description,
            json.dumps(self.column_descriptions, sort_keys=True),
            self.cron,
            str(self.start) if self.start else None,
            str(self.end) if self.end else None,
            str(self.retention) if self.retention else None,
            str(self.batch_size) if self.batch_size is not None else None,
            str(self.batch_concurrency) if self.batch_concurrency is not None else None,
            json.dumps(self.mapping_schema, sort_keys=True),
            *sorted(self.tags),
            *sorted(ref.json(sort_keys=True) for ref in self.all_references),
            *self.kind.metadata_hash_values,
            self.project,
            str(self.allow_partials),
            gen(self.session_properties_) if self.session_properties_ else None,
        ]

        for audit_name, audit_args in sorted(self.audits, key=lambda a: a[0]):
            metadata.append(audit_name)
            audit = None
            if audit_name in BUILT_IN_AUDITS:
                for arg_name, arg_value in audit_args.items():
                    metadata.append(arg_name)
                    metadata.append(gen(arg_value))
            elif audit_name in self.inline_audits:
                audit = self.inline_audits[audit_name]
            elif audit_name in audits:
                audit = audits[audit_name]
            else:
                raise SQLMeshError(f"Unexpected audit name '{audit_name}'.")

            if audit:
                query = (
                    audit.render_query(self, **t.cast(t.Dict[str, t.Any], audit_args))
                    or audit.query
                )
                metadata.extend(
                    [
                        gen(query),
                        audit.dialect,
                        str(audit.skip),
                        str(audit.blocking),
                    ]
                )

        for key, value in (self.virtual_properties or {}).items():
            metadata.append(key)
            metadata.append(gen(value))

        metadata.extend(gen(s) for s in self.signals)
        metadata.extend(self._additional_metadata)

        return hash_data(metadata)

    @property
    def is_model(self) -> bool:
        """Return True if this is a model node"""
        return True

    @property
    def _additional_metadata(self) -> t.List[str]:
        additional_metadata = []

        metadata_only_macros = [(k, v) for k, v in self.sorted_python_env if v.is_metadata]
        if metadata_only_macros:
            additional_metadata.append(str(metadata_only_macros))

        return additional_metadata

    @property
    def full_depends_on(self) -> t.Set[str]:
        if not self._full_depends_on:
            depends_on = self.depends_on_ or set()

            query = self.render_query(optimize=False)
            if query is not None:
                depends_on |= d.find_tables(
                    query, default_catalog=self.default_catalog, dialect=self.dialect
                )
            self._full_depends_on = depends_on

        return self._full_depends_on


class _SqlBasedModel(_Model):
    pre_statements_: t.Optional[t.List[exp.Expression]] = Field(
        default=None, alias="pre_statements"
    )
    post_statements_: t.Optional[t.List[exp.Expression]] = Field(
        default=None, alias="post_statements"
    )
    inline_audits_: t.Dict[str, t.Any] = Field(default={}, alias="inline_audits")

    __statement_renderers: t.Dict[int, ExpressionRenderer] = {}

    _expression_validator = expression_validator

    @field_validator("inline_audits_", mode="before")
    @field_validator_v1_args
    def _inline_audits_validator(cls, v: t.Any, values: t.Dict[str, t.Any]) -> t.Any:
        if not isinstance(v, dict):
            return {}

        from sqlmesh.core.audit import ModelAudit

        inline_audits = {}

        for name, audit in v.items():
            if isinstance(audit, ModelAudit):
                inline_audits[name] = audit
            elif isinstance(audit, dict):
                inline_audits[name] = ModelAudit.parse_obj(audit)

        return inline_audits

    def render_pre_statements(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Collection[Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        deployability_index: t.Optional[DeployabilityIndex] = None,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> t.List[exp.Expression]:
        return self._render_statements(
            self.pre_statements,
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=snapshots,
            expand=expand,
            deployability_index=deployability_index,
            engine_adapter=engine_adapter,
            **kwargs,
        )

    def render_post_statements(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Collection[Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        deployability_index: t.Optional[DeployabilityIndex] = None,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> t.List[exp.Expression]:
        return self._render_statements(
            self.post_statements,
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=snapshots,
            expand=expand,
            deployability_index=deployability_index,
            engine_adapter=engine_adapter,
            **kwargs,
        )

    @property
    def pre_statements(self) -> t.List[exp.Expression]:
        return self.pre_statements_ or []

    @property
    def post_statements(self) -> t.List[exp.Expression]:
        return self.post_statements_ or []

    @property
    def macro_definitions(self) -> t.List[d.MacroDef]:
        """All macro definitions from the list of expressions."""
        return [s for s in self.pre_statements + self.post_statements if isinstance(s, d.MacroDef)]

    @property
    def inline_audits(self) -> t.Dict[str, ModelAudit]:
        return self.inline_audits_

    def _render_statements(
        self,
        statements: t.Iterable[exp.Expression],
        **kwargs: t.Any,
    ) -> t.List[exp.Expression]:
        rendered = (
            self._statement_renderer(statement).render(**kwargs)
            for statement in statements
            if not isinstance(statement, d.MacroDef)
        )
        return [r for expressions in rendered if expressions for r in expressions]

    def _statement_renderer(self, expression: exp.Expression) -> ExpressionRenderer:
        expression_key = id(expression)
        if expression_key not in self.__statement_renderers:
            self.__statement_renderers[expression_key] = ExpressionRenderer(
                expression,
                self.dialect,
                self.macro_definitions,
                path=self._path,
                jinja_macro_registry=self.jinja_macros,
                python_env=self.python_env,
                only_execution_time=self.kind.only_execution_time,
                default_catalog=self.default_catalog,
                model_fqn=self.fqn,
            )
        return self.__statement_renderers[expression_key]

    @property
    def _data_hash_values(self) -> t.List[str]:
        data_hash_values = super()._data_hash_values

        for statement in (*self.pre_statements, *self.post_statements):
            statement_exprs: t.List[exp.Expression] = []
            if not isinstance(statement, d.MacroDef):
                rendered = self._statement_renderer(statement).render()
                if self._is_metadata_statement(statement):
                    continue
                if rendered:
                    statement_exprs = rendered
                else:
                    statement_exprs = [statement]
            data_hash_values.extend(gen(e) for e in statement_exprs)

        return data_hash_values

    @property
    def _additional_metadata(self) -> t.List[str]:
        additional_metadata = super()._additional_metadata

        for statement in (*self.pre_statements, *self.post_statements):
            if self._is_metadata_statement(statement):
                additional_metadata.append(gen(statement))

        return additional_metadata

    def _is_metadata_statement(self, statement: exp.Expression) -> bool:
        if isinstance(statement, d.MacroDef):
            return True
        if isinstance(statement, d.MacroFunc):
            target_macro = macro.get_registry().get(statement.name)
            if target_macro:
                return target_macro.metadata_only
            target_macro = self.python_env.get(statement.name)
            if target_macro:
                return bool(target_macro.is_metadata)
        return False


class SqlModel(_SqlBasedModel):
    """The model definition which relies on a SQL query to fetch the data.

    Args:
        query: The main query representing the model.
        pre_statements: The list of SQL statements that precede the model's query.
        post_statements: The list of SQL statements that follow after the model's query.
    """

    query: t.Union[exp.Query, d.JinjaQuery, d.MacroFunc]
    source_type: Literal["sql"] = "sql"

    _columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None

    def render_query(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        expand: t.Iterable[str] = tuple(),
        deployability_index: t.Optional[DeployabilityIndex] = None,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Query]:
        query = self._query_renderer.render(
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=snapshots,
            table_mapping=table_mapping,
            expand=expand,
            deployability_index=deployability_index,
            engine_adapter=engine_adapter,
            **kwargs,
        )
        return query

    def render_definition(
        self, include_python: bool = True, include_defaults: bool = False
    ) -> t.List[exp.Expression]:
        result = super().render_definition(
            include_python=include_python, include_defaults=include_defaults
        )
        result.extend(self.pre_statements)
        result.append(self.query)
        result.extend(self.post_statements)
        return result

    @property
    def is_sql(self) -> bool:
        return True

    @property
    def columns_to_types(self) -> t.Optional[t.Dict[str, exp.DataType]]:
        if self.columns_to_types_ is not None:
            self._columns_to_types = self.columns_to_types_
        elif self._columns_to_types is None:
            query = self._query_renderer.render()

            if query is None:
                return None

            self._columns_to_types = {
                select.output_name: select.type or exp.DataType.build("unknown")
                for select in query.selects
            }

        if "*" in self._columns_to_types:
            return None

        return {**self._columns_to_types, **self.managed_columns}

    @cached_property
    def column_descriptions(self) -> t.Dict[str, str]:
        if self.column_descriptions_ is not None:
            return self.column_descriptions_

        query = self.render_query()
        if query is None:
            return {}

        return {
            select.alias_or_name: select.comments[-1].strip()
            for select in query.selects
            if select.comments
        }

    def update_schema(
        self,
        schema: MappingSchema,
    ) -> None:
        super().update_schema(schema)
        self._columns_to_types = None
        self._query_renderer.update_schema(self.mapping_schema)

    def validate_definition(self) -> None:
        query = self._query_renderer.render()
        if query is None:
            if self.depends_on_ is None:
                raise_config_error(
                    "Dependencies must be provided explicitly for models that can be rendered only at runtime",
                    self._path,
                )
            return

        if not isinstance(query, exp.Query):
            raise_config_error("Missing SELECT query in the model definition", self._path)

        projection_list = query.selects
        if not projection_list:
            raise_config_error("Query missing select statements", self._path)

        name_counts: t.Dict[str, int] = {}
        for expression in projection_list:
            alias = expression.output_name
            if alias == "*":
                continue
            if not alias:
                raise_config_error(
                    f"Outer projection '{expression}' must have inferrable names or explicit aliases.",
                    self._path,
                )
            name_counts[alias] = name_counts.get(alias, 0) + 1

        for name, count in name_counts.items():
            if count > 1:
                raise_config_error(f"Found duplicate outer select name '{name}'", self._path)

        if self.depends_on_self and not self.annotated:
            raise_config_error(
                "Self-referencing models require inferrable column types. There are three options available to mitigate this issue: add explicit types to all projections in the outermost SELECT statement, leverage external models (https://sqlmesh.readthedocs.io/en/stable/concepts/models/external_models/), or use the `columns` model attribute (https://sqlmesh.readthedocs.io/en/stable/concepts/models/overview/#columns).",
                self._path,
            )

        super().validate_definition()

    def is_breaking_change(self, previous: Model) -> t.Optional[bool]:
        if not isinstance(previous, SqlModel):
            return None

        if self.lookback != previous.lookback:
            return None

        try:
            # the previous model which comes from disk could be unrenderable
            previous_query = previous.render_query()
        except Exception:
            previous_query = None
        this_query = self.render_query()

        if previous_query is None or this_query is None:
            # Can't determine if there's a breaking change if we can't render the query.
            return None

        edits = diff(previous_query, this_query, matchings=[(previous_query, this_query)])
        inserted_expressions = {e.expression for e in edits if isinstance(e, Insert)}

        for edit in edits:
            if isinstance(edit, Insert):
                expr = edit.expression
                if _is_udtf(expr):
                    # projection subqueries do not change cardinality, engines don't allow these to return
                    # more than one row of data
                    parent = expr.find_ancestor(exp.Subquery)

                    if not parent:
                        return None

                    expr = parent

                if not _is_projection(expr) and expr.parent not in inserted_expressions:
                    return None
            elif not isinstance(edit, Keep):
                return None

        return False

    @cached_property
    def _query_renderer(self) -> QueryRenderer:
        no_quote_identifiers = self.kind.is_view and self.dialect in ("trino", "spark")
        return QueryRenderer(
            self.query,
            self.dialect,
            self.macro_definitions,
            schema=self.mapping_schema,
            model_fqn=self.fqn,
            path=self._path,
            jinja_macro_registry=self.jinja_macros,
            python_env=self.python_env,
            only_execution_time=self.kind.only_execution_time,
            default_catalog=self.default_catalog,
            quote_identifiers=not no_quote_identifiers,
        )

    @property
    def _data_hash_values(self) -> t.List[str]:
        data = super()._data_hash_values

        query = self.render_query() or self.query
        data.append(gen(query))
        data.extend(self.jinja_macros.data_hash_values)
        return data


class SeedModel(_SqlBasedModel):
    """The model definition which uses a pre-built static dataset to source the data from.

    Args:
        seed: The content of a pre-built static dataset.
    """

    kind: SeedKind
    seed: Seed
    column_hashes_: t.Optional[t.Dict[str, str]] = Field(default=None, alias="column_hashes")
    derived_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    is_hydrated: bool = True
    source_type: Literal["seed"] = "seed"

    def render(
        self,
        *,
        context: ExecutionContext,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        **kwargs: t.Any,
    ) -> t.Iterator[QueryOrDF]:
        if not self.is_hydrated:
            return
        yield from self.render_seed()

    def render_seed(self) -> t.Iterator[QueryOrDF]:
        self._ensure_hydrated()

        date_columns = []
        datetime_columns = []
        bool_columns = []
        string_columns = []

        for name, tpe in (self.columns_to_types_ or {}).items():
            if tpe.this in (exp.DataType.Type.DATE, exp.DataType.Type.DATE32):
                date_columns.append(name)
            elif tpe.this in exp.DataType.TEMPORAL_TYPES:
                datetime_columns.append(name)
            elif tpe.is_type("boolean"):
                bool_columns.append(name)
            elif tpe.this in exp.DataType.TEXT_TYPES:
                string_columns.append(name)

        for df in self._reader.read(batch_size=self.kind.batch_size):
            # convert all date/time types to native pandas timestamp
            for column in [*date_columns, *datetime_columns]:
                df[column] = pd.to_datetime(df[column])

            # extract datetime.date from pandas timestamp for DATE columns
            for column in date_columns:
                df[column] = df[column].dt.date

            df[bool_columns] = df[bool_columns].apply(lambda i: str_to_bool(str(i)))
            df.loc[:, string_columns] = df[string_columns].mask(
                cond=lambda x: x.notna(),  # type: ignore
                other=df[string_columns].astype(str),  # type: ignore
            )
            yield df.replace({np.nan: None})

    @property
    def columns_to_types(self) -> t.Optional[t.Dict[str, exp.DataType]]:
        if self.columns_to_types_ is not None:
            return self.columns_to_types_
        if self.derived_columns_to_types is not None:
            return self.derived_columns_to_types
        if self.is_hydrated:
            return self._reader.columns_to_types
        return None

    @property
    def column_hashes(self) -> t.Dict[str, str]:
        if self.column_hashes_ is not None:
            return self.column_hashes_
        self._ensure_hydrated()
        return self._reader.column_hashes

    @property
    def is_seed(self) -> bool:
        return True

    @property
    def seed_path(self) -> Path:
        seed_path = Path(self.kind.path)
        if not seed_path.is_absolute():
            return self._path.parent / seed_path
        return seed_path

    @cached_property
    def depends_on(self) -> t.Set[str]:
        return (self.depends_on_ or set()) - {self.fqn}

    @property
    def depends_on_self(self) -> bool:
        return False

    @property
    def batch_size(self) -> t.Optional[int]:
        # Unlike other model kinds, the batch size provided in the SEED kind represents the
        # maximum number of rows to insert in a single batch.
        # We should never batch intervals for seed models.
        return None

    def to_dehydrated(self) -> SeedModel:
        """Creates a dehydrated copy of this model.

        The dehydrated seed model will not contain the seed content, but will contain
        the column hashes. This is useful for comparing two seed models without
        having to read the seed content from disk.

        Returns:
            A dehydrated copy of this model.
        """
        if not self.is_hydrated:
            return self

        return self.copy(
            update={
                "seed": Seed(content=""),
                "is_hydrated": False,
                "column_hashes_": self.column_hashes,
                "derived_columns_to_types": self.columns_to_types
                if self.columns_to_types_ is None
                else None,
            }
        )

    def to_hydrated(self, content: str) -> SeedModel:
        """Creates a hydrated copy of this model with the given seed content.

        Returns:
            A hydrated copy of this model.
        """
        if self.is_hydrated:
            return self

        return self.copy(
            update={
                "seed": Seed(content=content),
                "is_hydrated": True,
                "column_hashes_": None,
            },
        )

    def is_breaking_change(self, previous: Model) -> t.Optional[bool]:
        if not isinstance(previous, SeedModel):
            return None

        new_columns = set(self.column_hashes)
        old_columns = set(previous.column_hashes)

        if not new_columns.issuperset(old_columns):
            return None

        for col in old_columns:
            if self.column_hashes[col] != previous.column_hashes[col]:
                return None

        return False

    def _ensure_hydrated(self) -> None:
        if not self.is_hydrated:
            raise SQLMeshError(f"Seed model '{self.name}' is not hydrated.")

    @cached_property
    def _reader(self) -> CsvSeedReader:
        return self.seed.reader(dialect=self.dialect, settings=self.kind.csv_settings)

    @property
    def _data_hash_values(self) -> t.List[str]:
        data = super()._data_hash_values
        for column_name, column_hash in self.column_hashes.items():
            data.append(column_name)
            data.append(column_hash)
        return data


class PythonModel(_Model):
    """The model definition which relies on a Python script to fetch the data.

    Args:
        entrypoint: The name of a Python function which contains the data fetching / transformation logic.
    """

    kind: ModelKind = FullKind()
    entrypoint: str
    source_type: Literal["python"] = "python"

    def validate_definition(self) -> None:
        super().validate_definition()

        if self.kind and not self.kind.supports_python_models:
            raise SQLMeshError(
                f"Cannot create Python model '{self.name}' as the '{self.kind.name}' kind doesnt support Python models"
            )

    def render(
        self,
        *,
        context: ExecutionContext,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        **kwargs: t.Any,
    ) -> t.Iterator[QueryOrDF]:
        env = prepare_env(self.python_env)
        start, end = make_inclusive(start or c.EPOCH, end or c.EPOCH)
        execution_time = to_datetime(execution_time or c.EPOCH)

        variables = env.get(c.SQLMESH_VARS, {})
        variables.update(kwargs.pop("variables", {}))

        try:
            kwargs = {
                **variables,
                **kwargs,
                "start": start,
                "end": end,
                "execution_time": execution_time,
                "latest": execution_time,  # TODO: Preserved for backward compatibility. Remove in 1.0.0.
            }
            df_or_iter = env[self.entrypoint](
                context=context.with_variables(variables),
                **kwargs,
            )

            if not isinstance(df_or_iter, types.GeneratorType):
                df_or_iter = [df_or_iter]

            for df in df_or_iter:
                yield df
        except Exception as e:
            print_exception(e, self.python_env)
            raise SQLMeshError(f"Error executing Python model '{self.name}'")

    def render_definition(
        self, include_python: bool = True, include_defaults: bool = False
    ) -> t.List[exp.Expression]:
        # Ignore the provided value for the include_python flag, since the Pyhon model's
        # definition without Python code is meaningless.
        return super().render_definition(include_python=True, include_defaults=include_defaults)

    @property
    def is_python(self) -> bool:
        return True

    def is_breaking_change(self, previous: Model) -> t.Optional[bool]:
        return None

    @property
    def _data_hash_values(self) -> t.List[str]:
        data = super()._data_hash_values
        data.append(self.entrypoint)
        return data


class ExternalModel(_Model):
    """The model definition which represents an external source/table."""

    source_type: Literal["external"] = "external"
    gateway: t.Optional[str] = None

    def is_breaking_change(self, previous: Model) -> t.Optional[bool]:
        if not isinstance(previous, ExternalModel):
            return None
        if not previous.columns_to_types_or_raise.items() - self.columns_to_types_or_raise.items():
            return False
        return None

    @property
    def depends_on(self) -> t.Set[str]:
        return set()

    @property
    def depends_on_self(self) -> bool:
        return False


Model = t.Union[SqlModel, SeedModel, PythonModel, ExternalModel]


def load_sql_based_model(
    expressions: t.List[exp.Expression],
    *,
    defaults: t.Optional[t.Dict[str, t.Any]] = None,
    path: Path = Path(),
    module_path: Path = Path(),
    time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
    macros: t.Optional[MacroRegistry] = None,
    jinja_macros: t.Optional[JinjaMacroRegistry] = None,
    python_env: t.Optional[t.Dict[str, Executable]] = None,
    dialect: t.Optional[str] = None,
    physical_schema_override: t.Optional[t.Dict[str, str]] = None,
    default_catalog: t.Optional[str] = None,
    variables: t.Optional[t.Dict[str, t.Any]] = None,
    infer_names: t.Optional[bool] = False,
    **kwargs: t.Any,
) -> Model:
    """Load a model from a parsed SQLMesh model SQL file.

    Args:
        expressions: Model, *Statements, Query.
        defaults: Definition default values.
        path: An optional path to the model definition file.
        module_path: The python module path to serialize macros for.
        time_column_format: The default time column format to use if no model time column is configured.
        macros: The custom registry of macros. If not provided the default registry will be used.
        jinja_macros: The registry of Jinja macros.
        python_env: The custom Python environment for macros. If not provided the environment will be constructed
            from the macro registry.
        dialect: The default dialect if no model dialect is configured.
            The format must adhere to Python's strftime codes.
        physical_schema_override: The physical schema override for the model.
        default_catalog: The default catalog if no model catalog is configured.
        variables: The variables to pass to the model.
        kwargs: Additional kwargs to pass to the loader.
    """
    if not expressions:
        raise_config_error("Incomplete model definition, missing MODEL statement", path)

    dialect = dialect or ""
    meta = expressions[0]
    if not isinstance(meta, d.Model):
        raise_config_error(
            "MODEL statement is required as the first statement in the definition",
            path,
        )

    unrendered_signals = None
    for prop in meta.expressions:
        if prop.name.lower() == "signals":
            unrendered_signals = prop.args.get("value")

    meta_python_env = _python_env(
        expressions=meta,
        jinja_macro_references=None,
        module_path=module_path,
        macros=macros or macro.get_registry(),
        variables=variables,
        path=path,
    )
    meta_renderer = ExpressionRenderer(
        meta,
        dialect,
        [],
        path=path,
        jinja_macro_registry=jinja_macros,
        python_env=meta_python_env,
        default_catalog=default_catalog,
        quote_identifiers=False,
        normalize_identifiers=False,
    )
    rendered_meta_exprs = meta_renderer.render()
    if rendered_meta_exprs is None or len(rendered_meta_exprs) != 1:
        raise_config_error(
            f"Invalid MODEL statement:\n{meta.sql(dialect=dialect, pretty=True)}",
            path,
        )
        raise
    rendered_meta = rendered_meta_exprs[0]

    # Extract the query and any pre/post statements
    query_or_seed_insert, pre_statements, post_statements, inline_audits = (
        _split_sql_model_statements(expressions[1:], path, dialect)
    )

    meta_fields: t.Dict[str, t.Any] = {
        "dialect": dialect,
        "description": (
            "\n".join(comment.strip() for comment in rendered_meta.comments)
            if rendered_meta.comments
            else None
        ),
        **{prop.name.lower(): prop.args.get("value") for prop in rendered_meta.expressions},
        **kwargs,
    }
    if unrendered_signals:
        # Signals must remain unrendered, so that they can be rendered later at evaluation runtime.
        meta_fields["signals"] = unrendered_signals

    if isinstance(meta_fields.get("dialect"), exp.Expression):
        meta_fields["dialect"] = meta_fields["dialect"].name

    # The name of the model will be inferred from its path relative to `models/`, if it's not explicitly specified
    name = meta_fields.pop("name", "")
    if not name and infer_names:
        name = get_model_name(path)

    if not name:
        raise_config_error("Model must have a name", path)
    if "default_catalog" in meta_fields:
        raise_config_error(
            "`default_catalog` cannot be set on a per-model basis. It must be set at the connection level or in Airflow.",
            path,
        )

    jinja_macro_references, used_variables = extract_macro_references_and_variables(
        *(gen(e) for e in pre_statements),
        *(gen(e) for e in post_statements),
        *([gen(query_or_seed_insert)] if query_or_seed_insert is not None else []),
    )

    jinja_macros = (jinja_macros or JinjaMacroRegistry()).trim(jinja_macro_references)
    for jinja_macro in jinja_macros.root_macros.values():
        used_variables.update(extract_macro_references_and_variables(jinja_macro.definition)[1])

    common_kwargs = dict(
        pre_statements=pre_statements,
        post_statements=post_statements,
        defaults=defaults,
        path=path,
        module_path=module_path,
        macros=macros,
        python_env=python_env,
        jinja_macros=jinja_macros,
        jinja_macro_references=jinja_macro_references,
        physical_schema_override=physical_schema_override,
        default_catalog=default_catalog,
        variables=variables,
        used_variables=used_variables,
        inline_audits=inline_audits,
        **meta_fields,
    )

    if query_or_seed_insert is not None and (
        isinstance(query_or_seed_insert, (exp.Query, d.JinjaQuery))
        or (
            isinstance(query_or_seed_insert, d.MacroFunc)
            and query_or_seed_insert.this.name.lower() == "union"
        )
    ):
        return create_sql_model(
            name,
            query_or_seed_insert,
            time_column_format=time_column_format,
            **common_kwargs,
        )
    else:
        try:
            seed_properties = {
                p.name.lower(): p.args.get("value") for p in common_kwargs.pop("kind").expressions
            }
            return create_seed_model(
                name,
                SeedKind(**seed_properties),
                **common_kwargs,
            )
        except Exception as ex:
            raise_config_error(
                f"The model definition must either have a SELECT query, a JINJA_QUERY block, or a valid Seed kind. {ex}."
            )
            raise


def create_sql_model(
    name: TableName,
    query: exp.Expression,
    *,
    pre_statements: t.Optional[t.List[exp.Expression]] = None,
    post_statements: t.Optional[t.List[exp.Expression]] = None,
    defaults: t.Optional[t.Dict[str, t.Any]] = None,
    path: Path = Path(),
    module_path: Path = Path(),
    time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
    macros: t.Optional[MacroRegistry] = None,
    python_env: t.Optional[t.Dict[str, Executable]] = None,
    jinja_macros: t.Optional[JinjaMacroRegistry] = None,
    jinja_macro_references: t.Optional[t.Set[MacroReference]] = None,
    dialect: t.Optional[str] = None,
    physical_schema_override: t.Optional[t.Dict[str, str]] = None,
    variables: t.Optional[t.Dict[str, t.Any]] = None,
    used_variables: t.Optional[t.Set[str]] = None,
    **kwargs: t.Any,
) -> Model:
    """Creates a SQL model.

    Args:
        name: The name of the model, which is of the form [catalog].[db].table.
            The catalog and db are optional.
        query: The model's logic in a form of a SELECT query.
        pre_statements: The list of SQL statements that precede the model's query.
        post_statements: The list of SQL statements that follow after the model's query.
        defaults: Definition default values.
        path: An optional path to the model definition file.
        module_path: The python module path to serialize macros for.
        time_column_format: The default time column format to use if no model time column is configured.
            The format must adhere to Python's strftime codes.
        macros: The custom registry of macros. If not provided the default registry will be used.
        python_env: The custom Python environment for macros. If not provided the environment will be constructed
            from the macro registry.
        jinja_macros: The registry of Jinja macros.
        jinja_macro_references: The set of Jinja macros referenced by this model.
        dialect: The default dialect if no model dialect is configured.
        physical_schema_override: The physical schema override.
        variables: User-defined variables.
        used_variables: The set of variable names used by this model.
    """
    if not isinstance(query, (exp.Query, d.JinjaQuery, d.MacroFunc)):
        # Users are not expected to pass in a single MacroFunc instance for a model's query;
        # this is an implementation detail which allows us to create python models that return
        # SQL, either in the form of SQLGlot expressions or just plain strings.
        raise_config_error(
            "A query is required and must be a SELECT statement, a UNION statement, or a JINJA_QUERY block",
            path,
        )

    pre_statements = pre_statements or []
    post_statements = post_statements or []

    if not python_env:
        python_env = _python_env(
            [*pre_statements, query, *post_statements],
            jinja_macro_references,
            module_path,
            macros or macro.get_registry(),
            variables=variables,
            used_variables=used_variables,
            path=path,
        )
    else:
        python_env = _add_variables_to_python_env(python_env, used_variables, variables)

    return _create_model(
        SqlModel,
        name,
        defaults=defaults,
        path=path,
        time_column_format=time_column_format,
        python_env=python_env,
        jinja_macros=jinja_macros,
        dialect=dialect,
        query=query,
        pre_statements=pre_statements,
        post_statements=post_statements,
        physical_schema_override=physical_schema_override,
        **kwargs,
    )


def create_seed_model(
    name: TableName,
    seed_kind: SeedKind,
    *,
    dialect: t.Optional[str] = None,
    pre_statements: t.Optional[t.List[exp.Expression]] = None,
    post_statements: t.Optional[t.List[exp.Expression]] = None,
    defaults: t.Optional[t.Dict[str, t.Any]] = None,
    path: Path = Path(),
    module_path: Path = Path(),
    macros: t.Optional[MacroRegistry] = None,
    python_env: t.Optional[t.Dict[str, Executable]] = None,
    jinja_macros: t.Optional[JinjaMacroRegistry] = None,
    jinja_macro_references: t.Optional[t.Set[MacroReference]] = None,
    physical_schema_override: t.Optional[t.Dict[str, str]] = None,
    variables: t.Optional[t.Dict[str, t.Any]] = None,
    used_variables: t.Optional[t.Set[str]] = None,
    **kwargs: t.Any,
) -> Model:
    """Creates a Seed model.

    Args:
        name: The name of the model, which is of the form [catalog].[db].table.
            The catalog and db are optional.
        seed_kind: The information about the location of a seed and other related configuration.
        dialect: The default dialect if no model dialect is configured.
        pre_statements: The list of SQL statements that precede the insertion of the seed's content.
        post_statements: The list of SQL statements that follow after the insertion of the seed's content.
        defaults: Definition default values.
        path: An optional path to the model definition file.
        macros: The custom registry of macros. If not provided the default registry will be used.
        python_env: The custom Python environment for macros. If not provided the environment will be constructed
            from the macro registry.
        jinja_macros: The registry of Jinja macros.
        jinja_macro_references: The set of Jinja macros referenced by this model.
        physical_schema_override: The physical schema override.
        variables: User-defined variables.
        used_variables: The set of variable names used by this model.
    """
    seed_path = Path(seed_kind.path)
    marker, *subdirs = seed_path.parts
    if marker.lower() == "$root":
        seed_path = module_path.joinpath(*subdirs)
        seed_kind.path = str(seed_path)
    elif not seed_path.is_absolute():
        seed_path = path / seed_path if path.is_dir() else path.parent / seed_path

    seed = create_seed(seed_path)

    pre_statements = pre_statements or []
    post_statements = post_statements or []

    if not python_env:
        python_env = _python_env(
            [*pre_statements, *post_statements],
            jinja_macro_references,
            module_path,
            macros or macro.get_registry(),
            variables=variables,
            used_variables=used_variables,
            path=path,
        )
    else:
        python_env = _add_variables_to_python_env(python_env, used_variables, variables)

    return _create_model(
        SeedModel,
        name,
        dialect=dialect,
        defaults=defaults,
        path=path,
        seed=seed,
        kind=seed_kind,
        depends_on=kwargs.pop("depends_on", None),
        python_env=python_env,
        jinja_macros=jinja_macros,
        pre_statements=pre_statements,
        post_statements=post_statements,
        physical_schema_override=physical_schema_override,
        **kwargs,
    )


def create_python_model(
    name: str,
    entrypoint: str,
    python_env: t.Dict[str, Executable],
    *,
    defaults: t.Optional[t.Dict[str, t.Any]] = None,
    path: Path = Path(),
    time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
    depends_on: t.Optional[t.Set[str]] = None,
    physical_schema_override: t.Optional[t.Dict[str, str]] = None,
    variables: t.Optional[t.Dict[str, t.Any]] = None,
    **kwargs: t.Any,
) -> Model:
    """Creates a Python model.

    Args:
        name: The name of the model, which is of the form [catalog].[db].table.
            The catalog and db are optional.
        entrypoint: The name of a Python function which contains the data fetching / transformation logic.
        python_env: The Python environment of all objects referenced by the model implementation.
        defaults: Definition default values.
        path: An optional path to the model definition file.
        time_column_format: The default time column format to use if no model time column is configured.
        depends_on: The custom set of model's upstream dependencies.
    """
    # Find dependencies for python models by parsing code if they are not explicitly defined
    # Also remove self-references that are found
    parsed_depends_on, referenced_variables = (
        _parse_dependencies(python_env, entrypoint) if python_env is not None else (set(), set())
    )
    if depends_on is None:
        depends_on = parsed_depends_on - {name}

    variables = {k: v for k, v in (variables or {}).items() if k in referenced_variables}
    if variables:
        python_env[c.SQLMESH_VARS] = Executable.value(variables)

    return _create_model(
        PythonModel,
        name,
        defaults=defaults,
        path=path,
        time_column_format=time_column_format,
        depends_on=depends_on,
        entrypoint=entrypoint,
        python_env=python_env,
        physical_schema_override=physical_schema_override,
        **kwargs,
    )


def create_external_model(
    name: TableName,
    *,
    dialect: t.Optional[str] = None,
    path: Path = Path(),
    defaults: t.Optional[t.Dict[str, t.Any]] = None,
    **kwargs: t.Any,
) -> ExternalModel:
    """Creates an external model.

    Args:
        name: The name of the model, which is of the form [catalog].[db].table.
            The catalog and db are optional.
        dialect: The dialect to serialize.
        path: An optional path to the model definition file.
    """
    return t.cast(
        ExternalModel,
        _create_model(
            ExternalModel,
            name,
            defaults=defaults,
            dialect=dialect,
            path=path,
            kind=ModelKindName.EXTERNAL.value,
            **kwargs,
        ),
    )


def _create_model(
    klass: t.Type[_Model],
    name: TableName,
    *,
    defaults: t.Optional[t.Dict[str, t.Any]] = None,
    path: Path = Path(),
    time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
    jinja_macros: t.Optional[JinjaMacroRegistry] = None,
    jinja_macro_references: t.Optional[t.Set[MacroReference]] = None,
    depends_on: t.Optional[t.Set[str]] = None,
    dialect: t.Optional[str] = None,
    physical_schema_override: t.Optional[t.Dict[str, str]] = None,
    **kwargs: t.Any,
) -> Model:
    _validate_model_fields(klass, {"name", *kwargs} - {"grain", "table_properties"}, path)

    kwargs["session_properties"] = _resolve_session_properties(
        (defaults or {}).get("session_properties"), kwargs.get("session_properties")
    )

    dialect = dialect or ""
    physical_schema_override = physical_schema_override or {}

    raw_kind = kwargs.pop("kind", None)
    if raw_kind:
        kwargs["kind"] = create_model_kind(raw_kind, dialect, defaults or {})

    defaults = {k: v for k, v in (defaults or {}).items() if k in klass.all_fields()}

    try:
        model = klass(
            name=name,
            **{
                **(defaults or {}),
                "jinja_macros": jinja_macros or JinjaMacroRegistry(),
                "dialect": dialect,
                "depends_on": depends_on,
                "physical_schema_override": physical_schema_override.get(
                    exp.to_table(name, dialect=dialect).db
                ),
                **kwargs,
            },
        )
    except Exception as ex:
        raise_config_error(str(ex), location=path)
        raise

    model._path = path
    model.set_time_format(time_column_format)

    return t.cast(Model, model)


INSERT_SEED_MACRO_CALL = d.parse_one("@INSERT_SEED()")


def _split_sql_model_statements(
    expressions: t.List[exp.Expression], path: Path, dialect: t.Optional[str] = None
) -> t.Tuple[
    t.Optional[exp.Expression],
    t.List[exp.Expression],
    t.List[exp.Expression],
    UniqueKeyDict[str, ModelAudit],
]:
    """Extracts the SELECT query from a sequence of expressions.

    Args:
        expressions: The list of all SQL statements in the model definition.

    Returns:
        A tuple containing the extracted SELECT query or the `@INSERT_SEED()` call, the statements before the it,
        the statements after it, and the inline audit definitions.

    Raises:
        ConfigError: If the model definition contains more than one SELECT query or `@INSERT_SEED()` call.
    """
    from sqlmesh.core.audit import ModelAudit, load_audit

    query_positions = []
    sql_statements = []
    inline_audits: UniqueKeyDict[str, ModelAudit] = UniqueKeyDict("inline_audits")

    idx = 0
    length = len(expressions)
    while idx < length:
        expr = expressions[idx]

        if isinstance(expr, d.Audit):
            loaded_audit = load_audit([expr, expressions[idx + 1]], dialect=dialect)
            assert isinstance(loaded_audit, ModelAudit)
            inline_audits[loaded_audit.name] = loaded_audit
            idx += 2
        else:
            if (
                isinstance(expr, (exp.Query, d.JinjaQuery))
                or expr == INSERT_SEED_MACRO_CALL
                or (isinstance(expr, d.MacroFunc) and expr.this.name.lower() == "union")
            ):
                query_positions.append((expr, idx))
            sql_statements.append(expr)
            idx += 1

    if not query_positions:
        return None, sql_statements, [], inline_audits

    elif len(query_positions) > 1:
        raise_config_error("Only one SELECT query is allowed per model", path)

    query, pos = query_positions[0]
    return query, sql_statements[:pos], sql_statements[pos + 1 :], inline_audits


def _resolve_session_properties(
    default: t.Optional[t.Dict[str, t.Any]],
    provided: t.Optional[exp.Expression | t.Dict[str, t.Any]],
) -> t.Optional[exp.Expression]:
    if isinstance(provided, dict):
        session_properties = {k: exp.Literal.string(k).eq(v) for k, v in provided.items()}
    elif provided:
        if isinstance(provided, exp.Paren):
            provided = exp.Tuple(expressions=[provided.this])
        session_properties = {expr.this.name: expr for expr in provided}
    else:
        session_properties = {}

    for k, v in (default or {}).items():
        if k not in session_properties:
            session_properties[k] = exp.Literal.string(k).eq(v)

    if session_properties:
        return exp.Tuple(expressions=list(session_properties.values()))

    return None


def _validate_model_fields(klass: t.Type[_Model], provided_fields: t.Set[str], path: Path) -> None:
    missing_required_fields = klass.missing_required_fields(provided_fields)
    if missing_required_fields:
        raise_config_error(
            f"Missing required fields {missing_required_fields} in the model definition",
            path,
        )

    extra_fields = klass.extra_fields(provided_fields)
    if extra_fields:
        raise_config_error(f"Invalid extra fields {extra_fields} in the model definition", path)


def _python_env(
    expressions: t.Union[exp.Expression, t.List[exp.Expression]],
    jinja_macro_references: t.Optional[t.Set[MacroReference]],
    module_path: Path,
    macros: MacroRegistry,
    variables: t.Optional[t.Dict[str, t.Any]] = None,
    used_variables: t.Optional[t.Set[str]] = None,
    path: t.Optional[str | Path] = None,
) -> t.Dict[str, Executable]:
    python_env: t.Dict[str, Executable] = {}
    variables = variables or {}

    used_macros = {}
    used_variables = (used_variables or set()).copy()
    serialized_env = {}

    expressions = ensure_list(expressions)
    for expression in expressions:
        if not isinstance(expression, d.Jinja):
            for macro_func_or_var in expression.find_all(d.MacroFunc, d.MacroVar, exp.Identifier):
                if macro_func_or_var.__class__ is d.MacroFunc:
                    name = macro_func_or_var.this.name.lower()
                    if name in macros:
                        used_macros[name] = macros[name]
                        if name == c.VAR:
                            args = macro_func_or_var.this.expressions
                            if len(args) < 1:
                                raise_config_error("Macro VAR requires at least one argument", path)
                            if not args[0].is_string:
                                raise_config_error(
                                    f"The variable name must be a string literal, '{args[0].sql()}' was given instead",
                                    path,
                                )
                            used_variables.add(args[0].this.lower())
                elif macro_func_or_var.__class__ is d.MacroVar:
                    name = macro_func_or_var.name.lower()
                    if name in macros:
                        used_macros[name] = macros[name]
                    elif name in variables:
                        used_variables.add(name)
                elif (
                    isinstance(macro_func_or_var, (exp.Identifier, d.MacroStrReplace, d.MacroSQL))
                ) and "@" in macro_func_or_var.name:
                    for _, identifier, braced_identifier, _ in MacroStrTemplate.pattern.findall(
                        macro_func_or_var.name
                    ):
                        var_name = braced_identifier or identifier
                        if var_name in variables:
                            used_variables.add(var_name)

    for macro_ref in jinja_macro_references or set():
        if macro_ref.package is None and macro_ref.name in macros:
            used_macros[macro_ref.name] = macros[macro_ref.name]

    for name, used_macro in used_macros.items():
        if isinstance(used_macro, Executable):
            serialized_env[name] = used_macro
        elif not hasattr(used_macro, c.SQLMESH_BUILTIN):
            build_env(used_macro.func, env=python_env, name=name, path=module_path)

    serialized_env.update(serialize_env(python_env, path=module_path))
    return _add_variables_to_python_env(serialized_env, used_variables, variables)


def _add_variables_to_python_env(
    python_env: t.Dict[str, Executable],
    used_variables: t.Optional[t.Set[str]],
    variables: t.Optional[t.Dict[str, t.Any]],
) -> t.Dict[str, Executable]:
    _, python_used_variables = _parse_dependencies(python_env, None)
    used_variables = (used_variables or set()) | python_used_variables

    variables = {k: v for k, v in (variables or {}).items() if k in used_variables}
    if variables:
        python_env[c.SQLMESH_VARS] = Executable.value(variables)

    return python_env


def _parse_dependencies(
    python_env: t.Dict[str, Executable], entrypoint: t.Optional[str]
) -> t.Tuple[t.Set[str], t.Set[str]]:
    """Parses the source of a model function and finds upstream table dependencies and referenced variables based on calls to context / evaluator.

    Args:
        python_env: A dictionary of Python definitions.

    Returns:
        A tuple containing the set of upstream table dependencies and the set of referenced variables.
    """
    env = prepare_env(python_env)
    depends_on = set()
    variables = set()

    for executable in python_env.values():
        if not executable.is_definition:
            continue
        for node in ast.walk(ast.parse(executable.payload)):
            if isinstance(node, ast.Call):
                func = node.func
                if not isinstance(func, ast.Attribute) or not isinstance(func.value, ast.Name):
                    continue

                def get_first_arg(keyword_arg_name: str) -> t.Any:
                    if node.args:
                        first_arg: t.Optional[ast.expr] = node.args[0]
                    else:
                        first_arg = next(
                            (
                                keyword.value
                                for keyword in node.keywords
                                if keyword.arg == keyword_arg_name
                            ),
                            None,
                        )

                    try:
                        expression = to_source(first_arg)
                        return eval(expression, env)
                    except Exception:
                        raise ConfigError(
                            f"Error resolving dependencies for '{executable.path}'. Argument '{expression.strip()}' must be resolvable at parse time."
                        )

                if func.value.id == "context" and func.attr == "table":
                    depends_on.add(get_first_arg("model_name"))
                elif func.value.id in ("context", "evaluator") and func.attr == c.VAR:
                    variables.add(get_first_arg("var_name").lower())
            elif (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id in ("context", "evaluator")
                and node.attr == c.GATEWAY
            ):
                # Check whether the gateway attribute is referenced.
                variables.add(c.GATEWAY)
            elif isinstance(node, ast.FunctionDef) and node.name == entrypoint:
                variables.update([arg.arg for arg in node.args.args if arg.arg != "context"])

    return depends_on, variables


def _list_of_calls_to_exp(value: t.List[t.Tuple[str, t.Dict[str, t.Any]]]) -> exp.Expression:
    return exp.Tuple(
        expressions=[
            exp.Anonymous(
                this=v[0],
                expressions=[
                    exp.EQ(this=exp.convert(left), expression=exp.convert(right))
                    for left, right in v[1].items()
                ],
            )
            for v in value
        ]
    )


def _is_projection(expr: exp.Expression) -> bool:
    parent = expr.parent
    return isinstance(parent, exp.Select) and expr.arg_key == "expressions"


def _is_udtf(expr: exp.Expression) -> bool:
    return isinstance(expr, (exp.Explode, exp.Posexplode, exp.Unnest)) or (
        isinstance(expr, exp.Anonymous)
        and expr.this.upper() in ("EXPLODE_OUTER", "POSEXPLODE_OUTER", "UNNEST")
    )


def _single_value_or_tuple(values: t.Sequence) -> exp.Identifier | exp.Tuple:
    return (
        exp.to_identifier(values[0])
        if len(values) == 1
        else exp.Tuple(expressions=[exp.to_identifier(v) for v in values])
    )


def _single_expr_or_tuple(values: t.Sequence[exp.Expression]) -> exp.Expression | exp.Tuple:
    return values[0] if len(values) == 1 else exp.Tuple(expressions=values)


def _refs_to_sql(values: t.Any) -> exp.Expression:
    return exp.Tuple(expressions=values)


META_FIELD_CONVERTER: t.Dict[str, t.Callable] = {
    "start": lambda value: exp.Literal.string(value),
    "cron": lambda value: exp.Literal.string(value),
    "partitioned_by_": _single_expr_or_tuple,
    "clustered_by": _single_value_or_tuple,
    "depends_on_": lambda value: exp.Tuple(expressions=sorted(value)),
    "pre": _list_of_calls_to_exp,
    "post": _list_of_calls_to_exp,
    "audits": _list_of_calls_to_exp,
    "columns_to_types_": lambda value: exp.Schema(
        expressions=[exp.ColumnDef(this=exp.to_column(c), kind=t) for c, t in value.items()]
    ),
    "tags": _single_value_or_tuple,
    "grains": _refs_to_sql,
    "references": _refs_to_sql,
    "physical_properties_": lambda value: value,
    "virtual_properties_": lambda value: value,
    "session_properties_": lambda value: value,
    "allow_partials": exp.convert,
    "signals": lambda values: exp.Tuple(expressions=values),
}


def get_model_name(path: Path) -> str:
    path_parts = list(path.parts[path.parts.index("models") + 1 : -1]) + [path.stem]
    return ".".join(path_parts[-3:])
