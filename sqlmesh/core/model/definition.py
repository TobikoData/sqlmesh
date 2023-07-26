from __future__ import annotations

import ast
import json
import logging
import sys
import types
import typing as t
from difflib import unified_diff
from itertools import zip_longest
from pathlib import Path
from textwrap import indent

import pandas as pd
from astor import to_source
from pydantic import Field
from sqlglot import diff, exp
from sqlglot.diff import Insert, Keep
from sqlglot.helper import ensure_list
from sqlglot.schema import MappingSchema, nested_set
from sqlglot.time import format_time

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.macros import MacroRegistry, macro
from sqlmesh.core.model.common import expression_validator
from sqlmesh.core.model.kind import (
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    ModelKindName,
    SeedKind,
)
from sqlmesh.core.model.meta import ModelMeta
from sqlmesh.core.model.seed import Seed, create_seed
from sqlmesh.core.renderer import ExpressionRenderer, QueryRenderer
from sqlmesh.utils import str_to_bool
from sqlmesh.utils.date import TimeLike, make_inclusive, to_datetime
from sqlmesh.utils.errors import ConfigError, SQLMeshError, raise_config_error
from sqlmesh.utils.hashing import hash_data
from sqlmesh.utils.jinja import JinjaMacroRegistry, extract_macro_references
from sqlmesh.utils.metaprogramming import (
    Executable,
    build_env,
    prepare_env,
    print_exception,
    serialize_env,
)

if t.TYPE_CHECKING:
    from sqlmesh.core.audit import Audit
    from sqlmesh.core.context import ExecutionContext
    from sqlmesh.core.engine_adapter import EngineAdapter
    from sqlmesh.core.engine_adapter._typing import QueryOrDF
    from sqlmesh.core.snapshot import Snapshot
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
            batch_size     30,
            start          '2020-01-01',
            partitioned_by ds
        );

        @DEF(var, 'my_var');

        SELECT
          1 AS column_a # my first column,
          @var AS my_column #my second column,
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
        batch_size: The maximum number of incremental intervals that can be run per backfill job. If this is None,
            then backfilling this model will do all of history in one job. If this is set, a model's backfill
            will be chunked such that each individual job will only contain jobs with max `batch_size` intervals.
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

    _path: Path = Path()
    _depends_on: t.Optional[t.Set[str]] = None
    _depends_on_past: t.Optional[bool] = None
    _column_descriptions: t.Optional[t.Dict[str, str]] = None

    _expressions_validator = expression_validator

    def render(
        self,
        *,
        context: ExecutionContext,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        **kwargs: t.Any,
    ) -> t.Generator[QueryOrDF, None, None]:
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
            A generator which yields eiether a query object or one of the supported dataframe objects.
        """
        yield self.render_query_or_raise(
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=context.snapshots,
            is_dev=context.is_dev,
            engine_adapter=context.engine_adapter,
            **kwargs,
        )

    def render_definition(self, include_python: bool = True) -> t.List[exp.Expression]:
        """Returns the original list of sql expressions comprising the model definition.

        Args:
            include_python: Whether or not to include Python code in the rendered definition.
        """
        expressions = []
        comment = None
        for field in ModelMeta.__fields__.values():
            field_value = getattr(self, field.name)

            if field_value != field.default:
                if field.name == "description":
                    comment = field_value
                elif field.name == "kind":
                    expressions.append(
                        exp.Property(
                            this="kind",
                            value=field_value.to_expression(dialect=self.dialect),
                        )
                    )
                elif field.name != "column_descriptions_":
                    expressions.append(
                        exp.Property(
                            this=field.alias or field.name,
                            value=META_FIELD_CONVERTER.get(field.name, exp.to_identifier)(
                                field_value
                            ),
                        )
                    )

        model = d.Model(expressions=expressions)
        model.comments = [comment] if comment else None

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

        return [
            model,
            *python_expressions,
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
        is_dev: bool = False,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Subqueryable]:
        """Renders a model's query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            execution_time: The date/time time reference to use for execution time.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            table_mapping: Table mapping of physical locations. Takes precedence over snapshot mappings.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        return exp.select(
            *(
                exp.cast(exp.Null(), column_type, copy=False).as_(name, copy=False)
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
        is_dev: bool = False,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> exp.Subqueryable:
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
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.
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
            is_dev=is_dev,
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
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        is_dev: bool = False,
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
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.
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
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        is_dev: bool = False,
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
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The list of rendered expressions.
        """
        return []

    def ctas_query(self, **render_kwarg: t.Any) -> exp.Subqueryable:
        """Return a dummy query to do a CTAS.

        If a model's column types are unknown, the only way to create the table is to
        run the fully expanded query. This can be expensive so we add a WHERE FALSE to all
        SELECTS and hopefully the optimizer is smart enough to not do anything.

        Args:
            render_kwarg: Additional kwargs to pass to the renderer.
        Return:
            The mocked out ctas query.
        """
        query = self.render_query_or_raise(**render_kwarg)
        # the query is expanded so it's been copied, it's safe to mutate.
        for select in query.find_all(exp.Select):
            if select.args.get("from"):
                select.where(exp.false(), copy=False)

        return query

    def referenced_audits(self, audits: t.Dict[str, Audit]) -> t.List[Audit]:
        """Returns audits referenced in this model.

        Args:
            audits: Available audits by name.
        """
        from sqlmesh.core.audit import BUILT_IN_AUDITS

        referenced_audits = []
        for audit_name, _ in self.audits:
            if audit_name in audits:
                referenced_audits.append(audits[audit_name])
            elif audit_name not in BUILT_IN_AUDITS:
                raise_config_error(
                    f"Unknown audit '{audit_name}' referenced in model '{self.name}'",
                    self._path,
                )
        return referenced_audits

    def text_diff(self, other: Model) -> str:
        """Produce a text diff against another model.

        Args:
            other: The model to diff against.

        Returns:
            A unified text diff showing additions and deletions.
        """
        meta_a, *statements_a = self.render_definition()
        meta_b, *statements_b = other.render_definition()

        query_a = statements_a.pop() if statements_a else None
        query_b = statements_b.pop() if statements_b else None

        return "\n".join(
            (
                d.text_diff(meta_a, meta_b, self.dialect),
                *(
                    d.text_diff(sa, sb, self.dialect)
                    for sa, sb in zip_longest(statements_a, statements_b)
                ),
                d.text_diff(query_a, query_b, self.dialect),
            )
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
            if self.time_column.format:
                time = to_datetime(time).strftime(self.time_column.format)

            if columns_to_types is None:
                columns_to_types = self.columns_to_types_or_raise

            if self.time_column.column not in columns_to_types:
                raise ConfigError(
                    f"Time column '{self.time_column.column}' not found in model '{self.name}'."
                )

            time_column_type = columns_to_types[self.time_column.column]
            if time_column_type.this in exp.DataType.TEXT_TYPES:
                return exp.Literal.string(time)
            elif time_column_type.this in exp.DataType.NUMERIC_TYPES:
                return exp.Literal.number(time)
            elif time_column_type.this in exp.DataType.TEMPORAL_TYPES:
                return exp.cast(exp.Literal.string(time), time_column_type)
        return exp.convert(time)

    def update_schema(self, schema: MappingSchema) -> None:
        """Updates the schema for this model's dependencies based on the given mapping schema."""
        for dep in self.depends_on:
            table = exp.to_table(dep)
            mapping_schema = schema.find(table)

            if mapping_schema:
                nested_set(
                    self.mapping_schema,
                    tuple(str(part) for part in table.parts),
                    {k: str(v) for k, v in mapping_schema.items()},
                )
            else:
                # Reset the entire mapping if at least one upstream dependency is missing from the mapping
                # to prevent partial mappings from being used.
                if self.contains_star_projection:
                    logger.warning(
                        "Missing schema for model '%s' referenced in model '%s'. Run `sqlmesh create_external_models` "
                        "and / or make sure that the model '%s' can be rendered at parse time",
                        dep,
                        self.name,
                        dep,
                    )
                self.mapping_schema.clear()
                return

    @property
    def depends_on(self) -> t.Set[str]:
        """All of the upstream dependencies referenced in the model's query, excluding self references.

        Returns:
            A list of all the upstream table names.
        """
        return self.depends_on_ or set()

    @property
    def contains_star_projection(self) -> t.Optional[bool]:
        """Returns True if the model contains a star projection, False if it does not, and None if this
        cannot be determined at parse time."""
        return False

    @property
    def columns_to_types(self) -> t.Optional[t.Dict[str, exp.DataType]]:
        """Returns the mapping of column names to types of this model."""
        return self.columns_to_types_

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
        columns_to_types = self.columns_to_types
        if columns_to_types is None:
            return False
        return all(
            column_type.this != exp.DataType.Type.UNKNOWN
            for column_type in columns_to_types.values()
        )

    @property
    def sorted_python_env(self) -> t.List[t.Tuple[str, Executable]]:
        """Returns the python env sorted by executable kind and then var name."""
        return sorted(self.python_env.items(), key=lambda x: (x[1].kind, x[0]))

    @property
    def view_name(self) -> str:
        return exp.to_table(self.name).name

    @property
    def python_env(self) -> t.Dict[str, Executable]:
        return self.python_env_ or {}

    @property
    def schema_name(self) -> str:
        return exp.to_table(self.name).db or c.DEFAULT_SCHEMA

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

    @property
    def depends_on_past(self) -> bool:
        if self._depends_on_past is None:
            if self.kind.is_incremental_by_unique_key:
                return True

            query = self.render_query(optimize=False)
            if query is None:
                return False
            return self.name in d.find_tables(query, dialect=self.dialect)

        return self._depends_on_past

    @property
    def forward_only(self) -> bool:
        return getattr(self.kind, "forward_only", False)

    @property
    def disable_restatement(self) -> bool:
        return getattr(self.kind, "disable_restatement", False)

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
                    column_names = {c.lower() for c in columns_to_types}
                    missing_keys = unique_keys - column_names
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
        return hash_data(self._data_hash_fields)

    @property
    def _data_hash_fields(self) -> t.List[str]:
        data = [
            str(self.sorted_python_env),
            self.kind.name,
            self.cron,
            self.storage_format,
            str(self.lookback),
            *(expr.sql() for expr in (self.partitioned_by or [])),
            *(self.clustered_by or []),
            self.stamp,
            self.physical_schema,
        ]

        for column_name, column_type in (self.columns_to_types_ or {}).items():
            data.append(column_name)
            data.append(column_type.sql())

        for key, value in (self.table_properties_ or {}).items():
            data.append(key)
            data.append(value.sql())

        if isinstance(self.kind, IncrementalByTimeRangeKind):
            data.append(self.kind.time_column.column)
            data.append(self.kind.time_column.format)
        elif isinstance(self.kind, IncrementalByUniqueKeyKind):
            data.extend(self.kind.unique_key)

        return data  # type: ignore

    def metadata_hash(self, audits: t.Dict[str, Audit]) -> str:
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
            str(self.start) if self.start else None,
            str(self.retention) if self.retention else None,
            str(self.batch_size) if self.batch_size is not None else None,
            json.dumps(self.mapping_schema, sort_keys=True),
            *sorted(self.tags),
            *sorted(self.grain),
            str(self.forward_only),
            str(self.disable_restatement),
            str(self.interval_unit_) if self.interval_unit_ is not None else None,
        ]

        for audit_name, audit_args in sorted(self.audits, key=lambda a: a[0]):
            metadata.append(audit_name)

            if audit_name in BUILT_IN_AUDITS:
                for arg_name, arg_value in audit_args.items():
                    metadata.append(arg_name)
                    metadata.append(arg_value.sql(comments=True))
            elif audit_name in audits:
                audit = audits[audit_name]
                query = (
                    audit.query
                    if self.hash_raw_query
                    else audit.render_query(self, **t.cast(t.Dict[str, t.Any], audit_args))
                    or audit.query
                )
                metadata.extend(
                    [
                        query.sql(comments=True),
                        audit.dialect,
                        str(audit.skip),
                        str(audit.blocking),
                    ]
                )
            else:
                raise SQLMeshError(f"Unexpected audit name '{audit_name}'.")

        # Add comments from the query.
        if self.is_sql:
            rendered_query = self.render_query()
            if rendered_query:
                for e, _, _ in rendered_query.walk():
                    if e.comments:
                        metadata.extend(e.comments)

        return hash_data(metadata)


class _SqlBasedModel(_Model):
    pre_statements_: t.Optional[t.List[exp.Expression]] = Field(
        default=None, alias="pre_statements"
    )
    post_statements_: t.Optional[t.List[exp.Expression]] = Field(
        default=None, alias="post_statements"
    )

    __statement_renderers: t.Dict[int, ExpressionRenderer] = {}

    _expression_validator = expression_validator

    def render_pre_statements(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        is_dev: bool = False,
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
            is_dev=is_dev,
            engine_adapter=engine_adapter,
            **kwargs,
        )

    def render_post_statements(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        is_dev: bool = False,
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
            is_dev=is_dev,
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
        return [r for expressions in rendered for r in expressions]

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
            )
        return self.__statement_renderers[expression_key]

    @property
    def _data_hash_fields(self) -> t.List[str]:
        pre_statements = (
            self.pre_statements if self.hash_raw_query else self.render_pre_statements()
        )
        post_statements = (
            self.post_statements if self.hash_raw_query else self.render_post_statements()
        )
        macro_defs = self.macro_definitions if self.hash_raw_query else []
        return [
            *super()._data_hash_fields,
            *[e.sql(comments=False) for e in (*pre_statements, *post_statements, *macro_defs)],
        ]


class SqlModel(_SqlBasedModel):
    """The model definition which relies on a SQL query to fetch the data.

    Args:
        query: The main query representing the model.
        pre_statements: The list of SQL statements that precede the model's query.
        post_statements: The list of SQL statements that follow after the model's query.
    """

    query: t.Union[exp.Subqueryable, d.JinjaQuery]
    source_type: Literal["sql"] = "sql"

    _columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    __query_renderer: t.Optional[QueryRenderer] = None

    def render_query(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        expand: t.Iterable[str] = tuple(),
        is_dev: bool = False,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Subqueryable]:
        query = self._query_renderer.render(
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=snapshots,
            table_mapping=table_mapping,
            expand=expand,
            is_dev=is_dev,
            engine_adapter=engine_adapter,
            **kwargs,
        )
        if query and engine_adapter and logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Rendered query for '{self.name}':\n{indent(query.sql(dialect=self.dialect, pretty=True), '  ')}"
            )
        return query

    def render_definition(self, include_python: bool = True) -> t.List[exp.Expression]:
        result = super().render_definition(include_python=include_python)
        result.extend(self.pre_statements)
        result.extend(self.post_statements)
        result.append(self.query)
        return result

    @property
    def is_sql(self) -> bool:
        return True

    @property
    def depends_on(self) -> t.Set[str]:
        if self._depends_on is None:
            self._depends_on = self.depends_on_ or set()

            query = self.render_query(optimize=False)
            if query is not None:
                self._depends_on |= d.find_tables(query, dialect=self.dialect)

            self._depends_on -= {self.name}
        return self._depends_on

    @property
    def contains_star_projection(self) -> t.Optional[bool]:
        query = self._query_renderer.render(optimize=False)
        if query is None:
            return None
        return any(isinstance(expression, exp.Star) for expression in query.expressions)

    @property
    def columns_to_types(self) -> t.Optional[t.Dict[str, exp.DataType]]:
        if self.columns_to_types_ is not None:
            return self.columns_to_types_

        if self._columns_to_types is None:
            query = self._query_renderer.render()
            if query is None:
                return None
            self._columns_to_types = d.extract_columns_to_types(query)

        if "*" in self._columns_to_types:
            return None
        return self._columns_to_types

    @property
    def column_descriptions(self) -> t.Dict[str, str]:
        if self.column_descriptions_ is not None:
            return self.column_descriptions_

        if self._column_descriptions is None:
            query = self.render_query(optimize=False)
            if query is None:
                return {}

            self._column_descriptions = {
                select.alias: "\n".join(comment.strip() for comment in select.comments)
                for select in query.selects
                if select.comments
            }
        return self._column_descriptions

    def update_schema(self, schema: MappingSchema) -> None:
        super().update_schema(schema)
        self._columns_to_types = None
        self._query_renderer._optimized_cache = {}

    def validate_definition(self) -> None:
        query = self._query_renderer.render()

        if query is None:
            if self.depends_on_ is None:
                raise_config_error(
                    "Dependencies must be provided explicitly for models that can be rendered only at runtime",
                    self._path,
                )
            return

        if not isinstance(query, exp.Subqueryable):
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
                if _is_udtf(expr) or (
                    not _is_projection(expr) and expr.parent not in inserted_expressions
                ):
                    return None
            elif not isinstance(edit, Keep):
                return None

        return False

    @property
    def _query_renderer(self) -> QueryRenderer:
        if self.__query_renderer is None:
            self.__query_renderer = QueryRenderer(
                self.query,
                self.dialect,
                self.macro_definitions,
                schema=self.mapping_schema,
                model_name=self.name,
                path=self._path,
                jinja_macro_registry=self.jinja_macros,
                python_env=self.python_env,
                only_execution_time=self.kind.only_execution_time,
            )
        return self.__query_renderer

    @property
    def _data_hash_fields(self) -> t.List[str]:
        data = super()._data_hash_fields

        query = self.query if self.hash_raw_query else self.render_query() or self.query
        data.append(query.sql(comments=False))

        for macro_name, macro in sorted(self.jinja_macros.root_macros.items()):
            data.append(macro_name)
            data.append(macro.definition)

        for _, package in sorted(self.jinja_macros.packages.items(), key=lambda x: x[0]):
            for macro_name, macro in sorted(package.items(), key=lambda x: x[0]):
                data.append(macro_name)
                data.append(macro.definition)

        return data

    def __repr__(self) -> str:
        return f"Model<name: {self.name}, query: {self.query.sql(dialect=self.dialect)[0:30]}>"


class SeedModel(_SqlBasedModel):
    """The model definition which uses a pre-built static dataset to source the data from.

    Args:
        seed: The content of a pre-built static dataset.
    """

    kind: SeedKind
    seed: Seed
    column_hashes_: t.Optional[t.Dict[str, str]] = Field(default=None, alias="column_hashes")
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
    ) -> t.Generator[QueryOrDF, None, None]:
        self._ensure_hydrated()

        date_or_time_columns = []
        bool_columns = []
        string_columns = []
        for name, tpe in (self.columns_to_types_ or {}).items():
            if tpe.this in exp.DataType.TEMPORAL_TYPES:
                date_or_time_columns.append(name)
            elif tpe.is_type("boolean"):
                bool_columns.append(name)
            elif tpe.this in exp.DataType.TEXT_TYPES:
                string_columns.append(name)

        for df in self.seed.read(batch_size=self.kind.batch_size):
            for column in date_or_time_columns:
                df[column] = pd.to_datetime(df[column])
            df[bool_columns] = df[bool_columns].apply(lambda i: str_to_bool(str(i)))
            df[string_columns] = df[string_columns].astype(str)
            yield df

    def text_diff(self, other: Model) -> str:
        if not isinstance(other, SeedModel):
            return super().text_diff(other)

        other._ensure_hydrated()
        self._ensure_hydrated()

        meta_a = self.render_definition()[0]
        meta_b = other.render_definition()[0]
        return "\n".join(
            (
                d.text_diff(meta_a, meta_b, self.dialect),
                *(
                    d.text_diff(pa, pb, self.dialect)
                    for pa, pb in zip_longest(self.pre_statements, other.pre_statements)
                ),
                *(
                    d.text_diff(pa, pb, self.dialect)
                    for pa, pb in zip_longest(self.pre_statements, other.post_statements)
                ),
                *unified_diff(
                    self.seed.content.split("\n"),
                    other.seed.content.split("\n"),
                ),
            )
        ).strip()

    @property
    def columns_to_types(self) -> t.Optional[t.Dict[str, exp.DataType]]:
        if self.columns_to_types_ is not None:
            return self.columns_to_types_
        self._ensure_hydrated()
        return self.seed.columns_to_types

    @property
    def column_hashes(self) -> t.Dict[str, str]:
        if self.column_hashes_ is not None:
            return self.column_hashes_
        self._ensure_hydrated()
        return self.seed.column_hashes

    @property
    def is_seed(self) -> bool:
        return True

    @property
    def seed_path(self) -> Path:
        seed_path = Path(self.kind.path)
        if not seed_path.is_absolute():
            return self._path.parent / seed_path
        return seed_path

    @property
    def depends_on_past(self) -> bool:
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
            update={"seed": Seed(content=content), "is_hydrated": True, "column_hashes_": None}
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

    @property
    def _data_hash_fields(self) -> t.List[str]:
        data = super()._data_hash_fields
        for column_name, column_hash in self.column_hashes.items():
            data.append(column_name)
            data.append(column_hash)
        return data

    def __repr__(self) -> str:
        return f"Model<name: {self.name}, seed: {self.kind.path}>"


class PythonModel(_Model):
    """The model definition which relies on a Python script to fetch the data.

    Args:
        entrypoint: The name of a Python function which contains the data fetching / transformation logic.
    """

    entrypoint: str
    source_type: Literal["python"] = "python"

    def render(
        self,
        *,
        context: ExecutionContext,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        **kwargs: t.Any,
    ) -> t.Generator[QueryOrDF, None, None]:
        env = prepare_env(self.python_env)
        start, end = make_inclusive(start or c.EPOCH, end or c.EPOCH)
        execution_time = to_datetime(execution_time or c.EPOCH)
        try:
            df_or_iter = env[self.entrypoint](
                context=context,
                start=start,
                end=end,
                execution_time=execution_time,
                latest=execution_time,  # TODO: Preserved for backward compatibility. Remove in 1.0.0.
                **kwargs,
            )

            if not isinstance(df_or_iter, types.GeneratorType):
                df_or_iter = [df_or_iter]

            for df in df_or_iter:
                yield df
        except Exception as e:
            print_exception(e, self.python_env)
            raise SQLMeshError(f"Error executing Python model '{self.name}'")

    def render_definition(self, include_python: bool = True) -> t.List[exp.Expression]:
        # Ignore the provided value for the include_python flag, since the Pyhon model's
        # definition without Python code is meaningless.
        return super().render_definition(include_python=True)

    @property
    def is_python(self) -> bool:
        return True

    def is_breaking_change(self, previous: Model) -> t.Optional[bool]:
        return None

    @property
    def _data_hash_fields(self) -> t.List[str]:
        data = super()._data_hash_fields
        data.append(self.entrypoint)
        return data

    def __repr__(self) -> str:
        return f"Model<name: {self.name}, entrypoint: {self.entrypoint}>"


class ExternalModel(_Model):
    """The model definition which represents an external source/table."""

    source_type: Literal["external"] = "external"

    def is_breaking_change(self, previous: Model) -> t.Optional[bool]:
        if not isinstance(previous, ExternalModel):
            return None
        if not previous.columns_to_types_or_raise.items() - self.columns_to_types_or_raise.items():
            return False
        return None


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
        python_env: The custom Python environment for macros. If not provided the environment will be constructed
            from the macro registry.
        dialect: The default dialect if no model dialect is configured.
            The format must adhere to Python's strftime codes.
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

    # Extract the query and any pre/post statements
    query_or_seed_insert, pre_statements, post_statements = _split_sql_model_statements(
        expressions[1:], path
    )

    meta_fields: t.Dict[str, t.Any] = {
        "dialect": dialect,
        "description": "\n".join(comment.strip() for comment in meta.comments)
        if meta.comments
        else None,
        **{prop.name.lower(): prop.args.get("value") for prop in meta.expressions},
        **kwargs,
    }

    name = meta_fields.pop("name", "")
    if not name:
        raise_config_error("Model must have a name", path)

    jinja_macro_references: t.Set[MacroReference] = {
        r
        for references in [
            *[extract_macro_references(e.sql()) for e in pre_statements],
            *[extract_macro_references(e.sql()) for e in post_statements],
        ]
        for r in references
    }

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
        **meta_fields,
    )

    if query_or_seed_insert is not None and isinstance(
        query_or_seed_insert, (exp.Subqueryable, d.JinjaQuery)
    ):
        jinja_macro_references.update(extract_macro_references(query_or_seed_insert.sql()))
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
        except Exception:
            raise_config_error(
                "The model definition must either have a SELECT query, a JINJA_QUERY block, or a valid Seed kind",
                path,
            )
            raise


def create_sql_model(
    name: str,
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
        macros: The custom registry of macros. If not provided the default registry will be used.
        python_env: The custom Python environment for macros. If not provided the environment will be constructed
            from the macro registry.
        dialect: The default dialect if no model dialect is configured.
            The format must adhere to Python's strftime codes.
    """
    if not isinstance(query, (exp.Subqueryable, d.JinjaQuery)):
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
        )

    return _create_model(
        SqlModel,
        name,
        defaults=defaults,
        path=path,
        time_column_format=time_column_format,
        python_env=python_env,
        jinja_macros=jinja_macros,
        jinja_macro_references=jinja_macro_references,
        dialect=dialect,
        query=query,
        pre_statements=pre_statements,
        post_statements=post_statements,
        physical_schema_override=physical_schema_override,
        **kwargs,
    )


def create_seed_model(
    name: str,
    seed_kind: SeedKind,
    *,
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
    **kwargs: t.Any,
) -> Model:
    """Creates a Seed model.

    Args:
        name: The name of the model, which is of the form [catalog].[db].table.
            The catalog and db are optional.
        seed_kind: The information about the location of a seed and other related configuration.
        pre_statements: The list of SQL statements that precede the insertion of the seed's content.
        post_statements: The list of SQL statements that follow after the insertion of the seed's content.
        defaults: Definition default values.
        path: An optional path to the model definition file.
        macros: The custom registry of macros. If not provided the default registry will be used.
        python_env: The custom Python environment for macros. If not provided the environment will be constructed
            from the macro registry.
    """
    seed_path = Path(seed_kind.path)
    if not seed_path.is_absolute():
        seed_path = path / seed_path if path.is_dir() else path.parents[0] / seed_path
    seed = create_seed(seed_path)

    pre_statements = pre_statements or []
    post_statements = post_statements or []

    if not python_env:
        python_env = _python_env(
            [*pre_statements, *post_statements],
            jinja_macro_references,
            module_path,
            macros or macro.get_registry(),
        )

    return _create_model(
        SeedModel,
        name,
        defaults=defaults,
        path=path,
        seed=seed,
        kind=seed_kind,
        depends_on=kwargs.pop("depends_on", set()),
        python_env=python_env,
        jinja_macros=jinja_macros,
        jinja_macro_references=jinja_macro_references,
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
    depends_on = (
        _parse_depends_on(entrypoint, python_env)
        if depends_on is None and python_env is not None
        else None
    )
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
    name: str,
    *,
    dialect: t.Optional[str] = None,
    path: Path = Path(),
    **kwargs: t.Any,
) -> Model:
    """Creates an external model.

    Args:
        name: The name of the model, which is of the form [catalog].[db].table.
            The catalog and db are optional.
        dialect: The dialect to serialize.
        path: An optional path to the model definition file.
    """
    return _create_model(
        ExternalModel,
        name,
        dialect=dialect,
        path=path,
        kind=ModelKindName.EXTERNAL.value,
        **kwargs,
    )


def _create_model(
    klass: t.Type[_Model],
    name: str,
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

    _validate_model_fields(klass, {"name", "physical_schema_override", *kwargs}, path)

    dialect = dialect or ""
    physical_schema_override = physical_schema_override or {}

    jinja_macros = jinja_macros or JinjaMacroRegistry()
    if jinja_macro_references is not None:
        jinja_macros = jinja_macros.trim(jinja_macro_references)

    try:
        model = klass(
            name=name,
            **{
                **(defaults or {}),
                "jinja_macros": jinja_macros,
                "dialect": dialect,
                "depends_on": depends_on,
                "physical_schema_override_map": physical_schema_override,
                **kwargs,
            },
        )
    except Exception as ex:
        raise_config_error(str(ex), location=path)
        raise

    model._path = path
    model.set_time_format(time_column_format)

    return t.cast(Model, model)


INSERT_SEED_MACRO_CALL = d.parse("@INSERT_SEED()")[0]


def _split_sql_model_statements(
    expressions: t.List[exp.Expression], path: Path
) -> t.Tuple[t.Optional[exp.Expression], t.List[exp.Expression], t.List[exp.Expression]]:
    """Extracts the SELECT query from a sequence of expressions.

    Args:
        expressions: The list of all SQL statements in the model definition.

    Returns:
        A tuple containing the extracted SELECT query or the `@INSERT_SEED()` call, the statements before the it, and
        the statements after it.

    Raises:
        ConfigError: If the model definition contains more than one SELECT query or `@INSERT_SEED()` call.
    """
    query_positions = []
    for idx, expression in enumerate(expressions):
        if (
            isinstance(expression, (exp.Subqueryable, d.JinjaQuery))
            or expression == INSERT_SEED_MACRO_CALL
        ):
            query_positions.append((expression, idx))

    if not query_positions:
        return None, expressions, []
    elif len(query_positions) > 1:
        raise_config_error("Only one SELECT query is allowed per model", path)

    query, pos = query_positions[0]
    return query, expressions[:pos], expressions[pos + 1 :]


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
) -> t.Dict[str, Executable]:
    python_env: t.Dict[str, Executable] = {}

    used_macros = {}

    expressions = ensure_list(expressions)
    for expression in expressions:
        if not isinstance(expression, d.Jinja):
            for macro_func in expression.find_all(d.MacroFunc):
                if macro_func.__class__ is d.MacroFunc:
                    name = macro_func.this.name.lower()
                    used_macros[name] = macros[name]

    for macro_ref in jinja_macro_references or set():
        if macro_ref.package is None and macro_ref.name in macros:
            used_macros[macro_ref.name] = macros[macro_ref.name]

    for name, macro in used_macros.items():
        if not macro.func.__module__.startswith("sqlmesh."):
            build_env(
                macro.func,
                env=python_env,
                name=name,
                path=module_path,
            )

    return serialize_env(python_env, path=module_path)


def _parse_depends_on(model_func: str, python_env: t.Dict[str, Executable]) -> t.Set[str]:
    """Parses the source of a model function and finds upstream dependencies based on calls to context."""
    env = prepare_env(python_env)
    depends_on = set()
    executable = python_env[model_func]

    for node in ast.walk(ast.parse(executable.payload)):
        if not isinstance(node, ast.Call):
            continue

        func = node.func

        if (
            isinstance(func, ast.Attribute)
            and isinstance(func.value, ast.Name)
            and func.value.id == "context"
            and func.attr == "table"
        ):
            if node.args:
                table: t.Optional[ast.expr] = node.args[0]
            else:
                table = next(
                    (keyword.value for keyword in node.keywords if keyword.arg == "model_name"),
                    None,
                )

            try:
                expression = to_source(table)
                depends_on.add(eval(expression, env))
            except Exception:
                raise ConfigError(
                    f"Error resolving dependencies for '{executable.path}'. References to context must be resolvable at parse time.\n\n{expression}"
                )

    return depends_on


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
    return isinstance(parent, exp.Select) and expr in parent.expressions


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


META_FIELD_CONVERTER: t.Dict[str, t.Callable] = {
    "name": lambda value: exp.to_table(value),
    "start": lambda value: exp.Literal.string(value),
    "cron": lambda value: exp.Literal.string(value),
    "batch_size": lambda value: exp.Literal.number(value),
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
    "grain": _single_value_or_tuple,
    "hash_raw_query": exp.convert,
}
