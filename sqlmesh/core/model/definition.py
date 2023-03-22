from __future__ import annotations

import ast
import sys
import types
import typing as t
from difflib import unified_diff
from itertools import zip_longest
from pathlib import Path

import numpy as np
import pandas as pd
from astor import to_source
from pandas.core.dtypes.common import is_numeric_dtype
from pydantic import Field
from sqlglot import exp
from sqlglot.diff import ChangeDistiller, Insert, Keep
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.schema import MappingSchema
from sqlglot.time import format_time

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.engine_adapter import PySparkDataFrame
from sqlmesh.core.hooks import HookRegistry, hook
from sqlmesh.core.macros import MacroEvaluator, MacroRegistry, macro
from sqlmesh.core.model.common import expression_validator, parse_model_name
from sqlmesh.core.model.kind import SeedKind
from sqlmesh.core.model.meta import HookCall, ModelMeta
from sqlmesh.core.model.seed import Seed, create_seed
from sqlmesh.core.renderer import ExpressionRenderer, QueryRenderer
from sqlmesh.utils.date import TimeLike, make_inclusive, to_datetime
from sqlmesh.utils.errors import ConfigError, SQLMeshError, raise_config_error
from sqlmesh.utils.jinja import JinjaMacroRegistry
from sqlmesh.utils.metaprogramming import (
    Executable,
    build_env,
    prepare_env,
    print_exception,
    serialize_env,
)
from sqlmesh.utils.pandas import filter_df_by_timelike

if t.TYPE_CHECKING:
    from sqlmesh.core.audit import Audit
    from sqlmesh.core.context import ExecutionContext
    from sqlmesh.core.engine_adapter import EngineAdapter
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF
    from sqlmesh.core.snapshot import Snapshot

if sys.version_info >= (3, 9):
    from typing import Annotated, Literal
else:
    from typing_extensions import Annotated, Literal


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
        batch_size: The maximum number of intervals that can be run per backfill job. If this is None,
            then backfilling this model will do all of history in one job. If this is set, a model's backfill
            will be chunked such that each individual job will only contain jobs with max `batch_size` intervals.
        storage_format: The storage format used to store the physical table, only applicable in certain engines.
            (eg. 'parquet')
        partitioned_by: The partition columns, only applicable in certain engines. (eg. (ds, hour))
        pre: Pre-hooks to run before the model executes.
        post: Post-hooks to run after the model executes.
        expressions: All of the expressions between the model definition and final query, used for setting certain variables or environments.
        python_env: Dictionary containing all global variables needed to render the model's macros.
    """

    expressions_: t.Optional[t.List[exp.Expression]] = Field(default=None, alias="expressions")
    python_env_: t.Optional[t.Dict[str, Executable]] = Field(default=None, alias="python_env")
    jinja_macros: JinjaMacroRegistry = JinjaMacroRegistry()

    _path: Path = Path()
    _depends_on: t.Optional[t.Set[str]] = None
    _column_descriptions: t.Optional[t.Dict[str, str]] = None

    _expressions_validator = expression_validator

    def render(
        self,
        context: ExecutionContext,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        engine_adapter: t.Optional[EngineAdapter] = None,
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
            latest: The latest date/time to use for the run.

        Returns:
            A generator which yields eiether a query object or one of the supported dataframe objects.
        """
        yield self.render_query(
            start=start,
            end=end,
            latest=latest,
            snapshots=context.snapshots,
            is_dev=context.is_dev,
            engine_adapter=engine_adapter,
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
                else:
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
            *self.expressions,
            *python_expressions,
        ]

    def render_query(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        is_dev: bool = False,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> exp.Subqueryable:
        """Renders a model's query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental queries. Defaults to epoch start.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.
            audit_name: The name of audit if the query to render is for an audit.
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        return exp.select(
            *(
                exp.alias_(f"NULL::{column_type}", name)
                for name, column_type in self.columns_to_types.items()
            )
        ).from_(exp.values([tuple([1])], alias="t", columns=["dummy"]))

    def ctas_query(
        self, snapshots: t.Dict[str, Snapshot], is_dev: bool = False
    ) -> exp.Subqueryable:
        """Return a dummy query to do a CTAS.

        If a model's column types are unknown, the only way to create the table is to
        run the fully expanded query. This can be expensive so we add a WHERE FALSE to all
        SELECTS and hopefully the optimizer is smart enough to not do anything.

        Args:
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            is_dev: Indicates whether the creation happens in the development mode and temporary
                tables / table clones should be used where applicable.
        Return:
            The mocked out ctas query.
        """
        query = self.render_query(snapshots=snapshots, is_dev=is_dev)
        # the query is expanded so it's been copied, it's safe to mutate.
        for select in query.find_all(exp.Select):
            select.where("FALSE", copy=False)

        return query

    def run_pre_hooks(
        self,
        context: ExecutionContext,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        **kwargs: t.Any,
    ) -> None:
        """Runs all pre hooks.

        Args:
            context: The execution context used for running the hook.
            start: The start date/time of the run.
            end: The end date/time of the run.
            latest: The latest date/time to use for the run.
        """
        self._run_hooks(self.pre, context=context, start=start, end=end, latest=latest, **kwargs)

    def run_post_hooks(
        self,
        context: ExecutionContext,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        **kwargs: t.Any,
    ) -> None:
        """Runs all pre hooks.

        Args:
            context: The execution context used for running the hook.
            start: The start date/time of the run.
            end: The end date/time of the run.
            latest: The latest date/time to use for the run.
        """
        self._run_hooks(self.post, context=context, start=start, end=end, latest=latest, **kwargs)

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

    def update_schema(self, schema: MappingSchema) -> None:
        """Updates the schema associated with this model.

        Args:
            schema: The new schema.
        """

    def text_diff(self, other: Model) -> str:
        """Produce a text diff against another model.

        Args:
            other: The model to diff against.

        Returns:
            A unified text diff showing additions and deletions.
        """
        meta_a, *statements_a, query_a = self.render_definition()
        meta_b, *statements_b, query_b = other.render_definition()
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
                d.Dialect.get_or_raise(self.dialect).time_mapping,
            )
            assert formatted_time is not None
            self.time_column.format = formatted_time
        else:
            self.time_column.format = default_time_format

    def convert_to_time_column(self, time: TimeLike) -> exp.Expression:
        """Convert a TimeLike object to the same time format and type as the model's time column."""
        if self.time_column:
            if self.time_column.format:
                time = to_datetime(time).strftime(self.time_column.format)

            time_column_type = self.columns_to_types[self.time_column.column]
            if time_column_type.this in exp.DataType.TEXT_TYPES:
                return exp.Literal.string(time)
            elif time_column_type.this in exp.DataType.NUMERIC_TYPES:
                return exp.Literal.number(time)
            elif time_column_type.this in exp.DataType.TEMPORAL_TYPES:
                return exp.cast(exp.Literal.string(time), time_column_type)
        return exp.convert(time)

    @property
    def depends_on(self) -> t.Set[str]:
        """All of the upstream dependencies referenced in the model's query, excluding self references.

        Returns:
            A list of all the upstream table names.
        """
        if self.depends_on_ is not None:
            return self.depends_on_

        if self._depends_on is None:
            self._depends_on = _find_tables(self.render_query()) - {self.name}
        return self._depends_on

    @property
    def columns_to_types(self) -> t.Dict[str, exp.DataType]:
        """Returns the mapping of column names to types of this model."""
        if self.columns_to_types_ is not None:
            return self.columns_to_types_
        raise SQLMeshError(f"Column information has not been provided for model '{self.name}'")

    @property
    def annotated(self) -> bool:
        """Checks if all column projection types of this model are known."""
        return all(
            column_type.this != exp.DataType.Type.UNKNOWN
            for column_type in self.columns_to_types.values()
        )

    @property
    def sorted_python_env(self) -> t.List[t.Tuple[str, Executable]]:
        """Returns the python env sorted by executable kind and then var name."""
        return sorted(self.python_env.items(), key=lambda x: (x[1].kind, x[0]))

    @property
    def macro_definitions(self) -> t.List[d.MacroDef]:
        """All macro definitions from the list of expressions."""
        return [s for s in self.expressions if isinstance(s, d.MacroDef)]

    @property
    def sql_statements(self) -> t.Iterator[exp.Expression]:
        """All sql statements from the list of expressions."""
        rendered_statements = (
            self._expression_renderer(s).render()
            for s in self.expressions
            if not isinstance(s, d.MacroDef)
        )
        return (statement for statement in rendered_statements if statement is not None)

    @property
    def view_name(self) -> str:
        return parse_model_name(self.name)[2]

    @property
    def expressions(self) -> t.List[exp.Expression]:
        return self.expressions_ or []

    @property
    def python_env(self) -> t.Dict[str, Executable]:
        return self.python_env_ or {}

    @property
    def contains_star_query(self) -> bool:
        """Returns True if the model's query contains a star projection."""
        return False

    @property
    def is_sql(self) -> bool:
        return False

    @property
    def is_python(self) -> bool:
        return False

    @property
    def is_seed(self) -> bool:
        return False

    def validate_definition(self) -> None:
        """Validates the model's definition.

        Model's are not allowed to have duplicate column names, non-explicitly casted columns,
        or non infererrable column names.

        Raises:
            ConfigError
        """
        if self.partitioned_by:
            unique_partition_keys = {k.strip().lower() for k in self.partitioned_by}
            if len(self.partitioned_by) != len(unique_partition_keys):
                raise_config_error(
                    "All partition keys must be unique in the model definition",
                    self._path,
                )

            column_names = {c.lower() for c in self.columns_to_types}
            missing_keys = unique_partition_keys - column_names
            if missing_keys:
                missing_keys_str = ", ".join(f"'{k}'" for k in sorted(missing_keys))
                raise_config_error(
                    f"Partition keys [{missing_keys_str}] are missing in the model definition",
                    self._path,
                )

        if self.kind.is_incremental_by_time_range and not self.time_column:
            raise_config_error(
                "Incremental by time range models must have a time_column field.",
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
        return None

    def _run_hooks(
        self,
        hooks: t.List[HookCall],
        *,
        context: ExecutionContext,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        **kwargs: t.Any,
    ) -> None:
        env = prepare_env(self.python_env)
        start, end = make_inclusive(start or c.EPOCH, end or c.EPOCH)
        latest = to_datetime(latest or c.EPOCH)

        macro_evaluator = MacroEvaluator()

        for hook in hooks:
            if isinstance(hook, exp.Expression):
                rendered = self._expression_renderer(hook).render(
                    start=start,
                    end=end,
                    latest=latest,
                    engine_adapter=context.engine_adapter,
                    **kwargs,
                )
                if rendered:
                    context.engine_adapter.execute(rendered)
            else:
                name, hook_kwargs = hook
                # Evaluate SQL expressions before passing them into a Python
                # function as arguments.
                evaluated_hook_kwargs = {
                    key: macro_evaluator.eval_expression(value)
                    if isinstance(value, exp.Expression)
                    else value
                    for key, value in hook_kwargs.items()
                }
                env[name](
                    context=context,
                    start=start,
                    end=end,
                    latest=latest,
                    **{**kwargs, **evaluated_hook_kwargs},
                )

    def _expression_renderer(self, expression: exp.Expression) -> ExpressionRenderer:
        return ExpressionRenderer(
            expression,
            self.dialect,
            self.macro_definitions,
            path=self._path,
            jinja_macro_registry=self.jinja_macros,
            python_env=self.python_env,
        )


class SqlModel(_Model):
    """The model definition which relies on a SQL query to fetch the data.

    Args:
        query: The main query representing the model.
    """

    query: t.Union[exp.Subqueryable, d.Jinja]
    source_type: Literal["sql"] = "sql"

    _columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None
    __query_renderer: t.Optional[QueryRenderer] = None

    _query_validator = expression_validator

    def render_query(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        is_dev: bool = False,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> exp.Subqueryable:
        return self._query_renderer.render(
            start=start,
            end=end,
            latest=latest,
            add_incremental_filter=True,
            snapshots=snapshots,
            expand=expand,
            is_dev=is_dev,
            engine_adapter=engine_adapter,
            **kwargs,
        )

    def render_definition(self, include_python: bool = True) -> t.List[exp.Expression]:
        result = super().render_definition(include_python=include_python)
        result.append(self.query)
        return result

    @property
    def is_sql(self) -> bool:
        return True

    @property
    def contains_star_query(self) -> bool:
        return self._query_renderer.contains_star_query

    def update_schema(self, schema: MappingSchema) -> None:
        self._query_renderer.update_schema(schema)

    @property
    def columns_to_types(self) -> t.Dict[str, exp.DataType]:
        if self.columns_to_types_ is not None:
            return self.columns_to_types_

        if self._columns_to_types is None:
            self._columns_to_types = {
                expression.alias_or_name: expression.type
                for expression in self._query_renderer.render().expressions
            }

        return self._columns_to_types

    @property
    def column_descriptions(self) -> t.Dict[str, str]:
        if self.column_descriptions_ is not None:
            return self.column_descriptions_

        if self._column_descriptions is None:
            self._column_descriptions = {
                select.alias: "\n".join(comment.strip() for comment in select.comments)
                for select in self.render_query().expressions
                if select.comments
            }
        return self._column_descriptions

    def validate_definition(self) -> None:
        query = self._query_renderer.render()

        if not isinstance(query, exp.Subqueryable):
            raise_config_error("Missing SELECT query in the model definition", self._path)

        projection_list = (
            query.expressions if not isinstance(query, exp.Union) else query.this.expressions
        )
        if not projection_list:
            raise_config_error("Query missing select statements", self._path)

        name_counts: t.Dict[str, int] = {}
        for expression in projection_list:
            alias = expression.alias_or_name
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

        edits = ChangeDistiller(t=0.5).diff(previous.render_query(), self.render_query())
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
                path=self._path,
                jinja_macro_registry=self.jinja_macros,
                python_env=self.python_env,
                time_column=self.time_column,
                time_converter=self.convert_to_time_column,
                only_latest=self.kind.only_latest,
            )
        return self.__query_renderer

    def __repr__(self) -> str:
        return f"Model<name: {self.name}, query: {str(self.query)[0:30]}>"


class SeedModel(_Model):
    """The model definition which uses a pre-built static dataset to source the data from.

    Args:
        seed: The content of a pre-built static dataset.
    """

    kind: SeedKind
    seed: Seed
    source_type: Literal["seed"] = "seed"

    def render(
        self,
        context: ExecutionContext,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> t.Generator[QueryOrDF, None, None]:
        yield from self.seed.read(batch_size=self.kind.batch_size)

    def text_diff(self, other: Model) -> str:
        if not isinstance(other, SeedModel):
            return super().text_diff(other)

        meta_a = self.render_definition()[0]
        meta_b = other.render_definition()[0]
        return "\n".join(
            (
                d.text_diff(meta_a, meta_b, self.dialect),
                *unified_diff(
                    self.seed.content.split("\n"),
                    other.seed.content.split("\n"),
                ),
            )
        ).strip()

    @property
    def columns_to_types(self) -> t.Dict[str, exp.DataType]:
        if self.columns_to_types_ is not None:
            return self.columns_to_types_
        return self.seed.columns_to_types

    @property
    def is_seed(self) -> bool:
        return True

    @property
    def seed_path(self) -> Path:
        seed_path = Path(self.kind.path)
        if not seed_path.is_absolute():
            return self._path.parent / seed_path
        return seed_path

    def is_breaking_change(self, previous: Model) -> t.Optional[bool]:
        if not isinstance(previous, SeedModel):
            return None

        new_df = pd.concat([df for df in self.seed.read()])
        old_df = pd.concat([df for df in previous.seed.read()])

        new_columns = set(new_df.columns)
        old_columns = set(old_df.columns)

        if not new_columns.issuperset(old_columns):
            return None

        for col in old_columns:
            if new_df[col].dtype != old_df[col].dtype or new_df[col].shape != old_df[col].shape:
                return None
            elif is_numeric_dtype(new_df[col]):
                if not all(np.isclose(new_df[col], old_df[col])):
                    return None
            else:
                if not new_df[col].equals(old_df[col]):
                    return None

        return False

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
        context: ExecutionContext,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        engine_adapter: t.Optional[EngineAdapter] = None,
        **kwargs: t.Any,
    ) -> t.Generator[DF, None, None]:
        env = prepare_env(self.python_env)
        start, end = make_inclusive(start or c.EPOCH, end or c.EPOCH)
        latest = to_datetime(latest or c.EPOCH)
        try:
            df_or_iter = env[self.entrypoint](
                context=context, start=start, end=end, latest=latest, **kwargs
            )

            if not isinstance(df_or_iter, types.GeneratorType):
                df_or_iter = [df_or_iter]

            for df in df_or_iter:
                if self.kind.is_incremental_by_time_range:
                    assert self.time_column

                    if PySparkDataFrame is not None and isinstance(df, PySparkDataFrame):
                        import pyspark

                        df = df.where(
                            pyspark.sql.functions.col(self.time_column.column).between(
                                pyspark.sql.functions.lit(
                                    self.convert_to_time_column(start).sql("spark")
                                ),
                                pyspark.sql.functions.lit(
                                    self.convert_to_time_column(end).sql("spark")
                                ),
                            )
                        )
                    else:
                        assert self.time_column.format, "Time column format is required."
                        df = filter_df_by_timelike(
                            df, self.time_column.column, self.time_column.format, start, end
                        )
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

    def __repr__(self) -> str:
        return f"Model<name: {self.name}, entrypoint: {self.entrypoint}>"


Model = Annotated[t.Union[SqlModel, SeedModel, PythonModel], Field(discriminator="source_type")]


def load_model(
    expressions: t.List[exp.Expression],
    *,
    defaults: t.Optional[t.Dict[str, t.Any]] = None,
    path: Path = Path(),
    module_path: Path = Path(),
    time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
    macros: t.Optional[MacroRegistry] = None,
    hooks: t.Optional[HookRegistry] = None,
    python_env: t.Optional[t.Dict[str, Executable]] = None,
    dialect: t.Optional[str] = None,
    **kwargs: t.Any,
) -> Model:
    """Load a model from a parsed SQLMesh model file.

    Args:
        expressions: Model, *Statements, Query.
        defaults: Definition default values.
        path: An optional path to the model definition file.
        module_path: The python module path to serialize macros for.
        time_column_format: The default time column format to use if no model time column is configured.
        macros: The custom registry of macros. If not provided the default registry will be used.
        hooks: The custom registry of hooks. If not provided the default registry will be used.
        python_env: The custom Python environment for hooks/macros. If not provided the environment will be constructed
            from the macro registry.
        dialect: The default dialect if no model dialect is configured.
            The format must adhere to Python's strftime codes.
        kwargs: Additional kwargs to pass to the loader.
    """
    if not expressions:
        raise_config_error("Incomplete model definition, missing MODEL statement", path)

    dialect = dialect or ""
    meta = expressions[0]
    query = expressions[-1] if len(expressions) > 1 else None
    statements = expressions[1:-1]

    if not isinstance(meta, d.Model):
        raise_config_error(
            "MODEL statement is required as the first statement in the definition",
            path,
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

    if isinstance(query, d.MacroVar):
        if python_env is None:
            raise_config_error("The python environment must be provided for Python models", path)
            raise

        return create_python_model(
            name,
            query.name,
            python_env,
            defaults=defaults,
            path=path,
            time_column_format=time_column_format,
            **meta_fields,
        )
    elif query is not None:
        return create_sql_model(
            name,
            query,
            statements=statements,
            defaults=defaults,
            path=path,
            module_path=module_path,
            time_column_format=time_column_format,
            macros=macros,
            hooks=hooks,
            python_env=python_env,
            **meta_fields,
        )
    else:
        try:
            seed_properties = {
                p.name.lower(): p.args.get("value") for p in meta_fields.pop("kind").expressions
            }
            return create_seed_model(
                name,
                SeedKind(**seed_properties),
                defaults=defaults,
                path=path,
                **meta_fields,
            )
        except Exception:
            raise_config_error(
                "The model definition must either have a SELECT query or a valid Seed kind",
                path,
            )
            raise


def create_sql_model(
    name: str,
    query: exp.Expression,
    *,
    statements: t.Optional[t.List[exp.Expression]] = None,
    defaults: t.Optional[t.Dict[str, t.Any]] = None,
    path: Path = Path(),
    module_path: Path = Path(),
    time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
    macros: t.Optional[MacroRegistry] = None,
    hooks: t.Optional[HookRegistry] = None,
    python_env: t.Optional[t.Dict[str, Executable]] = None,
    dialect: t.Optional[str] = None,
    **kwargs: t.Any,
) -> Model:
    """Creates a SQL model.

    Args:
        name: The name of the model, which is of the form [catalog].[db].table.
            The catalog and db are optional.
        query: The model's logic in a form of a SELECT query.
        statements: The list of all SQL statements that are not a query or a model definition.
        defaults: Definition default values.
        path: An optional path to the model definition file.
        module_path: The python module path to serialize macros for.
        time_column_format: The default time column format to use if no model time column is configured.
        macros: The custom registry of macros. If not provided the default registry will be used.
        hooks: The custom registry of hooks. If not provided the default registry will be used.
        python_env: The custom Python environment for hooks/macros. If not provided the environment will be constructed
            from the macro registry.
        dialect: The default dialect if no model dialect is configured.
            The format must adhere to Python's strftime codes.
    """
    if not isinstance(query, (exp.Subqueryable, d.Jinja)):
        raise_config_error(
            "A query is required and must be a SELECT or UNION statement.",
            path,
        )

    if not python_env:
        python_env = _python_env(
            query,
            _extract_hooks(kwargs),
            module_path,
            macros or macro.get_registry(),
            hooks or hook.get_registry(),
        )

    return _create_model(
        SqlModel,
        name,
        defaults=defaults,
        path=path,
        time_column_format=time_column_format,
        python_env=python_env,
        dialect=dialect,
        expressions=statements or [],
        query=query,
        **kwargs,
    )


def create_seed_model(
    name: str,
    seed_kind: SeedKind,
    *,
    defaults: t.Optional[t.Dict[str, t.Any]] = None,
    path: Path = Path(),
    **kwargs: t.Any,
) -> Model:
    """Creates a Seed model.

    Args:
        name: The name of the model, which is of the form [catalog].[db].table.
            The catalog and db are optional.
        seed_kind: The information about the location of a seed and other related configuration.
        defaults: Definition default values.
        path: An optional path to the model definition file.
    """
    seed_path = Path(seed_kind.path)
    if not seed_path.is_absolute():
        seed_path = path / seed_path if path.is_dir() else path.parents[0] / seed_path
    seed = create_seed(seed_path)
    return _create_model(
        SeedModel,
        name,
        defaults=defaults,
        path=path,
        seed=seed,
        kind=seed_kind,
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
        **kwargs,
    )


def _create_model(
    klass: t.Type[_Model],
    name: str,
    *,
    defaults: t.Optional[t.Dict[str, t.Any]] = None,
    path: Path = Path(),
    time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
    depends_on: t.Optional[t.Set[str]] = None,
    dialect: t.Optional[str] = None,
    expressions: t.Optional[t.List[exp.Expression]] = None,
    **kwargs: t.Any,
) -> Model:
    _validate_model_fields(klass, {"name", *kwargs}, path)

    dialect = dialect or ""

    try:
        model = klass(
            name=name,
            expressions=expressions or [],
            **{
                **(defaults or {}),
                "dialect": dialect,
                "depends_on": depends_on,
                **kwargs,
            },
        )
    except Exception as ex:
        raise_config_error(str(ex), location=path)
        raise

    model._path = path
    model.set_time_format(time_column_format)
    model.validate_definition()

    return t.cast(Model, model)


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


def _find_tables(query: exp.Expression) -> t.Set[str]:
    """Find all tables referenced in a query.

    Args:
        query: The expression to find tables for.

    Returns:
        A Set of all the table names.
    """
    return {
        exp.table_name(table)
        for scope in traverse_scope(query)
        for table in scope.tables
        if isinstance(table.this, exp.Identifier) and exp.table_name(table) not in scope.cte_sources
    }


def _python_env(
    query: exp.Expression,
    hook_calls: t.List[HookCall],
    module_path: Path,
    macros: MacroRegistry,
    hooks: HookRegistry,
) -> t.Dict[str, Executable]:
    python_env: t.Dict[str, Executable] = {}

    used_macros = {}

    def _capture_expression_macros(expression: exp.Expression) -> None:
        if isinstance(expression, d.Jinja):
            for var in expression.expressions:
                if var in macros:
                    used_macros[var] = macros[var]
        else:
            for macro_func in expression.find_all(d.MacroFunc):
                if macro_func.__class__ is d.MacroFunc:
                    name = macro_func.this.name.lower()
                    used_macros[name] = macros[name]

    _capture_expression_macros(query)

    for hook in hook_calls:
        if isinstance(hook, exp.Expression):
            _capture_expression_macros(hook)
        else:
            name = hook[0]
            build_env(
                hooks[name].func,
                env=python_env,
                name=name,
                path=module_path,
            )

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


def _extract_hooks(kwargs: t.Dict[str, t.Any]) -> t.List[HookCall]:
    return (ModelMeta._value_or_tuple_with_args_validator(kwargs.get("pre")) or []) + (
        ModelMeta._value_or_tuple_with_args_validator(kwargs.get("post")) or []
    )


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


META_FIELD_CONVERTER: t.Dict[str, t.Callable] = {
    "name": lambda value: exp.to_table(value),
    "start": lambda value: exp.Literal.string(value),
    "cron": lambda value: exp.Literal.string(value),
    "batch_size": lambda value: exp.Literal.number(value),
    "partitioned_by_": lambda value: (
        exp.to_identifier(value[0]) if len(value) == 1 else exp.Tuple(expressions=value)
    ),
    "depends_on_": lambda value: exp.Tuple(expressions=value),
    "pre": _list_of_calls_to_exp,
    "post": _list_of_calls_to_exp,
    "audits": _list_of_calls_to_exp,
    "columns_to_types_": lambda value: exp.Schema(
        expressions=[exp.ColumnDef(this=exp.to_column(c), kind=t) for c, t in value.items()]
    ),
}
