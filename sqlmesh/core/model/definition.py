from __future__ import annotations

import ast
import types
import typing as t
from datetime import datetime
from itertools import zip_longest
from pathlib import Path

from astor import to_source
from jinja2 import Environment
from pydantic import Field, validator
from pydantic.fields import FieldInfo
from sqlglot import exp, maybe_parse, parse_one
from sqlglot.errors import SqlglotError
from sqlglot.optimizer import optimize
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.optimizer.simplify import simplify
from sqlglot.schema import MappingSchema
from sqlglot.time import format_time

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.audit import Audit
from sqlmesh.core.engine_adapter import PySparkDataFrame
from sqlmesh.core.macros import MacroEvaluator, MacroRegistry, macro
from sqlmesh.core.model.common import parse_model_name
from sqlmesh.core.model.meta import ModelMeta
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.date import TimeLike, date_dict, make_inclusive, to_datetime
from sqlmesh.utils.errors import ConfigError, MacroEvalError, SQLMeshError
from sqlmesh.utils.metaprogramming import (
    Executable,
    build_env,
    prepare_env,
    print_exception,
    serialize_env,
)

if t.TYPE_CHECKING:
    from sqlmesh.core.context import ExecutionContext
    from sqlmesh.core.engine_adapter import DF
    from sqlmesh.core.snapshot import Snapshot

META_FIELD_CONVERTER: t.Dict[str, t.Callable] = {
    "name": lambda value: exp.to_table(value),
    "start": lambda value: exp.Literal.string(value),
    "cron": lambda value: exp.Literal.string(value),
    "batch_size": lambda value: exp.Literal.number(value),
    "partitioned_by_": lambda value: (
        exp.to_identifier(value[0]) if len(value) == 1 else exp.Tuple(expressions=value)
    ),
    "depends_on_": lambda value: exp.Tuple(expressions=value),
    "columns_to_types_": lambda value: exp.Schema(
        expressions=[
            exp.ColumnDef(this=exp.to_column(c), kind=t) for c, t in value.items()
        ]
    ),
}

EPOCH_DS = "1970-01-01"

RENDER_OPTIMIZER_RULES = (qualify_tables, qualify_columns, annotate_types)


class Model(ModelMeta, frozen=True):
    """Model is the core abstraction for user defined SQL logic.

    A model represents a single SQL query and metadata surrounding it. Models can be
    run on arbitrary cadences and can be incremental or full refreshes. Models can also
    be materialized into physical tables or shared across other models as temporary views.

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
        cron: A cron string specifying how often the model should be refresh, leveraging the
            [croniter](https://github.com/kiorky/croniter) library.
        description: The optional model description.
        stamp: An optional arbitrary string sequence used to create new model versions without making
            changes to any of the functional components of the definition.
        start: The earliest date that the model will be backfilled for. If this is None,
            then the date is inferred by taking the most recent start date's of its ancestors.
            The start date can be a static datetime or a realtive datetime like "1 year ago"
        batch_size: The maximum number of intervals that can be run per backfill job. If this is None,
            then backfilling this model will do all of history in one job. If this is set, a model's backfill
            will be chunked such that each individual job will only contain jobs with max `batch_size` intervals.
        storage_format: The storage format used to store the physical table, only applicable in certain engines.
            (eg. 'parquet')
        partitioned_by: The partition columns, only applicable in certain engines. (eg. (ds, hour))
        query: The main query representing the model.
        expressions: All of the expressions between the model definition and final query, used for setting certain variables or environments.
        python_env: Dictionary containing all global variables needed to render the model's macros.
    """

    query: t.Union[exp.Subqueryable, d.MacroVar, d.Jinja]
    expressions_: t.Optional[t.List[exp.Expression]] = Field(
        default=None, alias="expressions"
    )
    python_env_: t.Optional[t.Dict[str, Executable]] = Field(
        default=None, alias="python_env"
    )
    _path: Path = Path()
    _depends_on: t.Optional[t.Set[str]] = None
    _columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = Field(
        default=None, alias="columns"
    )
    _column_descriptions: t.Optional[t.Dict[str, str]] = None
    _query_cache: t.Dict[
        t.Tuple[str, datetime, datetime, datetime], exp.Subqueryable
    ] = {}
    _schema: t.Optional[MappingSchema] = None
    audits: t.Dict[str, Audit] = UniqueKeyDict("audits")

    @validator("query", "expressions_", pre=True)
    def _parse_expression(
        cls,
        v: t.Union[
            t.List[str], t.List[exp.Expression], str, exp.Expression, t.Callable
        ],
    ) -> t.List[exp.Expression] | exp.Expression | t.Callable:
        """Helper method to deserialize SQLGlot expressions in Pydantic Models."""
        if callable(v):
            return v

        if isinstance(v, list):
            return [e for e in (maybe_parse(i) for i in v) if e]
        expression = maybe_parse(v)
        if not expression:
            raise ConfigError(f"Could not parse {v}")
        return expression

    @classmethod
    def load(
        cls,
        expressions: t.List[exp.Expression],
        *,
        path: Path = Path(),
        module_path: Path = Path(),
        time_column_format: str = c.DEFAULT_TIME_COLUMN_FORMAT,
        macros: t.Optional[MacroRegistry] = None,
        python_env: t.Optional[t.Dict[str, Executable]] = None,
        dialect: t.Optional[str] = None,
        **kwargs,
    ) -> Model:
        """Load a model from a parsed SQLMesh model file.

        Args:
            expressions: Model, *Statements, Query.
            module_path: The python module path to serialize macros for.
            path: An optional path to the file.
            dialect: The default dialect if no model dialect is configured.
            time_column_format: The default time column format to use if no model time column is configured.
                The format must adhere to Python's strftime codes.
        """
        if len(expressions) < 2:
            _raise_config_error(
                "Incomplete model definition, missing MODEL and QUERY", path
            )

        meta, *statements, query = expressions

        if not isinstance(meta, d.Model):
            _raise_config_error(
                "MODEL statement is required as the first statement in the definition",
                path,
            )

        provided_meta_fields = {p.name for p in meta.expressions}

        missing_required_fields = ModelMeta.missing_required_fields(
            provided_meta_fields
        )
        if missing_required_fields:
            _raise_config_error(
                f"Missing required fields {missing_required_fields} in the model definition",
                path,
            )

        extra_fields = ModelMeta.extra_fields(provided_meta_fields)
        if extra_fields:
            _raise_config_error(
                f"Invalid extra fields {extra_fields} in the model definition", path
            )

        if not isinstance(query, (exp.Subqueryable, d.MacroVar, d.Jinja)):
            _raise_config_error(
                "A query is required and must be a SELECT or UNION statement.",
                path,
            )

        if not query.expressions and not isinstance(query, d.MacroVar):
            _raise_config_error("Query missing select statements", path)

        if not python_env:
            python_env = _python_env(query, module_path, macros or macro.get_registry())

        try:
            model_meta = ModelMeta(
                **{
                    prop.name.lower(): prop.args.get("value")
                    for prop in meta.expressions
                },
                description="\n".join(comment.strip() for comment in meta.comments)
                if meta.comments
                else None,
                **kwargs,
            )

            # find dependencies for python models by parsing code if they are not explicitly defined
            if model_meta.depends_on_ is None and isinstance(query, d.MacroVar):
                depends_on = _parse_depends_on(query, python_env)
            else:
                depends_on = None

            model = cls(
                query=query,
                expressions=statements,
                python_env=python_env,
                **{
                    "dialect": dialect or "",
                    "depends_on": depends_on,
                    **model_meta.dict(),
                },
            )
        except Exception as ex:
            _raise_config_error(str(ex), location=path)

        model._path = path
        model.set_time_format(time_column_format)
        model.validate_definition()

        return model

    @property
    def depends_on(self) -> t.Set[str]:
        """All of the upstream dependencies referenced in the model's query, excluding self references.

        Returns:
            A list of all the upstream table names.
        """
        if self.depends_on_ is not None:
            return self.depends_on_

        if self._depends_on is None:
            self._depends_on = _find_tables(self._render_query()) - {self.name}
        return self._depends_on

    @property
    def column_descriptions(self) -> t.Dict[str, str]:
        """A dictionary of column names to annotation comments."""
        if self._column_descriptions is None:
            self._column_descriptions = {
                select.alias: "\n".join(comment.strip() for comment in select.comments)
                for select in self._render_query().expressions
                if select.comments
            }
        return self._column_descriptions

    @property
    def columns_to_types(self) -> t.Dict[str, exp.DataType]:
        """Returns the mapping of column names to types of this model."""
        if self.columns_to_types_:
            return self.columns_to_types_

        if self._columns_to_types is None or isinstance(
            self._columns_to_types, FieldInfo
        ):
            query = annotate_types(self._render_query())
            self._columns_to_types = {
                expression.alias_or_name: expression.type
                for expression in query.expressions
            }

        return self._columns_to_types

    @property
    def contains_star_query(self) -> bool:
        """Returns True if the model's query contains a star projection."""
        return any(
            isinstance(expression, exp.Star)
            for expression in self.render_query().expressions
        )

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
    def is_sql(self) -> bool:
        return not isinstance(self.query, d.MacroVar)

    def render(self, show_python: bool = False) -> t.List[exp.Expression]:
        """Returns the original list of sql expressions comprising the model.

        Args:
            show_python: Whether or not to show Python Code in the rendered model for SQL based models.
        """
        expressions = []
        comment = None
        for field in ModelMeta.__fields__.values():
            field_value = getattr(self, field.name)

            if field_value is not None:
                if field.name == "description":
                    comment = field_value
                elif field.name == "kind":
                    expressions.append(
                        exp.Property(
                            this="kind",
                            value=field_value.to_expression(self.dialect),
                        )
                    )
                else:
                    expressions.append(
                        exp.Property(
                            this=field.alias or field.name,
                            value=META_FIELD_CONVERTER.get(
                                field.name, exp.to_identifier
                            )(field_value),
                        )
                    )

        model = d.Model(expressions=expressions)
        model.comments = [comment] if comment else None

        segments = [model, *self.expressions]

        if show_python or not self.is_sql:
            python_env = d.PythonCode(
                expressions=[
                    v.payload
                    if v.is_import or v.is_definition
                    else f"{k} = {v.payload}"
                    for k, v in self.sorted_python_env
                ]
            )

            if python_env.expressions:
                segments.append(python_env)

        if self.is_sql:
            segments.append(self.query)

        return segments

    def update_schema(self, schema: MappingSchema) -> None:
        self._schema = schema

        if self.contains_star_query:
            # We need to re-render in order to expand the star projection
            self._query_cache.clear()
            self._render_query()

    def _render_query(
        self,
        query_: t.Optional[exp.Expression] = None,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        add_incremental_filter: bool = False,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        audit_name: t.Optional[str] = None,
        dialect: t.Optional[str] = None,
        is_dev: bool = False,
        **kwargs,
    ) -> exp.Subqueryable:
        """Renders a query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            query_: The query to render, defaults to self.query.
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental queries. Defaults to epoch start.
            add_incremental_filter: Add an incremental filter to the query if the model is incremental.
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
        from sqlmesh.core.snapshot import to_table_mapping

        dates = (
            *make_inclusive(start or EPOCH_DS, end or EPOCH_DS),
            to_datetime(latest or EPOCH_DS),
        )
        key = (audit_name or "", *dates)

        snapshots = snapshots or {}
        mapping = to_table_mapping(snapshots.values(), is_dev)
        # if a snapshot is provided but not mapped, we need to expand it or the query
        # won't be valid
        expand = set(expand) | {name for name in snapshots if name not in mapping}

        if key not in self._query_cache:
            if query_ or self.is_sql:
                query_ = query_ or self.query
                render_kwargs = {
                    **date_dict(*dates, only_latest=self.kind.only_latest),
                    **kwargs,
                }

                if isinstance(query_, d.Jinja):
                    env = prepare_env(self.python_env)

                    try:
                        query_ = parse_one(
                            Environment()
                            .from_string(query_.name)
                            .render(**env, **render_kwargs),
                            read=self.dialect,
                        )
                    except Exception as ex:
                        raise ConfigError(
                            f"Invalid model query. {ex} at '{self._path}'"
                        ) from ex

                macro_evaluator = MacroEvaluator(
                    dialect or self.dialect, self.python_env
                )
                macro_evaluator.locals.update(render_kwargs)

                for definition in self.macro_definitions:
                    try:
                        macro_evaluator.evaluate(definition)
                    except MacroEvalError as ex:
                        _raise_config_error(
                            f"Failed to evaluate macro '{definition}'. {ex}", self._path
                        )

                try:
                    self._query_cache[key] = macro_evaluator.transform(query_)  # type: ignore
                except MacroEvalError as ex:
                    _raise_config_error(
                        f"Failed to evaluate macro '{definition}'. {ex}", self._path
                    )
            else:
                self._query_cache[key] = exp.select(
                    *(
                        exp.alias_(f"NULL::{column_type}", name)
                        for name, column_type in self.columns_to_types.items()
                    )
                )

            if self._schema:
                # This takes care of expanding star projections
                try:
                    self._query_cache[key] = optimize(
                        self._query_cache[key],
                        schema=self._schema,
                        rules=RENDER_OPTIMIZER_RULES,
                    )
                except SqlglotError as ex:
                    _raise_config_error(f"Invalid model query. {ex}", self._path)

                self._columns_to_types = {
                    expression.alias_or_name: expression.type
                    for expression in self._query_cache[key].expressions
                }

                self.validate_definition()

        query = self._query_cache[key]

        if expand:

            def _expand(node: exp.Expression) -> exp.Expression:
                if isinstance(node, exp.Table) and snapshots:
                    name = exp.table_name(node)
                    model = snapshots[name].model if name in snapshots else None
                    if name in expand and model:
                        return model._render_query(
                            start=start,
                            end=end,
                            latest=latest,
                            snapshots=snapshots,
                            expand=expand,
                            is_dev=is_dev,
                            **kwargs,
                        ).subquery(
                            alias=node.alias or model.view_name,
                            copy=False,
                        )
                return node

            query = query.transform(_expand)

        # Ensure there is no data leakage in incremental mode by filtering out all
        # events that have data outside the time window of interest.
        if add_incremental_filter and self.kind.is_incremental_by_time_range:
            # expansion copies the query for us. if it doesn't occur, make sure to copy.
            if not expand:
                query = query.copy()
            for node, _, _ in query.walk(prune=lambda n, *_: isinstance(n, exp.Select)):
                if isinstance(node, exp.Select):
                    self._filter_time_column(node, *dates[0:2])

        if mapping:
            return exp.replace_tables(query, mapping)

        if not isinstance(query, exp.Subqueryable):
            _raise_config_error(
                f"Query needs to be a SELECT or a UNION {query}", self._path
            )

        return query

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
        query = self._render_query(snapshots=snapshots, expand=snapshots, is_dev=is_dev)
        # the query is expanded so it's been copied, it's safe to mutate.
        for select in query.find_all(exp.Select):
            select.where("FALSE", copy=False)

        return query

    def render_query(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        is_dev: bool = False,
        **kwargs,
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
        return self._render_query(
            start=start,
            end=end,
            latest=latest,
            add_incremental_filter=True,
            snapshots=snapshots,
            expand=expand,
            is_dev=is_dev,
            **kwargs,
        )

    def exec_python(
        self,
        context: ExecutionContext,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        **kwargs,
    ) -> t.Generator[DF, None, None]:
        """Executes this model's python script.

        A python model is expected to return a dataframe or a generator that yields dataframes.

        Args:
            context: The execution context used for fetching data.
            start: The start date/time of the run.
            end: The end date/time of the run.
            latest: The latest date/time to use for the run.

        Returns:
            The return type must be a supported dataframe.
        """
        if self.is_sql:
            raise SQLMeshError(
                f"Model '{self.name}' is a SQL model and cannot be executed as a Python script."
            )

        env = prepare_env(self.python_env)
        start, end = make_inclusive(start or EPOCH_DS, end or EPOCH_DS)
        latest = to_datetime(latest or EPOCH_DS)
        try:
            df_or_iter = env[self.query.name](
                context=context, start=start, end=end, latest=latest, **kwargs
            )

            if not isinstance(df_or_iter, types.GeneratorType):
                df_or_iter = [df_or_iter]

            for df in df_or_iter:
                if self.kind.is_incremental_by_time_range:
                    assert self.time_column

                    if PySparkDataFrame and isinstance(df, PySparkDataFrame):
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
                        if self.time_column.format:
                            start_format: TimeLike = start.strftime(
                                self.time_column.format
                            )
                            end_format: TimeLike = end.strftime(self.time_column.format)
                        else:
                            start_format = start
                            end_format = end

                        df_time = df[self.time_column.column]
                        df = df[(df_time >= start_format) & (df_time <= end_format)]
                yield df
        except Exception as e:
            print_exception(e, self.python_env)
            raise SQLMeshError(
                f"Error executing Python model '{self.name}::{self.query.name}'"
            )

    def render_audit_queries(
        self,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        is_dev: bool = False,
        **kwargs,
    ) -> t.Generator[t.Tuple[Audit, exp.Subqueryable], None, None]:
        """Renders this model's audit queries, expanding macros with provided kwargs.

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental queries. Defaults to epoch start.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        for audit in self.audits.values():
            query = self._render_query(
                audit.query,
                start=start,
                end=end,
                latest=latest,
                snapshots=snapshots,
                audit_name=audit.name,
                dialect=audit.dialect,
                is_dev=is_dev,
                **kwargs,
            )
            yield audit, query

    def text_diff(self, other: Model) -> str:
        """Produce a text diff against another model.

        Args:
            other: The model to diff against.

        Returns:
            A unified text diff showing additions and deletions.
        """
        meta_a, *statements_a, query_a = self.render(show_python=True)
        meta_b, *statements_b, query_b = other.render(show_python=True)
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

    def set_time_format(
        self, default_time_format: str = c.DEFAULT_TIME_COLUMN_FORMAT
    ) -> None:
        """Sets the default time format for a model.

        Args:
            default_time_format: A python time format used as the default format when none is provided.
        """
        if not self.time_column:
            return

        if self.time_column.format:
            # Transpile the time column format into the generic dialect
            self.time_column.format = format_time(
                self.time_column.format,
                d.Dialect.get_or_raise(self.dialect).time_mapping,
            )
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
                return exp.Cast(this=exp.Literal.string(time), to=time_column_type)
        return exp.convert(time)

    @property
    def macro_definitions(self) -> t.List[d.MacroDef]:
        """All macro definitions from the list of expressions."""
        return [s for s in (self.expressions or []) if isinstance(s, d.MacroDef)]

    @property
    def sql_statements(self) -> t.List[exp.Expression]:
        """All sql statements from the list of expressions."""
        return [s for s in (self.expressions or []) if not isinstance(s, d.MacroDef)]

    @property
    def view_name(self) -> str:
        return parse_model_name(self.name)[2]

    @property
    def expressions(self) -> t.List[exp.Expression]:
        return self.expressions_ or []

    @property
    def python_env(self) -> t.Dict[str, Executable]:
        return self.python_env_ or {}

    def validate_definition(self) -> None:
        """Validates the model's definition.

        Model's are not allowed to have duplicate column names, non-explicitly casted columns,
        or non infererrable column names.

        Raises:
            ConfigError
        """
        name_counts: t.Dict[str, int] = {}
        query = self._render_query()

        if not isinstance(query, exp.Subqueryable):
            _raise_config_error(
                "Missing SELECT query in the model definition", self._path
            )

        if not query.expressions:
            _raise_config_error("Query missing select statements", self._path)

        for expression in query.expressions:
            alias = expression.alias_or_name
            name_counts[alias] = name_counts.get(alias, 0) + 1

            if not alias:
                _raise_config_error(
                    f"Outer projection `{expression}` must have inferrable names or explicit aliases.",
                    self._path,
                )

        for name, count in name_counts.items():
            if count > 1:
                _raise_config_error(
                    f"Found duplicate outer select name `{name}`", self._path
                )

        if self.partitioned_by:
            unique_partition_keys = {k.strip().lower() for k in self.partitioned_by}
            if len(self.partitioned_by) != len(unique_partition_keys):
                _raise_config_error(
                    "All partition keys must be unique in the model definition",
                    self._path,
                )

            projections = {p.lower() for p in query.named_selects}
            missing_keys = unique_partition_keys - projections
            if missing_keys:
                missing_keys_str = ", ".join(f"'{k}'" for k in sorted(missing_keys))
                _raise_config_error(
                    f"Partition keys [{missing_keys_str}] are missing in the query in the model definition",
                    self._path,
                )

        if self.kind.is_incremental_by_time_range and not self.time_column:
            _raise_config_error(
                "Incremental by time range models must have a time_column field.",
                self._path,
            )

    def _validate_view(self, query: exp.Expression) -> None:
        pass

    def _filter_time_column(
        self, query: exp.Select, start: TimeLike, end: TimeLike
    ) -> None:
        """Filters a query on the time column to ensure no data leakage when running in incremental mode."""
        if not self.time_column:
            return

        low = self.convert_to_time_column(start)
        high = self.convert_to_time_column(end)

        time_column_projection = next(
            select
            for select in query.selects
            if select.alias_or_name == self.time_column.column
        )

        if isinstance(time_column_projection, exp.Alias):
            time_column_projection = time_column_projection.this

        between = exp.Between(this=time_column_projection.copy(), low=low, high=high)

        if not query.args.get("group"):
            query.where(between, copy=False)
        else:
            query.having(between, copy=False)

        simplify(query)

    def __repr__(self):
        return f"Model<name: {self.name}, query: {str(self.query)[0:30]}>"


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
        if isinstance(table.this, exp.Identifier)
        and exp.table_name(table) not in scope.cte_sources
    }


def _python_env(
    query: exp.Expression, module_path: Path, macros: MacroRegistry
) -> t.Dict[str, Executable]:
    python_env: t.Dict[str, Executable] = {}

    used_macros = {}

    if isinstance(query, d.Jinja):
        for var in query.expressions:
            if var in macros:
                used_macros[var] = macros[var]
    else:
        for macro_func in query.find_all(d.MacroFunc):
            if macro_func.__class__ is d.MacroFunc:
                name = macro_func.this.name.lower()
                used_macros[name] = macros[name]

    for name, macro in used_macros.items():
        if macro.serialize:
            build_env(
                macro.func,
                env=python_env,
                name=name,
                path=module_path,
            )

    return serialize_env(python_env, path=module_path)


def _parse_depends_on(
    model_func: d.MacroVar, python_env: t.Dict[str, Executable]
) -> t.Set[str]:
    """Parses the source of a model function and finds upstream dependencies based on calls to context."""
    env = prepare_env(python_env)
    depends_on = set()
    executable = python_env[model_func.name]

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
                    (
                        keyword.value
                        for keyword in node.keywords
                        if keyword.arg == "model_name"
                    ),
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


def _raise_config_error(msg: str, location: t.Optional[str | Path] = None) -> None:
    if location:
        raise ConfigError(f"{msg} at '{location}'")
    raise ConfigError(msg)
