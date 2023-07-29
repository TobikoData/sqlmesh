from __future__ import annotations

import logging
import typing as t
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from sqlglot import exp, parse
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.simplify import simplify
from sqlglot.schema import MappingSchema

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.utils.date import TimeLike, date_dict, make_inclusive, to_datetime
from sqlmesh.utils.errors import (
    ConfigError,
    MacroEvalError,
    ParsetimeAdapterCallError,
    raise_config_error,
)
from sqlmesh.utils.jinja import JinjaMacroRegistry
from sqlmesh.utils.metaprogramming import Executable, prepare_env

if t.TYPE_CHECKING:
    from sqlglot._typing import E

    from sqlmesh.core.snapshot import Snapshot


logger = logging.getLogger(__name__)


def _dates(
    start: t.Optional[TimeLike] = None,
    end: t.Optional[TimeLike] = None,
    execution_time: t.Optional[TimeLike] = None,
) -> t.Tuple[datetime, datetime, datetime]:
    return (
        *make_inclusive(start or c.EPOCH, end or c.EPOCH),
        to_datetime(execution_time or c.EPOCH),
    )


class BaseExpressionRenderer:
    def __init__(
        self,
        expression: exp.Expression,
        dialect: str,
        macro_definitions: t.List[d.MacroDef],
        path: Path = Path(),
        jinja_macro_registry: t.Optional[JinjaMacroRegistry] = None,
        python_env: t.Optional[t.Dict[str, Executable]] = None,
        only_execution_time: bool = False,
    ):
        self._expression = expression
        self._dialect = dialect
        self._macro_definitions = macro_definitions
        self._path = path
        self._jinja_macro_registry = jinja_macro_registry or JinjaMacroRegistry()
        self._python_env = python_env or {}
        self._only_execution_time = only_execution_time

        self._cache: t.Dict[t.Tuple[datetime, datetime, datetime], t.List[exp.Expression]] = {}

    def _render(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        is_dev: bool = False,
        **kwargs: t.Any,
    ) -> t.List[exp.Expression]:
        """Renders a expression, expanding macros with provided kwargs

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            execution_time: The date/time time reference to use for execution time.
            kwargs: Additional kwargs to pass to the renderer.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            table_mapping: Table mapping of physical locations. Takes precedence over snapshot mappings.
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.

        Returns:
            The rendered expressions.
        """

        cache_key = _dates(start, end, execution_time)
        start_dt, end_dt, execution_dt = cache_key
        if cache_key not in self._cache:
            expressions = [self._expression]

            render_kwargs = {
                **date_dict(
                    execution_dt,
                    start_dt if not self._only_execution_time else None,
                    end_dt if not self._only_execution_time else None,
                ),
                **kwargs,
            }

            env = prepare_env(self._python_env)
            jinja_env = self._jinja_macro_registry.build_environment(
                **{**render_kwargs, **env},
                snapshots=(snapshots or {}),
                table_mapping=table_mapping,
                is_dev=is_dev,
            )

            if isinstance(self._expression, d.Jinja):
                try:
                    rendered_expression = jinja_env.from_string(self._expression.name).render()
                    if not rendered_expression.strip():
                        self._cache[cache_key] = []
                        return []

                    parsed_expressions = [
                        e for e in parse(rendered_expression, read=self._dialect) if e
                    ]
                    if not parsed_expressions:
                        raise ConfigError(f"Failed to parse an expression {self._expression}")
                    expressions = parsed_expressions
                except ParsetimeAdapterCallError:
                    raise
                except Exception as ex:
                    raise ConfigError(f"Invalid expression. {ex} at '{self._path}'") from ex

            macro_evaluator = MacroEvaluator(
                self._dialect,
                python_env=self._python_env,
                jinja_env=jinja_env,
            )

            for definition in self._macro_definitions:
                try:
                    macro_evaluator.evaluate(definition)
                except MacroEvalError as ex:
                    raise_config_error(f"Failed to evaluate macro '{definition}'. {ex}", self._path)

            macro_evaluator.locals.update(render_kwargs)

            resolved_expressions: t.List[exp.Expression] = []
            for expression in expressions:
                try:
                    expression = macro_evaluator.transform(expression)  # type: ignore
                except MacroEvalError as ex:
                    raise_config_error(f"Failed to resolve macro for expression. {ex}", self._path)

                if expression:
                    with _normalize_and_quote(expression, self._dialect) as expression:
                        pass
                    resolved_expressions.append(expression)

            self._cache[cache_key] = resolved_expressions

        return self._cache[cache_key]

    def update_cache(
        self,
        expression: exp.Expression,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        **kwargs: t.Any,
    ) -> None:
        self._cache[_dates(start, end, execution_time)] = [expression]

    def _resolve_tables(
        self,
        expression: E,
        *,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        expand: t.Iterable[str] = tuple(),
        is_dev: bool = False,
        **kwargs: t.Any,
    ) -> E:
        if not snapshots and not table_mapping and not expand:
            return expression

        expression = expression.copy()
        with _normalize_and_quote(expression, self._dialect) as expression:
            return _resolve_tables(
                expression,
                snapshots=snapshots,
                table_mapping=table_mapping,
                expand=expand,
                is_dev=is_dev,
                start=start,
                end=end,
                execution_time=execution_time,
                **kwargs,
            )


class ExpressionRenderer(BaseExpressionRenderer):
    def render(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        is_dev: bool = False,
        expand: t.Iterable[str] = tuple(),
        **kwargs: t.Any,
    ) -> t.List[exp.Expression]:
        expressions = super()._render(
            start=start,
            end=end,
            execution_time=execution_time,
            snapshots=snapshots,
            is_dev=is_dev,
            **kwargs,
        )

        return [
            self._resolve_tables(
                e,
                snapshots=snapshots,
                table_mapping=table_mapping,
                expand=expand,
                is_dev=is_dev,
                start=start,
                end=end,
                execution_time=execution_time,
                **kwargs,
            )
            for e in expressions
        ]


class QueryRenderer(BaseExpressionRenderer):
    def __init__(
        self,
        query: exp.Expression,
        dialect: str,
        macro_definitions: t.List[d.MacroDef],
        schema: t.Optional[t.Dict[str, t.Any]] = None,
        model_name: t.Optional[str] = None,
        path: Path = Path(),
        jinja_macro_registry: t.Optional[JinjaMacroRegistry] = None,
        python_env: t.Optional[t.Dict[str, Executable]] = None,
        only_execution_time: bool = False,
    ):
        super().__init__(
            expression=query,
            dialect=dialect,
            macro_definitions=macro_definitions,
            path=path,
            jinja_macro_registry=jinja_macro_registry,
            python_env=python_env,
            only_execution_time=only_execution_time,
        )

        self._model_name = model_name

        self._optimized_cache: t.Dict[t.Tuple[datetime, datetime, datetime], exp.Expression] = {}

        self.schema = {} if schema is None else schema

    def render(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        is_dev: bool = False,
        expand: t.Iterable[str] = tuple(),
        optimize: bool = True,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Subqueryable]:
        """Renders a query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            query: The query to render.
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            execution_time: The date/time time reference to use for execution time. Defaults to epoch start.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            table_mapping: Table mapping of physical locations. Takes precedence over snapshot mappings.
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.
            optimize: Whether to optimize the query.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        cache_key = _dates(start, end, execution_time)

        if not optimize or cache_key not in self._optimized_cache:
            try:
                expressions = super()._render(
                    start=start,
                    end=end,
                    execution_time=execution_time,
                    snapshots=snapshots,
                    table_mapping=table_mapping,
                    is_dev=is_dev,
                    **kwargs,
                )
            except ParsetimeAdapterCallError:
                return None

            if not expressions:
                raise ConfigError(f"Failed to render query:\n{self._expression}")

            if len(expressions) > 1:
                raise ConfigError(f"Too many statements in query:\n{self._expression}")

            query = t.cast(exp.Subqueryable, expressions[0])

            if optimize:
                query = self._optimize_query(query)
                self._optimized_cache[cache_key] = query
        else:
            query = t.cast(exp.Subqueryable, self._optimized_cache[cache_key])

        # Table resolution MUST happen after optimization, otherwise the schema won't match the table names.
        query = self._resolve_tables(
            query,
            snapshots=snapshots,
            table_mapping=table_mapping,
            expand=expand,
            is_dev=is_dev,
            start=start,
            end=end,
            execution_time=execution_time,
            **kwargs,
        )

        if not isinstance(query, exp.Subqueryable):
            raise_config_error(f"Query needs to be a SELECT or a UNION {query}.", self._path)

        return query

    def update_cache(
        self,
        expression: exp.Expression,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        optimized: bool = False,
        **kwargs: t.Any,
    ) -> None:
        if not optimized:
            super().update_cache(
                expression, start=start, end=end, execution_time=execution_time, **kwargs
            )
        else:
            self._optimized_cache[_dates(start, end, execution_time)] = expression

    def _optimize_query(self, query: exp.Subqueryable) -> exp.Subqueryable:
        # We don't want to normalize names in the schema because that's handled by the optimizer
        schema = MappingSchema(self.schema, dialect=self._dialect, normalize=False)
        original = query
        failure = False

        dependencies = d.find_tables(query, dialect=self._dialect) - {self._model_name}
        for dependency in dependencies:
            if schema.find(exp.to_table(dependency)) is None:
                if self._model_name is not None:
                    logger.warning(
                        "Query cannot be optimized due to missing schema for model '%s'. "
                        "Run `sqlmesh create_external_models` and / or make sure that the model '%s' can be rendered at parse time",
                        dependency,
                        self._model_name,
                    )
                schema = MappingSchema(None, dialect=self._dialect, normalize=False)
                break

        should_optimize = not schema.empty or not dependencies

        try:
            if should_optimize:
                query = query.copy()

                simplify(
                    qualify(
                        query,
                        dialect=self._dialect,
                        schema=schema,
                        infer_schema=False,
                    )
                )
        except SqlglotError as ex:
            failure = True
            logger.error(
                "%s for model '%s', the column may not exist or is ambiguous", ex, self._model_name
            )
        finally:
            if failure or not should_optimize:
                query = original.copy()

                with _normalize_and_quote(query, self._dialect) as query:
                    for select in query.selects:
                        if not isinstance(select, exp.Alias) and select.output_name not in (
                            "*",
                            "",
                        ):
                            select.replace(exp.alias_(select, select.output_name))

        return annotate_types(query, schema=schema)


@contextmanager
def _normalize_and_quote(query: E, dialect: str) -> t.Iterator[E]:
    qualify_tables(query)
    normalize_identifiers(query, dialect=dialect)
    yield query
    quote_identifiers(query, dialect=dialect)


def _resolve_tables(
    expression: E,
    *,
    snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
    table_mapping: t.Optional[t.Dict[str, str]] = None,
    expand: t.Iterable[str] = tuple(),
    is_dev: bool = False,
    **render_kwargs: t.Any,
) -> E:
    from sqlmesh.core.snapshot import to_table_mapping

    snapshots = snapshots or {}
    table_mapping = table_mapping or {}
    mapping = {**to_table_mapping(snapshots.values(), is_dev), **table_mapping}
    # if a snapshot is provided but not mapped, we need to expand it or the query
    # won't be valid
    expand = set(expand) | {name for name in snapshots if name not in mapping}

    if expand:

        def _expand(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Table) and snapshots:
                name = exp.table_name(node)
                model = snapshots[name].model if name in snapshots else None
                if name in expand and model and not model.is_seed and not model.kind.is_external:
                    nested_query = model.render_query(
                        snapshots=snapshots,
                        expand=expand,
                        is_dev=is_dev,
                        **render_kwargs,
                    )
                    if nested_query is not None:
                        return nested_query.subquery(
                            alias=node.alias or model.view_name,
                            copy=False,
                        )
                    else:
                        logger.warning("Failed to expand the nested model '%s'", name)
            return node

        expression = expression.transform(_expand, copy=False)

    if mapping:
        expression = exp.replace_tables(expression, mapping, copy=False)

    return expression
