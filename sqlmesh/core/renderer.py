from __future__ import annotations

import logging
import typing as t
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.simplify import simplify
from sqlglot.schema import ensure_schema

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import TimeColumn
from sqlmesh.utils.date import TimeLike, date_dict, make_inclusive, to_datetime
from sqlmesh.utils.errors import (
    ConfigError,
    MacroEvalError,
    ParsetimeAdapterCallError,
    SQLMeshError,
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
    latest: t.Optional[TimeLike] = None,
) -> t.Tuple[datetime, datetime, datetime]:
    return (
        *make_inclusive(start or c.EPOCH, end or c.EPOCH),
        to_datetime(latest or c.EPOCH),
    )


class ExpressionRenderer:
    def __init__(
        self,
        expression: exp.Expression,
        dialect: str,
        macro_definitions: t.List[d.MacroDef],
        path: Path = Path(),
        jinja_macro_registry: t.Optional[JinjaMacroRegistry] = None,
        python_env: t.Optional[t.Dict[str, Executable]] = None,
        only_latest: bool = False,
    ):
        self._expression = expression
        self._dialect = dialect
        self._macro_definitions = macro_definitions
        self._path = path
        self._jinja_macro_registry = jinja_macro_registry or JinjaMacroRegistry()
        self._python_env = python_env or {}
        self._only_latest = only_latest

        self._cache: t.Dict[t.Tuple[datetime, datetime, datetime], t.Optional[exp.Expression]] = {}

    def render(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        is_dev: bool = False,
        expand: t.Iterable[str] = tuple(),
        **kwargs: t.Any,
    ) -> t.Optional[exp.Expression]:
        """Renders a expression, expanding macros with provided kwargs

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental models. Defaults to epoch start.
            kwargs: Additional kwargs to pass to the renderer.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.

        Returns:
            The rendered expression.
        """

        dates = _dates(start, end, latest)
        cache_key = dates
        if cache_key not in self._cache:
            expression = self._expression

            render_kwargs = {
                **date_dict(*dates, only_latest=self._only_latest),
                **kwargs,
            }

            env = prepare_env(self._python_env)
            jinja_env = self._jinja_macro_registry.build_environment(**{**render_kwargs, **env})

            if isinstance(expression, d.Jinja):
                try:
                    rendered_expression = jinja_env.from_string(expression.name).render()
                    if not rendered_expression.strip():
                        self._cache[cache_key] = None
                        return None

                    parsed_expression = parse_one(rendered_expression, read=self._dialect)
                    if not parsed_expression:
                        raise ConfigError(f"Failed to parse a expression {expression}")
                    expression = parsed_expression
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

            try:
                expression = macro_evaluator.transform(expression)  # type: ignore
            except MacroEvalError as ex:
                raise_config_error(f"Failed to resolve macro for expression. {ex}", self._path)

            _normalize_and_quote(expression, self._dialect)

            self._cache[cache_key] = expression

        expression = t.cast(exp.Expression, self._cache[cache_key])

        if expression and (snapshots or expand):
            expression = expression.copy()

            with _normalize_and_quote(expression, self._dialect) as expression:
                expression = _resolve_tables(
                    expression,
                    snapshots=snapshots,
                    expand=expand,
                    is_dev=is_dev,
                    start=start,
                    end=end,
                    latest=latest,
                    **kwargs,
                )

        return expression

    def update_cache(
        self,
        expression: exp.Expression,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        **kwargs: t.Any,
    ) -> None:
        self._cache[_dates(start, end, latest)] = expression


class QueryRenderer(ExpressionRenderer):
    def __init__(
        self,
        query: exp.Expression,
        dialect: str,
        macro_definitions: t.List[d.MacroDef],
        schema: t.Optional[t.Dict[str, t.Any]] = None,
        path: Path = Path(),
        jinja_macro_registry: t.Optional[JinjaMacroRegistry] = None,
        python_env: t.Optional[t.Dict[str, Executable]] = None,
        time_column: t.Optional[TimeColumn] = None,
        time_converter: t.Optional[t.Callable[[TimeLike], exp.Expression]] = None,
        only_latest: bool = False,
    ):
        super().__init__(
            expression=query,
            dialect=dialect,
            macro_definitions=macro_definitions,
            path=path,
            jinja_macro_registry=jinja_macro_registry,
            python_env=python_env,
            only_latest=only_latest,
        )

        self._time_column = time_column
        self._time_converter = time_converter or (lambda v: exp.convert(v))

        self._optimized_cache: t.Dict[t.Tuple[datetime, datetime, datetime], exp.Expression] = {}

        self.schema = {} if schema is None else schema

    def render(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        is_dev: bool = False,
        expand: t.Iterable[str] = tuple(),
        add_incremental_filter: bool = False,
        optimize: bool = True,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Subqueryable]:
        """Renders a query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            query: The query to render.
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental queries. Defaults to epoch start.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.
            add_incremental_filter: Add an incremental filter to the query if the model is incremental.
            optimize: Whether to optimize the query.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        cache_key = _dates(start, end, latest)
        skip_cache = bool(snapshots or expand)

        if skip_cache or not optimize or cache_key not in self._optimized_cache:
            try:
                query = t.cast(
                    exp.Subqueryable,
                    super().render(
                        start=start,
                        end=end,
                        latest=latest,
                        snapshots=snapshots,
                        is_dev=is_dev,
                        expand=expand,
                        **kwargs,
                    ),
                )
            except ParsetimeAdapterCallError:
                logger.debug("Failed to render query at parse time:\n%s", self._expression)
                return None

            if not query:
                raise ConfigError(f"Failed to render query:\n{query}")

            if optimize:
                query = self._optimize_query(query)
                if not skip_cache:
                    self._optimized_cache[cache_key] = query
        else:
            query = t.cast(exp.Subqueryable, self._optimized_cache[cache_key])

        # Ensure there is no data leakage in incremental mode by filtering out all
        # events that have data outside the time window of interest.
        if add_incremental_filter and self._time_column:
            query = query.copy()
            dates = _dates(start, end, latest)
            with_ = query.args.pop("with", None)
            query = (
                exp.select("*", copy=False)
                .from_(
                    t.cast(exp.Subqueryable, query).subquery("_subquery", copy=False), copy=False
                )
                .where(self.time_column_filter(*dates[0:2]), copy=False)
            )

            if with_:
                query.set("with", with_)

        if not isinstance(query, exp.Subqueryable):
            raise_config_error(f"Query needs to be a SELECT or a UNION {query}.", self._path)

        return query

    def update_cache(
        self,
        expression: exp.Expression,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        optimized: bool = False,
        **kwargs: t.Any,
    ) -> None:
        if not optimized:
            super().update_cache(expression, start=start, end=end, latest=latest, **kwargs)
        else:
            self._optimized_cache[_dates(start, end, latest)] = expression

    def time_column_filter(self, start: TimeLike, end: TimeLike) -> exp.Between:
        """Returns a between statement with the properly formatted time column."""
        if not self._time_column:
            raise SQLMeshError(
                "Cannot produce time column filter because model does not have a time column."
            )

        return exp.column(self._time_column.column).between(
            self._time_converter(start), self._time_converter(end)  # type: ignore
        )

    def _optimize_query(self, query: exp.Subqueryable) -> exp.Subqueryable:
        # We don't want to normalize names in the schema because that's handled by the optimizer
        schema = ensure_schema(self.schema, dialect=self._dialect, normalize=False)
        original = query
        failure = False

        try:
            if not schema.empty:
                query = query.copy()

                qualify(
                    query,
                    dialect=self._dialect,
                    schema=schema,
                    infer_schema=False,
                    validate_qualify_columns=False,
                )
        except SqlglotError as ex:
            failure = True
            logger.error("%s for '%s', the column may not exist or is ambiguous", ex, self._path)
        finally:
            if failure or schema.empty:
                query = original.copy()

                with _normalize_and_quote(query, self._dialect) as query:
                    for select in query.selects:
                        if not isinstance(select, exp.Alias) and select.output_name not in (
                            "*",
                            "",
                        ):
                            select.replace(exp.alias_(select, select.output_name))

        return annotate_types(simplify(query), schema=schema)


@contextmanager
def _normalize_and_quote(query: E, dialect: str) -> t.Iterator[E]:
    normalize_identifiers(query, dialect=dialect)
    qualify_tables(query)
    yield query
    quote_identifiers(query, dialect=dialect)


def _resolve_tables(
    expression: exp.Expression,
    *,
    snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
    expand: t.Iterable[str] = tuple(),
    is_dev: bool = False,
    **render_kwargs: t.Any,
) -> exp.Expression:
    from sqlmesh.core.snapshot import to_table_mapping

    snapshots = snapshots or {}
    mapping = to_table_mapping(snapshots.values(), is_dev)
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
