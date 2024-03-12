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

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.macros import MacroEvaluator, RuntimeStage
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

    from sqlmesh.core.snapshot import DeployabilityIndex, Snapshot

CacheKey = t.Tuple[datetime, datetime, datetime, RuntimeStage]


logger = logging.getLogger(__name__)


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
        schema: t.Optional[t.Dict[str, t.Any]] = None,
        default_catalog: t.Optional[str] = None,
    ):
        self._expression = expression
        self._dialect = dialect
        self._macro_definitions = macro_definitions
        self._path = path
        self._jinja_macro_registry = jinja_macro_registry or JinjaMacroRegistry()
        self._python_env = python_env or {}
        self._only_execution_time = only_execution_time
        self._default_catalog = default_catalog
        self.update_schema({} if schema is None else schema)
        self._cache: t.Dict[CacheKey, t.List[t.Optional[exp.Expression]]] = {}

    def update_schema(self, schema: t.Dict[str, t.Any]) -> None:
        self.schema = d.normalize_mapping_schema(schema, dialect=self._dialect)

    def _render(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        runtime_stage: RuntimeStage = RuntimeStage.LOADING,
        **kwargs: t.Any,
    ) -> t.List[t.Optional[exp.Expression]]:
        """Renders a expression, expanding macros with provided kwargs

        Args:
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            execution_time: The date/time time reference to use for execution time.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            table_mapping: Table mapping of physical locations. Takes precedence over snapshot mappings.
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
            runtime_stage: Indicates the current runtime stage, for example if we're still loading the project, etc.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expressions.
        """

        cache_key = self._cache_key(start, end, execution_time, runtime_stage)

        if cache_key not in self._cache:
            expressions = [self._expression]

            render_kwargs = {
                **date_dict(
                    cache_key[2],
                    cache_key[0] if not self._only_execution_time else None,
                    cache_key[1] if not self._only_execution_time else None,
                ),
                **kwargs,
            }

            env = prepare_env(self._python_env)
            jinja_env = self._jinja_macro_registry.build_environment(
                **{**render_kwargs, **env},
                snapshots=(snapshots or {}),
                table_mapping=table_mapping,
                deployability_index=deployability_index,
                default_catalog=self._default_catalog,
            )

            if isinstance(self._expression, d.Jinja):
                try:
                    expressions = []
                    rendered_expression = jinja_env.from_string(self._expression.name).render()
                    if rendered_expression.strip():
                        expressions = [
                            e for e in parse(rendered_expression, read=self._dialect) if e
                        ]

                        if not expressions:
                            raise ConfigError(f"Failed to parse an expression:\n{self._expression}")
                except ParsetimeAdapterCallError:
                    raise
                except Exception as ex:
                    raise ConfigError(
                        f"Could not render or parse jinja at '{self._path}'.\n{ex}"
                    ) from ex

            macro_evaluator = MacroEvaluator(
                self._dialect,
                python_env=self._python_env,
                jinja_env=jinja_env,
                schema=self.schema,
                runtime_stage=runtime_stage,
                resolve_tables=lambda e: self._resolve_tables(
                    e,
                    snapshots=snapshots,
                    table_mapping=table_mapping,
                    deployability_index=deployability_index,
                    start=start,
                    end=end,
                    execution_time=execution_time,
                    runtime_stage=runtime_stage,
                ),
                snapshots=snapshots,
                default_catalog=self._default_catalog,
            )

            for definition in self._macro_definitions:
                try:
                    macro_evaluator.evaluate(definition)
                except MacroEvalError as ex:
                    raise_config_error(f"Failed to evaluate macro '{definition}'. {ex}", self._path)

            macro_evaluator.locals.update(render_kwargs)

            resolved_expressions: t.List[t.Optional[exp.Expression]] = []
            for expression in expressions:
                try:
                    expression = macro_evaluator.transform(expression)  # type: ignore
                except MacroEvalError as ex:
                    raise_config_error(f"Failed to resolve macro for expression. {ex}", self._path)

                if expression:
                    with _normalize_and_quote(
                        expression, self._dialect, self._default_catalog
                    ) as expression:
                        if hasattr(expression, "selects"):
                            for select in expression.selects:
                                if not isinstance(select, exp.Alias) and select.output_name not in (
                                    "*",
                                    "",
                                ):
                                    alias = exp.alias_(select, select.output_name, quoted=True)
                                    comments = alias.this.comments
                                    if comments:
                                        alias.add_comments(comments)
                                        comments.clear()

                                    select.replace(alias)
                    resolved_expressions.append(expression)

            # We dont cache here if columns_to_type was called in a macro.
            # This allows the model's query to be re-rendered so that the
            # MacroEvaluator can resolve columns_to_types calls and provide true schemas.
            if not macro_evaluator.columns_to_types_called:
                self._cache[cache_key] = resolved_expressions
            return resolved_expressions

        return self._cache[cache_key]

    def update_cache(
        self,
        expression: t.Optional[exp.Expression],
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        **kwargs: t.Any,
    ) -> None:
        self._cache[self._cache_key(start, end, execution_time)] = [expression]

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
        deployability_index: t.Optional[DeployabilityIndex] = None,
        **kwargs: t.Any,
    ) -> E:
        if not snapshots and not table_mapping and not expand:
            return expression

        from sqlmesh.core.snapshot import to_table_mapping

        expression = expression.copy()
        with _normalize_and_quote(expression, self._dialect, self._default_catalog) as expression:
            snapshots = snapshots or {}
            table_mapping = table_mapping or {}
            mapping = {**to_table_mapping(snapshots.values(), deployability_index), **table_mapping}
            expand = set(expand) | {
                name for name, snapshot in snapshots.items() if snapshot.is_embedded
            }

            if expand:
                model_mapping = {
                    name: snapshot.model
                    for name, snapshot in snapshots.items()
                    if snapshot.is_model
                }

                def _expand(node: exp.Expression) -> exp.Expression:
                    if isinstance(node, exp.Table) and snapshots:
                        name = exp.table_name(node, identify=True)
                        model = model_mapping.get(name)
                        if (
                            name in expand
                            and model
                            and not model.is_seed
                            and not model.kind.is_external
                        ):
                            nested_query = model.render_query(
                                start=start,
                                end=end,
                                execution_time=execution_time,
                                snapshots=snapshots,
                                table_mapping=table_mapping,
                                expand=expand,
                                deployability_index=deployability_index,
                                **kwargs,
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
                expression = exp.replace_tables(
                    expression, mapping, dialect=self._dialect, copy=False
                )

            return expression

    def _cache_key(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        runtime_stage: RuntimeStage = RuntimeStage.LOADING,
    ) -> CacheKey:
        return (
            *make_inclusive(start or c.EPOCH, end or c.EPOCH),
            to_datetime(execution_time or c.EPOCH),
            runtime_stage,
        )


class ExpressionRenderer(BaseExpressionRenderer):
    def render(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        expand: t.Iterable[str] = tuple(),
        **kwargs: t.Any,
    ) -> t.Optional[t.List[exp.Expression]]:
        try:
            expressions = super()._render(
                start=start,
                end=end,
                execution_time=execution_time,
                snapshots=snapshots,
                deployability_index=deployability_index,
                **kwargs,
            )
        except ParsetimeAdapterCallError:
            return None

        return [
            self._resolve_tables(
                e,
                snapshots=snapshots,
                table_mapping=table_mapping,
                expand=expand,
                deployability_index=deployability_index,
                start=start,
                end=end,
                execution_time=execution_time,
                **kwargs,
            )
            for e in expressions
            if e
        ]


class QueryRenderer(BaseExpressionRenderer):
    def __init__(
        self,
        query: exp.Expression,
        dialect: str,
        macro_definitions: t.List[d.MacroDef],
        schema: t.Optional[t.Dict[str, t.Any]] = None,
        model_fqn: t.Optional[str] = None,
        path: Path = Path(),
        jinja_macro_registry: t.Optional[JinjaMacroRegistry] = None,
        python_env: t.Optional[t.Dict[str, Executable]] = None,
        only_execution_time: bool = False,
        default_catalog: t.Optional[str] = None,
    ):
        super().__init__(
            expression=query,
            dialect=dialect,
            macro_definitions=macro_definitions,
            path=path,
            jinja_macro_registry=jinja_macro_registry,
            python_env=python_env,
            only_execution_time=only_execution_time,
            schema=schema,
            default_catalog=default_catalog,
        )

        self._model_fqn = model_fqn

        self._optimized_cache: t.Dict[CacheKey, exp.Query] = {}

    def update_schema(self, schema: t.Dict[str, t.Any]) -> None:
        super().update_schema(schema)
        self._optimized_cache = {}

    def render(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        table_mapping: t.Optional[t.Dict[str, str]] = None,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        expand: t.Iterable[str] = tuple(),
        optimize: bool = True,
        runtime_stage: RuntimeStage = RuntimeStage.LOADING,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Query]:
        """Renders a query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            query: The query to render.
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            execution_time: The date/time time reference to use for execution time. Defaults to epoch start.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            table_mapping: Table mapping of physical locations. Takes precedence over snapshot mappings.
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.
            optimize: Whether to optimize the query.
            runtime_stage: Indicates the current runtime stage, for example if we're still loading the project, etc.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        cache_key = self._cache_key(start, end, execution_time, runtime_stage)

        if not optimize or cache_key not in self._optimized_cache:
            try:
                expressions = super()._render(
                    start=start,
                    end=end,
                    execution_time=execution_time,
                    snapshots=snapshots,
                    table_mapping=table_mapping,
                    deployability_index=deployability_index,
                    runtime_stage=runtime_stage,
                    **kwargs,
                )
            except ParsetimeAdapterCallError:
                return None

            if not expressions:
                raise ConfigError(f"Failed to render query at '{self._path}':\n{self._expression}")

            if len(expressions) > 1:
                raise ConfigError(f"Too many statements in query:\n{self._expression}")

            query = expressions[0]

            if not query:
                return None
            if not isinstance(query, exp.Query):
                raise_config_error(f"Query needs to be a SELECT or a UNION {query}.", self._path)
                raise

            # we find deps here so that it is cached in the model cache
            deps = d.find_tables(
                query, default_catalog=self._default_catalog, dialect=self._dialect
            )

            if optimize:
                query = self._optimize_query(query, deps)
                self._optimized_cache[cache_key] = query
        else:
            query = self._optimized_cache[cache_key]

        # Table resolution MUST happen after optimization, otherwise the schema won't match the table names.
        query = self._resolve_tables(
            query,
            snapshots=snapshots,
            table_mapping=table_mapping,
            expand=expand,
            deployability_index=deployability_index,
            start=start,
            end=end,
            execution_time=execution_time,
            runtime_stage=runtime_stage,
            **kwargs,
        )
        return query

    def update_cache(
        self,
        expression: t.Optional[exp.Expression],
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        optimized: bool = False,
        **kwargs: t.Any,
    ) -> None:
        if optimized:
            if not isinstance(expression, exp.Query):
                raise SQLMeshError(f"Expected a Query but got: {expression}")
            self._optimized_cache[self._cache_key(start, end, execution_time)] = expression
        else:
            super().update_cache(
                expression, start=start, end=end, execution_time=execution_time, **kwargs
            )

    def _optimize_query(self, query: exp.Query, all_deps: t.Set[str]) -> exp.Query:
        # We don't want to normalize names in the schema because that's handled by the optimizer
        original = query
        missing_deps = set()
        all_deps = all_deps - {self._model_fqn}
        should_optimize = not self.schema.empty or not all_deps

        for dep in all_deps:
            if not self.schema.find(exp.to_table(dep)):
                should_optimize = False
                missing_deps.add(dep)

        if self._model_fqn and not should_optimize and any(s.is_star for s in query.selects):
            deps = ", ".join(f"'{dep}'" for dep in sorted(missing_deps))

            logger.warning(
                f"SELECT * cannot be expanded due to missing schema(s) for model(s): {deps}. "
                "Run `sqlmesh create_external_models` and / or make sure that the model "
                f"'{self._model_fqn}' can be rendered at parse time.",
            )

        try:
            if should_optimize:
                query = query.copy()
                simplify(
                    annotate_types(
                        qualify(
                            query,
                            dialect=self._dialect,
                            schema=self.schema,
                            infer_schema=False,
                            catalog=self._default_catalog,
                        ),
                        schema=self.schema,
                    )
                )
        except SqlglotError as ex:
            query = original

            logger.warning(
                "%s for model '%s', the column may not exist or is ambiguous", ex, self._model_fqn
            )

        if not query.type:
            for select in query.expressions:
                annotate_types(select)

        return query


@contextmanager
def _normalize_and_quote(query: E, dialect: str, default_catalog: t.Optional[str]) -> t.Iterator[E]:
    qualify_tables(query, catalog=default_catalog, dialect=dialect)
    normalize_identifiers(query, dialect=dialect)
    yield query
    quote_identifiers(query, dialect=dialect)
