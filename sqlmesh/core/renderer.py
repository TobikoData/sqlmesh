from __future__ import annotations

import typing as t
from datetime import datetime
from pathlib import Path

from jinja2 import Environment
from sqlglot import exp, parse_one
from sqlglot.errors import SchemaError, SqlglotError
from sqlglot.optimizer import optimize
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.simplify import simplify
from sqlglot.schema import MappingSchema

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import TimeColumn
from sqlmesh.utils.date import TimeLike, date_dict, make_inclusive, to_datetime
from sqlmesh.utils.errors import ConfigError, MacroEvalError, raise_config_error
from sqlmesh.utils.metaprogramming import Executable, prepare_env

if t.TYPE_CHECKING:
    from sqlmesh.core.snapshot import Snapshot

RENDER_OPTIMIZER_RULES = (qualify_tables, qualify_columns, annotate_types)


class QueryRenderer:
    def __init__(
        self,
        query: exp.Expression,
        dialect: str,
        macro_definitions: t.List[d.MacroDef],
        path: Path = Path(),
        python_env: t.Optional[t.Dict[str, Executable]] = None,
        time_column: t.Optional[TimeColumn] = None,
        time_converter: t.Optional[t.Callable[[TimeLike], exp.Expression]] = None,
        only_latest: bool = False,
    ):
        self._query = query
        self._dialect = dialect
        self._macro_definitions = macro_definitions
        self._path = path
        self._python_env = python_env or {}
        self._time_column = time_column
        self._time_converter = time_converter or (lambda v: exp.convert(v))
        self._only_latest = only_latest

        self._query_cache: t.Dict[
            t.Tuple[datetime, datetime, datetime], exp.Subqueryable
        ] = {}
        self._schema: t.Optional[MappingSchema] = None

    def render(
        self,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        add_incremental_filter: bool = False,
        snapshots: t.Optional[t.Dict[str, Snapshot]] = None,
        expand: t.Iterable[str] = tuple(),
        is_dev: bool = False,
        **kwargs: t.Any,
    ) -> exp.Subqueryable:
        """Renders a query, expanding macros with provided kwargs, and optionally expanding referenced models.

        Args:
            query: The query to render.
            start: The start datetime to render. Defaults to epoch start.
            end: The end datetime to render. Defaults to epoch start.
            latest: The latest datetime to use for non-incremental queries. Defaults to epoch start.
            add_incremental_filter: Add an incremental filter to the query if the model is incremental.
            snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
            expand: Expand referenced models as subqueries. This is used to bypass backfills when running queries
                that depend on materialized tables.  Model definitions are inlined and can thus be run end to
                end on the fly.
            query_key: A query key used to look up a rendered query in the cache.
            is_dev: Indicates whether the rendering happens in the development mode and temporary
                tables / table clones should be used where applicable.
            kwargs: Additional kwargs to pass to the renderer.

        Returns:
            The rendered expression.
        """
        from sqlmesh.core.snapshot import to_table_mapping

        dates = (
            *make_inclusive(start or c.EPOCH_DS, end or c.EPOCH_DS),
            to_datetime(latest or c.EPOCH_DS),
        )
        cache_key = dates

        snapshots = snapshots or {}
        mapping = to_table_mapping(snapshots.values(), is_dev)
        # if a snapshot is provided but not mapped, we need to expand it or the query
        # won't be valid
        expand = set(expand) | {name for name in snapshots if name not in mapping}

        query = self._query

        if cache_key not in self._query_cache:
            render_kwargs = {
                **date_dict(*dates, only_latest=self._only_latest),
                **kwargs,
            }

            if isinstance(query, d.Jinja):
                env = prepare_env(self._python_env)
                env[c.SQLMESH] = True

                try:
                    parsed_query = parse_one(
                        Environment()
                        .from_string("\n".join((*env["__jinja_macros__"], query.name)))
                        .render(**env, **render_kwargs),
                        read=self._dialect,
                    )
                    if not parsed_query:
                        raise ConfigError(f"Failed to parse a Jinja query {query}")
                    query = parsed_query
                except Exception as ex:
                    print(env)
                    raise ConfigError(
                        f"Invalid model query. {ex} at '{self._path}'"
                    ) from ex

            macro_evaluator = MacroEvaluator(self._dialect, env=env)
            macro_evaluator.locals.update(render_kwargs)

            for definition in self._macro_definitions:
                try:
                    macro_evaluator.evaluate(definition)
                except MacroEvalError as ex:
                    raise_config_error(
                        f"Failed to evaluate macro '{definition}'. {ex}", self._path
                    )

            try:
                self._query_cache[cache_key] = macro_evaluator.transform(query)  # type: ignore
            except MacroEvalError as ex:
                raise_config_error(
                    f"Failed to resolve macro for query. {ex}", self._path
                )

            if self._schema:
                # This takes care of expanding star projections

                try:
                    self._query_cache[cache_key] = optimize(
                        self._query_cache[cache_key],
                        schema=self._schema,
                        rules=RENDER_OPTIMIZER_RULES,
                    )
                except SchemaError:
                    pass
                except SqlglotError as ex:
                    raise_config_error(f"Invalid model query. {ex}", self._path)

                self._columns_to_types = {
                    expression.alias_or_name: expression.type
                    for expression in self._query_cache[cache_key].expressions
                }

        query = self._query_cache[cache_key]

        if expand:

            def _expand(node: exp.Expression) -> exp.Expression:
                if isinstance(node, exp.Table) and snapshots:
                    name = exp.table_name(node)
                    model = snapshots[name].model if name in snapshots else None
                    if name in expand and model and not model.is_seed:
                        return model.render_query(
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
        if add_incremental_filter:
            # expansion copies the query for us. if it doesn't occur, make sure to copy.
            if not expand:
                query = query.copy()
            for node, _, _ in query.walk(prune=lambda n, *_: isinstance(n, exp.Select)):
                if isinstance(node, exp.Select):
                    self.filter_time_column(node, *dates[0:2])

        if mapping:
            return exp.replace_tables(query, mapping)

        if not isinstance(query, exp.Subqueryable):
            raise_config_error(
                f"Query needs to be a SELECT or a UNION {query}.", self._path
            )

        return t.cast(exp.Subqueryable, query)

    @property
    def contains_star_query(self) -> bool:
        """Returns True if the model's query contains a star projection."""
        return any(
            isinstance(expression, exp.Star) for expression in self.render().expressions
        )

    def update_schema(self, schema: MappingSchema) -> None:
        self._schema = schema

        if self.contains_star_query:
            # We need to re-render in order to expand the star projection
            self._query_cache.clear()
            self.render()

    def filter_time_column(
        self, query: exp.Select, start: TimeLike, end: TimeLike
    ) -> None:
        """Filters a query on the time column to ensure no data leakage when running in incremental mode."""
        if not self._time_column:
            return

        low = self._time_converter(start)
        high = self._time_converter(end)

        time_column_identifier = exp.to_identifier(self._time_column.column)
        if time_column_identifier is None:
            raise_config_error(
                f"Time column '{self._time_column.column}' must be a valid identifier.",
                self._path,
            )
            raise

        time_column_projection = next(
            (
                select
                for select in query.selects
                if select.alias_or_name == self._time_column.column
            ),
            time_column_identifier,
        )

        if isinstance(time_column_projection, exp.Alias):
            time_column_projection = time_column_projection.this

        between = exp.Between(this=time_column_projection.copy(), low=low, high=high)

        if not query.args.get("group"):
            query.where(between, copy=False)
        else:
            query.having(between, copy=False)

        simplify(query)
