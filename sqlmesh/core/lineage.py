from __future__ import annotations

import typing as t
from collections import defaultdict
from functools import lru_cache

from sqlglot import exp
from sqlglot.helper import first
from sqlglot.lineage import Node
from sqlglot.lineage import lineage as sqlglot_lineage
from sqlglot.optimizer import Scope, build_scope

from sqlmesh.core.dialect import normalize_mapping_schema, normalize_model_name

if t.TYPE_CHECKING:
    from sqlmesh.core.context import Context
    from sqlmesh.core.model import Model


def _render_query(model: Model) -> exp.Query:
    """Render a model's query, adding in managed columns"""
    query = model.render_query_or_raise()
    if model.managed_columns:
        query.select(
            *[
                exp.alias_(exp.cast(exp.Null(), to=col_type), col)
                for col, col_type in model.managed_columns.items()
                if col not in query.named_selects
            ],
            append=True,
        )
    return query


@lru_cache()
def _build_scope(query: exp.Query) -> Scope:
    return build_scope(query)


def lineage(
    column: str | exp.Column,
    model: Model,
    trim_selects: bool = True,
    **kwargs: t.Any,
) -> Node:

    query = _render_query(model)

    return sqlglot_lineage(
        column,
        sql=query,
        schema=normalize_mapping_schema(model.mapping_schema, dialect=model.dialect),
        scope=_build_scope(query),
        trim_selects=trim_selects,
        dialect=model.dialect,
        **{
            "infer_schema": True,
            **kwargs,
        },
    )


def column_dependencies(context: Context, model_name: str, column: str) -> t.Dict[str, t.Set[str]]:
    model = context.get_model(model_name)
    parents = defaultdict(set)

    for node in lineage(column, model, trim_selects=False).walk():
        if node.downstream:
            continue

        table = node.expression.find(exp.Table)
        if table:
            name = normalize_model_name(
                table, default_catalog=context.default_catalog, dialect=model.dialect
            )
            parents[name].add(exp.to_column(node.name).name)
    return dict(parents)


def column_description(context: Context, model_name: str, column: str) -> t.Optional[str]:
    """Returns a column's description, inferring if needed."""
    model = context.get_model(model_name)

    if column in model.column_descriptions:
        return model.column_descriptions[column]

    dependencies = column_dependencies(context, model_name, column)

    if len(dependencies) != 1:
        return None

    parent, columns = first(dependencies.items())

    if len(columns) != 1:
        return None

    return column_description(context, parent, first(columns))
