from __future__ import annotations

import typing as t
from collections import defaultdict

from sqlglot import exp
from sqlglot.helper import first
from sqlglot.lineage import Node
from sqlglot.lineage import lineage as sqlglot_lineage

from sqlmesh.core.dialect import normalize_mapping_schema, normalize_model_name

if t.TYPE_CHECKING:
    from sqlmesh.core.context import Context
    from sqlmesh.core.model import Model


def _render_query(model: Model) -> exp.Query:
    """Render a model's query, adding in managed columns"""
    query = model.render_query_or_raise().copy()
    if model.managed_columns:
        query.select(
            *[
                exp.alias_(exp.cast(exp.Null(), to=col_type), col)
                for col, col_type in model.managed_columns.items()
                if col not in query.named_selects
            ],
            append=True,
            copy=False,
        )
    return query


def lineage(
    column: str | exp.Column,
    model: Model,
    **kwargs: t.Any,
) -> Node:
    return sqlglot_lineage(
        column,
        sql=_render_query(model),
        schema=normalize_mapping_schema(model.mapping_schema, dialect=model.dialect),
        dialect=model.dialect,
        **{
            "infer_schema": True,
            **kwargs,
        },
    )


def column_dependencies(context: Context, model_name: str, column: str) -> t.Dict[str, t.Set[str]]:
    model = context.get_model(model_name)
    parents = defaultdict(set)

    for node in lineage(column, model).walk():
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
