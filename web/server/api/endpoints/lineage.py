from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.lineage import Node, lineage

from sqlmesh.core.context import Context
from web.server.api.endpoints.models import get_models_and_columns

Lineage = t.NewType("Lineage", t.Dict[str, t.Dict[str, t.Dict[str, t.List[str]]]])


def get_column_lineage(context: Context) -> Lineage:
    lineage: t.Any = dict()
    nodes = _get_nodes(context)

    for model_name in nodes:
        for column_name in nodes[model_name]:
            for i, node in enumerate(nodes[model_name][column_name].walk()):
                if i > 0 and _get_table(node):
                    _walk_node(lineage, node)

    return lineage


def _walk_node(
    lineage: t.Any,
    node: Node,
) -> None:
    """Walk a node and add it to the lineage graph"""
    node_model, node_column = _get_model_and_column(node)

    for child_node in node.downstream:
        child_model, child_column = _get_model_and_column(child_node)

        _add_column_to_graph(lineage, node_model, node_column, child_model, child_column)


def _add_column_to_graph(
    lineage: t.Any, node_model: str, node_column: str, child_model: str, child_column: str
) -> None:
    """Add a column to the lineage graph"""
    if not node_model in lineage:
        lineage[node_model] = {}

    if not node_column in lineage[node_model]:
        lineage[node_model][node_column] = {}

    if not child_model in lineage[node_model][node_column]:
        lineage[node_model][node_column][child_model] = []

    lineage[node_model][node_column][child_model].append(child_column)


def _get_nodes(context: Context) -> t.Dict[str, t.Dict[str, Node]]:
    """Get a mapping of model names to column names to lineage nodes"""
    models = get_models_and_columns(context)
    nodes: t.Dict[str, t.Dict[str, Node]] = {model.name: {} for model in models}

    for model in models:
        for column in model.columns:
            nodes[model.name][column.name] = lineage(
                column=column.name,
                sql=context.models[model.name].render_query(),
                sources={
                    model: context.models[model].render_query()
                    for model in context.dag.upstream(model.name)
                },
            )
    return nodes


def _get_table(node: Node) -> str:
    """Get a node's table/source"""
    if isinstance(node.expression, exp.Table):
        return exp.table_name(node.expression)
    else:
        return node.alias


def _get_model_and_column(node: Node) -> t.Tuple[str, str]:
    """Get a model and column"""
    table = _get_table(node)
    col = exp.to_column(node.name).name

    return (table, col)
