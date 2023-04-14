from __future__ import annotations

import collections
import traceback
import typing as t

from fastapi import APIRouter, Depends, HTTPException
from sqlglot import exp
from sqlglot.lineage import Node, lineage
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
from web.server.api.endpoints.models import (
    get_model_with_columns,
    get_models_with_columns,
)
from web.server.models import DAG
from web.server.settings import get_loaded_context

router = APIRouter()

Lineage = t.NewType("Lineage", t.Dict[str, t.Dict[str, t.Dict[str, t.List[str]]]])


@router.get("")
async def column_lineage(
    column: str,
    model: str,
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, t.Dict[str, t.Dict[str, t.List[str]]]]:
    """Get a column's lineage"""
    try:
        node = lineage(
            column=column,
            sql=context.models[model].render_query(),
            sources={
                model: context.models[model].render_query() for model in context.dag.upstream(model)
            },
        )
    except Exception:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=traceback.format_exc()
        )

    graph = {}
    table = model

    for i, node in enumerate(node.walk()):
        if i > 0:
            table = _get_table(node)
            column = exp.to_column(node.name).name
        graph[table] = {column: _process_downstream(node.downstream)}
    return graph


@router.get("/{model_name:str}")
async def model_column_lineage(
    model_name: str,
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, DAG]:
    """Get a model's column lineage"""
    columns = get_column_lineage(context, model_name)
    models = (
        set(key for column in columns[model_name].values() for key in column.keys())
        if model_name in columns
        else set()
    )

    models.add(model_name)

    for model in context.dag.graph[model_name]:
        models.add(model)

    return {
        name: DAG(
            models=list(model_names)
            if name == model_name
            else [model_name]
            if model_name in model_names
            else [],
            columns=columns[model_name] if name == model_name and model_name in columns else None,
        )
        for name, model_names in context.dag.graph.items()
        if name in models or model_name in model_names
    }


def get_column_lineage(context: Context, model_name: t.Optional[str] = None) -> Lineage:
    lineage: t.Any = dict()
    nodes = _get_nodes(context, model_name)

    for model_name in nodes:
        table = model_name
        for column_name in nodes[model_name]:
            column = column_name
            node = nodes[model_name][column_name]

            if len(node.downstream) == 0:
                continue

            for i, node in enumerate(node.walk()):
                if i > 0:
                    table, column = _get_model_and_column(node)
                if not table:
                    table = model_name
                _walk_node(lineage, node, table, column)
    return lineage


def _get_table(node: Node) -> str:
    """Get a node's table/source"""
    if isinstance(node.expression, exp.Table):
        return exp.table_name(node.expression)
    else:
        return node.alias


def _process_downstream(downstream: t.List[Node]) -> t.Dict[str, t.List[str]]:
    """Aggregate a list of downstream nodes by table/source"""
    graph = collections.defaultdict(list)
    for node in downstream:
        column = exp.to_column(node.name).name
        table = _get_table(node)
        graph[table].append(column)
    return graph


def _walk_node(lineage: t.Any, node: Node, node_model: str, node_column: str) -> None:
    """Walk a node and add it to the lineage graph"""

    for child_node in node.downstream:
        child_model, child_column = _get_model_and_column(child_node)
        if not child_model:
            continue
        _add_column_to_graph(lineage, node_model, node_column, child_model, child_column)


def _add_column_to_graph(
    lineage: t.Any, model: str, column: str, child_model: str, child_column: str
) -> None:
    """Add a column to the lineage graph"""
    if not model in lineage:
        lineage[model] = {}

    if not column in lineage[model]:
        lineage[model][column] = {}

    if not child_model in lineage[model][column]:
        lineage[model][column][child_model] = []

    if not child_column in lineage[model][column][child_model]:
        lineage[model][column][child_model].append(child_column)


def _get_nodes(
    context: Context, model_name: t.Optional[str] = None
) -> t.Dict[str, t.Dict[str, Node]]:
    """Get a mapping of model names to column names to lineage nodes"""
    models = (
        [get_model_with_columns(context, model_name)]
        if model_name is not None
        else get_models_with_columns(context)
    )
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


def _get_model_and_column(node: Node) -> t.Tuple[str, str]:
    """Get a model and column"""
    table = _get_table(node)
    col = exp.to_column(node.name).name

    return (table, col)
