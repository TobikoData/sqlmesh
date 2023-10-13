from __future__ import annotations

import collections
import typing as t

from fastapi import APIRouter, Depends
from sqlglot import exp
from sqlglot.lineage import Node, lineage

from sqlmesh import Snapshot
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import normalize_model_name
from web.server.exceptions import ApiException
from web.server.models import LineageColumn
from web.server.settings import get_loaded_context

router = APIRouter()


def _get_table(node: Node) -> str:
    """Get a node's table/source"""
    if isinstance(node.expression, exp.Table):
        return exp.table_name(node.expression)
    else:
        return node.alias


def _get_node_source(node: Node, dialect: str) -> str:
    """Get a node's source"""
    if isinstance(node.expression, exp.Table):
        source = f"SELECT {node.name} FROM {node.expression.this}"
    else:
        source = node.source.sql(pretty=True, dialect=dialect)
    return source


def _process_downstream(
    downstream: t.List[Node], model_fqn_to_snapshot: t.Dict[str, Snapshot]
) -> t.Dict[str, t.List[str]]:
    """Aggregate a list of downstream nodes by table/source"""
    graph = collections.defaultdict(list)
    for node in downstream:
        column = exp.to_column(node.name).name
        table = _get_table(node) or node.name
        table = model_fqn_to_snapshot[table].name if table in model_fqn_to_snapshot else table
        graph[table].append(column)
    return graph


@router.get("/{model_name:str}/{column_name:str}")
async def column_lineage(
    column_name: str,
    model_name: str,
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, t.Dict[str, LineageColumn]]:
    """Get a column's lineage"""
    model_fqn = normalize_model_name(model_name, context.default_catalog, context.config.dialect)
    try:
        node = lineage(
            column=column_name,
            sql=context.models[model_fqn].render_query_or_raise(),
            sources={
                model: context.models[model].render_query_or_raise()
                for model in context.dag.upstream(model_fqn)
                if model in context.models
            },
            dialect=context.models[model_fqn].dialect,
        )
    except Exception:
        raise ApiException(
            message="Unable to get a column lineage",
            origin="API -> lineage -> column_lineage",
        )

    graph: t.Dict[str, t.Dict[str, LineageColumn]] = {}
    node_name = model_name

    for i, node in enumerate(node.walk()):
        if i > 0:
            node_name = _get_table(node) or node.name
            node_name = (
                context.snapshots[node_name].name if node_name in context.snapshots else node_name
            )
            column_name = exp.to_column(node.name).name
        if column_name in graph.get(node_name, []):
            continue

        node_fqn = normalize_model_name(node_name, context.default_catalog, context.config.dialect)
        dialect = context.models[node_fqn].dialect if node_fqn in context.models else ""
        graph[node_name] = {
            column_name: LineageColumn(
                expression=node.expression.sql(pretty=True, dialect=dialect),
                source=_get_node_source(node=node, dialect=dialect),
                models=_process_downstream(node.downstream, context.snapshots),
            )
        }

    return graph


@router.get("/{model_name:str}")
async def model_lineage(
    model_name: str,
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, t.Set[str]]:
    """Get a model's lineage"""
    model_name = normalize_model_name(model_name, context.default_catalog, context.config.dialect)
    graph = context.dag.lineage(model_name).graph
    return {
        context.snapshots[fqn].name: {context.snapshots[dep].name for dep in deps}
        for fqn, deps in graph.items()
    }
