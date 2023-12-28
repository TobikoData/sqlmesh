from __future__ import annotations

import collections
import typing as t

from fastapi import APIRouter, Depends
from sqlglot import exp
from sqlglot.lineage import Node, lineage

from sqlmesh.core.context import Context
from web.server.exceptions import ApiException
from web.server.models import LineageColumn
from web.server.settings import get_loaded_context

router = APIRouter()


def _get_table(node: Node) -> str:
    """Get a node's table/source"""
    table: t.Union[exp.Table, str] = node.alias or node.name
    if isinstance(node.expression, exp.Table):
        table = node.expression

    return exp.table_name(table, identify=True)


def _get_node_source(node: Node, dialect: str) -> str:
    """Get a node's source"""
    if isinstance(node.expression, exp.Table):
        source = f"SELECT {node.name} FROM {node.expression.this}"
    else:
        source = node.source.sql(pretty=True, dialect=dialect)
    return source


def _process_downstream(downstream: t.List[Node]) -> t.Dict[str, t.List[str]]:
    """Aggregate a list of downstream nodes by table/source"""
    graph = collections.defaultdict(list)
    for node in downstream:
        column = exp.to_column(node.name).name
        table = _get_table(node)
        graph[table].append(column)
    return graph


@router.get("/{model_name:str}/{column_name:str}")
async def column_lineage(
    column_name: str,
    model_name: str,
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, t.Dict[str, LineageColumn]]:
    """Get a column's lineage"""
    try:
        model = context.get_model(model_name)
        node = lineage(
            column=column_name,
            sql=model.render_query_or_raise(),
            sources={
                m: context.models[m].render_query_or_raise()
                for m in context.dag.upstream(model.fqn)
                if m in context.models
            },
            dialect=model.dialect,
        )
    except Exception:
        raise ApiException(
            message="Unable to get a column lineage",
            origin="API -> lineage -> column_lineage",
        )

    graph: t.Dict[str, t.Dict[str, LineageColumn]] = {}
    node_name = model.fqn

    for i, node in enumerate(node.walk()):
        if i > 0:
            node_name = _get_table(node)
            column_name = exp.to_column(node.name).name
        if column_name in graph.get(node_name, []):
            continue

        # At this point node_name should be fqn/normalized/quoted
        dialect = context.models[node_name].dialect if node_name in context.models else ""
        graph[node_name] = {
            column_name: LineageColumn(
                expression=node.expression.sql(pretty=True, dialect=dialect),
                source=_get_node_source(node=node, dialect=dialect),
                models=_process_downstream(node.downstream),
            )
        }

    return graph


@router.get("/{model_name:str}")
async def model_lineage(
    model_name: str,
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, t.Set[str]]:
    """Get a model's lineage"""
    try:
        model_name = context.get_model(model_name).fqn
    except Exception:
        raise ApiException(
            message="Unable to get a model lineage",
            origin="API -> lineage -> model_lineage",
        )

    return context.dag.lineage(model_name).graph
