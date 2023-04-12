from __future__ import annotations

import collections
import traceback
import typing as t

from fastapi import APIRouter, Depends, HTTPException
from sqlglot import exp
from sqlglot.lineage import Node, lineage
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
from web.server.settings import get_loaded_context

router = APIRouter()


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


@router.get("/")
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
