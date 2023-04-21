from __future__ import annotations

import collections
import traceback
import typing as t

from fastapi import APIRouter, Depends, HTTPException
from sqlglot import exp
from sqlglot.lineage import Node, lineage
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
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
        source = node.source.transform(
            lambda n: exp.Tag(this=n, prefix="<b>", postfix="</b>") if n is node.expression else n,
            copy=False,
        ).sql(pretty=True, dialect=dialect)
    return source


def _process_downstream(
    downstream: t.List[Node], node_column: str, cache_column_names: t.Dict[str, str]
) -> t.Dict[str, t.List[str]]:
    """Aggregate a list of downstream nodes by table/source"""
    graph = collections.defaultdict(list)
    for node in downstream:
        column = exp.to_column(node.name).name
        table = _get_table(node)
        if not column:
            continue
        if not table:
            cache_column_names[column] = node_column
            continue
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
        node = lineage(
            column=column_name,
            sql=context.models[model_name].render_query(),
            sources={
                model: context.models[model].render_query()
                for model in context.dag.upstream(model_name)
            },
        )
    except Exception:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=traceback.format_exc()
        )

    graph = {}
    table = model_name
    cache_column_names: t.Dict[str, str] = {}

    for i, node in enumerate(node.walk()):
        if i > 0:
            table = _get_table(node) or model_name
            column_name = exp.to_column(node.name).name
        if column_name in cache_column_names:
            column_name = cache_column_names[column_name]
        graph[table] = {
            column_name: LineageColumn(
                source=_get_node_source(node=node, dialect=context.models[table].dialect),
                models=_process_downstream(
                    node.downstream,
                    column_name,
                    cache_column_names,
                ),
            )
        }
    return graph


@router.get("/{model_name:str}")
async def model_lineage(
    model_name: str,
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, t.Set[str]]:
    """Get a model's lineage"""
    return context.dag.lineage(model_name).graph
