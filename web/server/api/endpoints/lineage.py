from __future__ import annotations

import typing as t
from collections import defaultdict

from fastapi import APIRouter, Depends
from sqlglot import exp
from sqlglot.lineage import lineage

from sqlmesh.core.context import Context
from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.lineage import _render_query
from web.server.exceptions import ApiException
from web.server.models import LineageColumn
from web.server.settings import get_loaded_context

if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from sqlglot.lineage import Node


router = APIRouter()


def get_node_source(node: Node, dialect: DialectType) -> str:
    """Get a node's source"""
    if isinstance(node.expression, exp.Table):
        source = f"SELECT {node.name} FROM {node.expression.this}"
    else:
        source = node.source.sql(pretty=True, dialect=dialect)
    return source


def get_source_name(node: Node, default_catalog: t.Optional[str], dialect: str) -> str:
    table = node.expression.find(exp.Table)
    if table:
        return normalize_model_name(table, default_catalog=default_catalog, dialect=dialect)
    if node.reference_node_name:
        # CTE name
        return node.reference_node_name
    return ""


def get_column_name(node: Node) -> str:
    if isinstance(node.expression, exp.Alias):
        return node.expression.alias_or_name
    return exp.to_column(node.name).name


@router.get("/{model_name:str}/{column_name:str}")
async def column_lineage(
    column_name: str,
    model_name: str,
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, t.Dict[str, LineageColumn]]:
    """Get a column's lineage"""
    try:
        model_name = context.get_model(model_name).fqn
    except Exception:
        raise ApiException(
            message="Unable to get column lineage",
            origin="API -> lineage -> column_lineage",
        )

    graph: t.Dict[str, t.Dict[str, LineageColumn]] = defaultdict(dict)
    nodes = [(model_name, column_name)]
    while nodes:
        model_name, column = nodes.pop(0)
        model = context.get_model(model_name)
        if not model:
            # External model
            graph[model_name][column] = LineageColumn(
                expression=f"FROM {model_name}",
                source=f"SELECT {column} FROM {model_name}",
                models={},
            )
            continue
        root = lineage(
            column, sql=_render_query(model), schema=model.mapping_schema, infer_schema=True
        )

        for node in root.walk():
            if root.name == "UNION" and node is root:
                continue
            node_name = (
                get_source_name(
                    node, default_catalog=context.default_catalog, dialect=model.dialect
                )
                or model_name
            )
            node_column = get_column_name(node)
            if node_column in graph[node_name]:
                dependencies = defaultdict(set, graph[node_name][column].models)
            else:
                dependencies = defaultdict(set)
            for d in node.downstream:
                table = get_source_name(
                    d, default_catalog=context.default_catalog, dialect=model.dialect
                )
                column_name = get_column_name(d)
                if table and column_name != "*":
                    dependencies[table].add(column_name)
                    if not d.downstream:
                        nodes.append((table, column_name))

            graph[node_name][node_column] = LineageColumn(
                expression=node.expression.sql(pretty=True, dialect=model.dialect),
                source=node.source.sql(pretty=True, dialect=model.dialect),
                models=dependencies,
            )
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
            message="Unable to get model lineage",
            origin="API -> lineage -> model_lineage",
        )

    return context.dag.lineage(model_name).graph
