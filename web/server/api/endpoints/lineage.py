from __future__ import annotations

import typing as t
from collections import defaultdict

from fastapi import APIRouter, Depends

from sqlmesh.core.context import Context
from sqlmesh.core.lineage import column_dependencies
from web.server.exceptions import ApiException
from web.server.models import LineageColumn
from web.server.settings import get_loaded_context

if t.TYPE_CHECKING:
    pass


router = APIRouter()


@router.get("/{model_name:str}/{column_name:str}")
async def column_lineage(
    column_name: str,
    model_name: str,
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, t.Dict[str, LineageColumn]]:
    """Get a column's lineage"""
    try:
        model = context.get_model(model_name).fqn
    except Exception:
        raise ApiException(
            message="Unable to get column lineage",
            origin="API -> lineage -> column_lineage",
        )

    graph: t.Dict[str, t.Dict[str, LineageColumn]] = defaultdict(dict)
    nodes = [(model, column_name)]
    while nodes:
        model, column = nodes.pop(0)
        dependencies = defaultdict(list)
        for table, column_names in column_dependencies(context, model, column).items():
            for column_name in column_names:
                if column_name == "*":
                    continue
                dependencies[table].append(column_name)
                nodes.append((table, column_name))

        graph[model][column] = LineageColumn(
            expression="",
            source="",
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
