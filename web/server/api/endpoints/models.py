from __future__ import annotations

import pathlib
import typing as t

from fastapi import APIRouter, Depends, HTTPException
from sqlglot import exp
from starlette.status import HTTP_404_NOT_FOUND

from sqlmesh.core.context import Context
from sqlmesh.core.model import Model
from sqlmesh.utils.date import now, to_datetime
from web.server import models
from web.server.settings import get_loaded_context

router = APIRouter()


@router.get(
    "",
    response_model=t.Union[t.List[models.Model], models.ApiExceptionPayload],
    response_model_exclude_unset=True,
    response_model_exclude_none=True,
)
def get_models(
    context: Context = Depends(get_loaded_context),
) -> t.List[models.Model]:
    """Get a list of models"""
    return serialize_all_models(context)


@router.get(
    "/{name:str}",
    response_model=models.Model,
    response_model_exclude_unset=True,
    response_model_exclude_none=True,
)
def get_model(
    name: str,
    context: Context = Depends(get_loaded_context),
) -> models.Model:
    """Get a single model"""
    model = context.get_model(name)
    if not model:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)
    return serialize_model(model, context.path, render_query=True)


def serialize_all_models(
    context: Context, render_queries: t.Optional[t.Set[str]] = None
) -> t.List[models.Model]:
    render_queries = render_queries or set()
    return sorted(
        [
            serialize_model(model, context.path, model.name in render_queries)
            for model in context.models.values()
        ],
        key=lambda model: model.name,
    )


def serialize_model(
    model: Model, context_path: pathlib.Path, render_query: bool = False
) -> models.Model:
    type = _get_model_type(model)
    default_catalog = model.default_catalog
    dialect = model.dialect or "SQLGlot"
    time_column = (
        f"{model.time_column.column} | {model.time_column.format}" if model.time_column else None
    )
    tags = ", ".join(model.tags) if model.tags else None
    partitioned_by = (
        ", ".join(expr.sql(pretty=True, dialect=model.dialect) for expr in model.partitioned_by)
        if model.partitioned_by
        else None
    )
    clustered_by = ", ".join(model.clustered_by) if model.clustered_by else None
    lookback = model.lookback if model.lookback > 0 else None
    columns_to_types = model.columns_to_types or {}
    columns = [
        models.Column(
            name=name, type=str(data_type), description=model.column_descriptions.get(name)
        )
        for name, data_type in columns_to_types.items()
    ]
    details = models.ModelDetails(
        owner=model.owner,
        kind=model.kind.name,
        batch_size=model.batch_size,
        cron=model.cron,
        stamp=model.stamp,
        start=model.start,
        retention=model.retention,
        storage_format=model.storage_format,
        time_column=time_column,
        tags=tags,
        references=[
            models.Reference(name=ref.name, expression=ref.expression.sql(), unique=ref.unique)
            for ref in model.all_references
        ],
        partitioned_by=partitioned_by,
        clustered_by=clustered_by,
        lookback=lookback,
        cron_prev=to_datetime(model.cron_prev(value=now())),
        cron_next=to_datetime(model.cron_next(value=now())),
        interval_unit=model.interval_unit,
        annotated=model.annotated,
    )

    sql = None
    if render_query:
        query = model.render_query() or (
            model.query if hasattr(model, "query") else exp.select('"FAILED TO RENDER QUERY"')
        )
        sql = query.sql(pretty=True, dialect=model.dialect)

    return models.Model(
        name=model.name,
        fqn=model.fqn,
        path=str(model._path.relative_to(context_path)),
        dialect=dialect,
        columns=columns,
        details=details,
        description=model.description,
        sql=sql,
        type=type,
        default_catalog=default_catalog,
    )


def _get_model_type(model: Model) -> str:
    if model.is_sql:
        return models.ModelType.SQL
    if model.is_python:
        return models.ModelType.PYTHON
    if model.is_seed:
        return models.ModelType.SEED
    return models.ModelType.EXTERNAL
