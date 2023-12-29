from __future__ import annotations

import typing as t

from fastapi import APIRouter, Depends, HTTPException
from sqlglot import exp
from starlette.status import HTTP_404_NOT_FOUND

from sqlmesh.core.context import Context
from sqlmesh.core.dialect import normalize_model_name
from sqlmesh.core.model import Model
from sqlmesh.utils.date import now, to_datetime
from web.server import models
from web.server.settings import get_loaded_context

router = APIRouter()


@router.get(
    "/{model_name:str}",
    response_model=models.Model,
    response_model_exclude_unset=True,
    response_model_exclude_none=True,
)
def get_model(
    model_name: str,
    context: Context = Depends(get_loaded_context),
) -> models.Model:
    """Get a file, including its contents."""
    model_name = normalize_model_name(
        model_name, default_catalog=context.default_catalog, dialect=context.engine_adapter.dialect
    )
    try:
        model = get_all_model_full(context.models[model_name], context)
    except FileNotFoundError:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)

    return model


def get_all_models(context: Context) -> t.List[models.Model]:
    return [get_all_model_base(model, context) for model in context.models.values()]

    def _get_model_type(model: Model) -> str:
        if model.is_sql:
            return models.ModelType.SQL
        if model.is_python:
            return models.ModelType.PYTHON
        if model.is_seed:
            return models.ModelType.SEED
        return models.ModelType.EXTERNAL


def get_all_model_base(model: Model, context: Context) -> models.Model:
    return models.Model(
        name=model.name,
        path=str(model._path.relative_to(context.path)),
        type=get_model_type(model),
        dialect=model.dialect if model.dialect else "Default",
    )


def get_all_model_full(model: Model, context: Context) -> models.Model:
    type = get_model_type(model)
    default_catalog = model.default_catalog
    dialect = model.dialect if model.dialect else "Default"
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

    query = model.render_query() or (
        model.query if hasattr(model, "query") else exp.select('"FAILED TO RENDER QUERY"')
    )

    return models.Model(
        name=model.name,
        path=str(model._path.relative_to(context.path)),
        dialect=dialect,
        columns=columns,
        details=details,
        description=model.description,
        sql=query.sql(pretty=True, dialect=model.dialect),
        type=type,
        default_catalog=default_catalog,
    )


def get_model_type(model: Model) -> str:
    if model.is_sql:
        return "sql"
    if model.is_python:
        return "python"
    if model.is_seed:
        return "seed"
    return "external"
