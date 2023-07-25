from __future__ import annotations

import typing as t

from fastapi import APIRouter, Depends

from sqlmesh.core.context import Context
from sqlmesh.core.model import Model
from sqlmesh.utils.date import now, to_datetime
from web.server import models
from web.server.exceptions import ApiException
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
    """Get a mapping of model names to model metadata"""
    try:
        context.load()
    except Exception:
        raise ApiException(
            message="Unable to get models",
            origin="API -> models -> get_models",
        )

    return get_all_models(context)


def get_all_models(context: Context) -> t.List[models.Model]:
    output = []

    def _get_model_type(model: Model) -> str:
        if model.is_sql:
            return "sql"
        if model.is_python:
            return "python"
        if model.is_seed:
            return "seed"
        return "external"

    for model in context.models.values():
        type = _get_model_type(model)
        dialect = model.dialect if model.dialect else "Default"
        time_column = (
            f"{model.time_column.column} | {model.time_column.format}"
            if model.time_column
            else None
        )
        tags = ", ".join(model.tags) if model.tags else None
        grain = ", ".join(model.grain) if model.grain else None
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
            grain=grain,
            partitioned_by=partitioned_by,
            clustered_by=clustered_by,
            lookback=lookback,
            cron_prev=to_datetime(model.cron_prev(value=now())),
            cron_next=to_datetime(model.cron_next(value=now())),
            interval_unit=model.interval_unit,
            annotated=model.annotated,
        )

        output.append(
            models.Model(
                name=model.name,
                path=str(model._path.relative_to(context.path)),
                dialect=dialect,
                columns=columns,
                details=details,
                description=model.description,
                sql=model.render_query_or_raise().sql(pretty=True, dialect=model.dialect),
                type=type,
            )
        )

    return output
