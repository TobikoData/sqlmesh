from __future__ import annotations

import sys
import traceback
import typing as t

from fastapi import APIRouter, Depends, HTTPException
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
from sqlmesh.core.model import Model
from sqlmesh.utils.date import now, now_timestamp, to_datetime
from web.server import models
from web.server.models import Error
from web.server.settings import get_loaded_context

router = APIRouter()


@router.get(
    "",
    response_model=t.List[models.Model],
    response_model_exclude_unset=True,
    response_model_exclude_none=True,
)
def get_models(
    context: Context = Depends(get_loaded_context),
) -> t.List[models.Model]:
    """Get a mapping of model names to model metadata"""
    output = []

    def _get_model_type(model: Model) -> str | None:
        if model.is_sql:
            return "sql"
        if model.is_python:
            return "python"
        if model.is_seed:
            return "seed"
        return None

    try:
        for model in context.models.values():
            type = _get_model_type(model)
            dialect = model.dialect if model.dialect else "Default"
            time_column = (
                f"{model.time_column.column} | {model.time_column.format}"
                if model.time_column
                else None
            )
            tags = ", ".join(model.tags) if model.tags else None
            partitioned_by = ", ".join(model.partitioned_by) if model.partitioned_by else None
            lookback = model.lookback if model.lookback > 0 else None
            columns = [
                models.Column(
                    name=name, type=str(data_type), description=model.column_descriptions.get(name)
                )
                for name, data_type in model.columns_to_types.items()
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
                partitioned_by=partitioned_by,
                lookback=lookback,
                cron_prev=to_datetime(model.cron_prev(value=now())),
                cron_next=to_datetime(model.cron_next(value=now())),
                interval_unit=model.interval_unit(),
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
                    sql=model.render_query().sql(pretty=True, dialect=model.dialect),
                    type=type,
                )
            )

        return output
    except Exception:
        error_type, error_value, error_traceback = sys.exc_info()

        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            detail=Error(
                timestamp=now_timestamp(),
                status=HTTP_422_UNPROCESSABLE_ENTITY,
                message="Unable to get environments",
                origin="API -> environments -> get_environments",
                description=str(error_value),
                type=str(error_type),
                traceback=traceback.format_exc(),
                stack=traceback.format_tb(error_traceback),
            ).dict(),
        )
