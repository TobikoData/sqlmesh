import typing as t

from fastapi import APIRouter, Depends

from sqlmesh.core.context import Context
from sqlmesh.utils.date import now, to_datetime
from web.server import models
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
    return [
        models.Model(
            name=model.name,
            path=str(model._path.relative_to(context.path)),
            dialect=model.dialect if model.dialect else "Default",
            columns=[
                models.Column(
                    name=name, type=str(data_type), description=model.column_descriptions.get(name)
                )
                for name, data_type in model.columns_to_types.items()
            ],
            details=models.ModelDetails(
                owner=model.owner,
                kind=model.kind.name,
                batch_size=model.batch_size,
                cron=model.cron,
                stamp=model.stamp,
                start=model.start,
                retention=model.retention,
                storage_format=model.storage_format,
                time_column=f"{model.time_column.column} | {model.time_column.format}"
                if model.time_column
                else None,
                tags=", ".join(model.tags) if model.tags else None,
                partitioned_by=", ".join(model.partitioned_by) if model.partitioned_by else None,
                lookback=model.lookback if model.lookback > 0 else None,
                cron_prev=to_datetime(model.cron_prev(value=now())),
                cron_next=to_datetime(model.cron_next(value=now())),
                interval_unit=model.interval_unit(),
                annotated=model.annotated,
                contains_star_query=model.contains_star_query,
            ),
            description=model.description,
            sql=model.render_query().sql(pretty=True, dialect=model.dialect),
            type="sql"
            if model.is_sql
            else "python"
            if model.is_python
            else "seed"
            if model.is_seed
            else None,
        )
        for model in context.models.values()
    ]
