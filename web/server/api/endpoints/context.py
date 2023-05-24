from fastapi import APIRouter, Depends

from sqlmesh.core.context import Context
from web.server import models
from web.server.settings import Settings, get_loaded_context, get_settings

router = APIRouter()


@router.get(
    "",
    response_model=models.Context,
    response_model_exclude_unset=True,
)
def get_api_context(
    context: Context = Depends(get_loaded_context),
    settings: Settings = Depends(get_settings),
) -> models.Context:
    """Get the context"""

    return models.Context(
        concurrent_tasks=context.concurrent_tasks,
        engine_adapter=context.engine_adapter.dialect,
        scheduler=context.config.get_scheduler(context.gateway).type_,
        time_column_format=context.config.time_column_format,
        models=list(context.models),
        config=settings.config,
    )
