from fastapi import APIRouter, Depends, Request

from sqlmesh.cli.main import _sqlmesh_version
from sqlmesh.core.context import Context
from web.server import models
from web.server.console import ApiConsole
from web.server.settings import get_loaded_context

router = APIRouter()


@router.get(
    "",
    response_model=models.Meta,
    response_model_exclude_unset=True,
)
def get_api_meta(
    request: Request,
    context: Context = Depends(get_loaded_context),
) -> models.Meta:
    """Get the metadata"""
    console: ApiConsole = context.console  # type: ignore
    console.log_event_apply()
    return models.Meta(
        version=_sqlmesh_version(),
        has_running_task=hasattr(request.app.state, "task") and not request.app.state.task.done(),
    )
