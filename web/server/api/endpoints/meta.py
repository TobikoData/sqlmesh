from fastapi import APIRouter, Request

from sqlmesh.cli.main import _sqlmesh_version
from web.server import models
from web.server.console import api_console

router = APIRouter()


@router.get(
    "",
    response_model=models.Meta,
    response_model_exclude_unset=True,
)
def get_api_meta(
    request: Request,
) -> models.Meta:
    """Get the metadata"""
    api_console.log_event_plan_apply()
    return models.Meta(
        version=_sqlmesh_version(),
        has_running_task=hasattr(request.app.state, "task") and not request.app.state.task.done(),
    )
