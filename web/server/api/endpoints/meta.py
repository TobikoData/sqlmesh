from __future__ import annotations

import enum

from fastapi import APIRouter, Request

from sqlmesh.cli.main import _sqlmesh_version
from web.server import models
from web.server.console import api_console

router = APIRouter()


class Mode(str, enum.Enum):
    ALL = "all"  # Allow all modules
    DOCS = "docs"  # Only docs module
    READ_ONLY = (
        "read-only"  # Allow docs, errors, environments and plan progress but not apply plans
    )
    PLAN = "plan"  # Allow docs and plan related modules but omits editor module


mode_to_modules = {
    Mode.READ_ONLY: [
        models.Modules.DOCS,
        models.Modules.ERRORS,
        models.Modules.ENVIRONMENTS,
        models.Modules.PLAN_PROGRESS,
    ],
    Mode.DOCS: [models.Modules.DOCS],
    Mode.PLAN: [
        models.Modules.DOCS,
        models.Modules.ERRORS,
        models.Modules.PLANS,
        models.Modules.PLAN_PROGRESS,
        models.Modules.ENVIRONMENTS,
    ],
    Mode.ALL: [
        models.Modules.EDITOR,
        models.Modules.DOCS,
        models.Modules.ERRORS,
        models.Modules.PLANS,
        models.Modules.PLAN_PROGRESS,
        models.Modules.ENVIRONMENTS,
    ],
}


@router.get(
    "",
    response_model=models.Meta,
    response_model_exclude_unset=True,
)
def get_api_meta(
    request: Request,
) -> models.Meta:
    """Get the metadata"""
    api_console.log_event_plan_cancel()
    api_console.log_event_plan_overview()
    api_console.log_event_plan_apply()

    return models.Meta(
        version=_sqlmesh_version(),
        has_running_task=hasattr(request.app.state, "task") and not request.app.state.task.done(),
        modules=mode_to_modules[Mode.ALL],
    )
