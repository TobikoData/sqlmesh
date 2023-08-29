from fastapi import APIRouter, Depends

from sqlmesh.core import constants as c
from sqlmesh.core.context import Context
from sqlmesh.core.environment import Environment
from web.server.exceptions import ApiException
from web.server.models import ApiEnvironmentsPayload
from web.server.settings import get_loaded_context

router = APIRouter()


@router.get("", response_model=ApiEnvironmentsPayload)
async def get_environments(
    context: Context = Depends(get_loaded_context),
) -> ApiEnvironmentsPayload:
    """Get the environments"""
    try:
        environments = {env.name: env for env in context.state_reader.get_environments()}
    except Exception:
        raise ApiException(
            message="Unable to get environments",
            origin="API -> environments -> get_environments",
        )

    if c.PROD not in environments:
        environments[c.PROD] = Environment(
            name=c.PROD,
            snapshots=[],
            start_at=c.EPOCH,
            plan_id="",
        )
    if context.config.default_target_environment not in environments:
        environments[context.config.default_target_environment] = Environment(
            name=context.config.default_target_environment,
            snapshots=[],
            start_at=c.EPOCH,
            plan_id="",
        )
    return ApiEnvironmentsPayload(
        environments=environments,
        pinned_environments=context.config.pinned_environments,
        default_target_environment=context.config.default_target_environment,
    )


@router.delete("/{environment:str}")
async def delete_environment(
    context: Context = Depends(get_loaded_context),
    environment: str = "",
) -> bool:
    """Invalidate and delete an environment"""
    try:
        context.state_sync.invalidate_environment(environment)
        context.state_sync.delete_expired_environments()
        return True
    except Exception:
        raise ApiException(
            message="Unable to delete environments",
            origin="API -> environments -> delete_environment",
        )
