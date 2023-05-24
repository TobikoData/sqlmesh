import typing as t

from fastapi import APIRouter, Depends

from sqlmesh.core import constants as c
from sqlmesh.core.context import Context
from sqlmesh.core.environment import Environment
from web.server.exceptions import ApiException
from web.server.settings import get_loaded_context

router = APIRouter()


@router.get("")
async def get_environments(
    context: Context = Depends(get_loaded_context),
) -> t.Dict[str, Environment]:
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
            start_at=0,
            plan_id="",
        )
    return environments
