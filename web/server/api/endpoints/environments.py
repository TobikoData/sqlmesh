import sys
import traceback
import typing as t

from fastapi import APIRouter, Depends, HTTPException
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core import constants as c
from sqlmesh.core.context import Context
from sqlmesh.core.environment import Environment
from sqlmesh.utils.date import now_timestamp
from web.server.models import Error
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

    if c.PROD not in environments:
        environments[c.PROD] = Environment(
            name=c.PROD,
            snapshots=[],
            start_at=0,
            plan_id="",
        )
    return environments
