import typing as t

from fastapi import APIRouter, Depends

from sqlmesh.core.context import Context
from sqlmesh.core.environment import Environment
from web.server.settings import get_loaded_context

router = APIRouter()


@router.get("")
def get_environments(context: Context = Depends(get_loaded_context)) -> t.Dict[str, Environment]:
    """Get the environments"""

    return {env.name: env for env in context.state_reader.get_environments()}
