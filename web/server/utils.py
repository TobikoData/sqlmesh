import asyncio
import typing as t

from fastapi import Depends, HTTPException
from starlette.status import HTTP_404_NOT_FOUND

from sqlmesh.core.context import Context
from web.server.settings import get_context


async def run_in_executor(func: t.Callable) -> t.Any:
    """Run in the default loop's executor"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func)


def validate_path(
    path: str,
    context: Context = Depends(get_context),
) -> str:
    resolved_path = context.path.resolve()
    full_path = (resolved_path / path).resolve()
    try:
        full_path.relative_to(resolved_path)
    except ValueError:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)

    if any(full_path.match(pattern) for pattern in context.ignore_patterns):
        raise HTTPException(status_code=HTTP_404_NOT_FOUND)

    return path
