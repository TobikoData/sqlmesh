import asyncio
import traceback
import typing as t
from pathlib import Path

from fastapi import Depends, HTTPException
from starlette.status import HTTP_404_NOT_FOUND, HTTP_422_UNPROCESSABLE_ENTITY

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


def replace_file(src: Path, dst: Path) -> None:
    """Move a file or directory at src to dst."""
    if src != dst:
        try:
            src.replace(dst)
        except FileNotFoundError:
            raise HTTPException(status_code=HTTP_404_NOT_FOUND)
        except OSError:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=traceback.format_exc()
            )
