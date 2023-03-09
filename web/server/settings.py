from __future__ import annotations

import asyncio
import logging
import traceback
from functools import lru_cache
from pathlib import Path

from fastapi import Depends, HTTPException
from pydantic import BaseSettings
from starlette.status import HTTP_422_UNPROCESSABLE_ENTITY

from sqlmesh.core.context import Context
from web.server.models import FileType

logger = logging.getLogger(__name__)
get_context_lock = asyncio.Lock()


class Settings(BaseSettings):
    project_path: Path = Path("examples/sushi")
    config: str = ""


@lru_cache()
def get_settings() -> Settings:
    return Settings()


@lru_cache()
def _get_context(path: str, config: str) -> Context:
    from web.server.main import api_console

    return Context(path=path, config=config, console=api_console, load=False)


@lru_cache()
def _get_loaded_context(path: str, config: str) -> Context:
    context = _get_context(path, config)
    context.load()
    return context


@lru_cache()
def _get_path_mappings(context: Context) -> dict[Path, FileType]:
    mapping: dict[Path, FileType] = {}
    for audit in context._audits.values():
        path = audit._path.relative_to(context.path)
        mapping[path] = FileType.audit
    for model in context.models.values():
        path = model._path.relative_to(context.path)
        mapping[path] = FileType.model
    return mapping


async def get_loaded_context(settings: Settings = Depends(get_settings)) -> Context:
    loop = asyncio.get_running_loop()
    try:
        async with get_context_lock:
            return await loop.run_in_executor(
                None, _get_loaded_context, settings.project_path, settings.config
            )
    except Exception:
        raise HTTPException(
            status_code=HTTP_422_UNPROCESSABLE_ENTITY, detail=traceback.format_exc()
        )


async def get_context(settings: Settings = Depends(get_settings)) -> Context | None:
    try:
        async with get_context_lock:
            return _get_context(settings.project_path, settings.config)
    except Exception:
        logger.exception("Error creating a context")
    return None


async def get_path_mapping(settings: Settings = Depends(get_settings)) -> dict[Path, FileType]:
    try:
        context = await get_loaded_context(settings)
    except Exception:
        logger.exception("Error creating a context")
        return {}
    return _get_path_mappings(context)
