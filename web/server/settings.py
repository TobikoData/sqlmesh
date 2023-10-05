from __future__ import annotations

import asyncio
import logging
from functools import lru_cache
from pathlib import Path

from fastapi import Depends

from sqlmesh.core.context import Context
from sqlmesh.core.model.definition import Model
from sqlmesh.utils.pydantic import PYDANTIC_MAJOR_VERSION
from web.server.exceptions import ApiException
from web.server.models import FileType

if PYDANTIC_MAJOR_VERSION >= 2:
    from pydantic_settings import BaseSettings  # type: ignore
else:
    from pydantic import BaseSettings  # type: ignore


logger = logging.getLogger(__name__)
get_context_lock = asyncio.Lock()


class Settings(BaseSettings):
    project_path: Path = Path("examples/sushi")
    config: str = ""
    gateway: str = ""


@lru_cache()
def get_settings() -> Settings:
    return Settings()


@lru_cache()
def _get_context(path: str | Path, config: str, gateway: str) -> Context:
    from web.server.main import api_console

    return Context(paths=str(path), config=config, console=api_console, gateway=gateway, load=False)


@lru_cache()
def _get_path_mappings(context: Context) -> dict[Path, FileType]:
    mapping: dict[Path, FileType] = {}
    for audit in context._audits.values():
        if audit._path:
            path = audit._path.relative_to(context.path)
            mapping[path] = FileType.audit
    for model in context.models.values():
        if model.kind.is_external or not model._path:
            continue
        path = model._path.relative_to(context.path)
        mapping[path] = FileType.model
    return mapping


@lru_cache()
def _get_loaded_context(path: str | Path, config: str, gateway: str) -> Context:
    print(config)
    context = _get_context(path, config, gateway)
    context.load()
    return context


@lru_cache()
def _get_path_to_model_mapping(context: Context) -> dict[Path, Model]:
    return {model._path: model for model in context._models.values()}


async def get_path_mapping(settings: Settings = Depends(get_settings)) -> dict[Path, FileType]:
    try:
        context = await get_loaded_context(settings)
    except Exception:
        logger.exception("Error creating a context")
        return {}
    return _get_path_mappings(context)


async def get_path_to_model_mapping(
    settings: Settings = Depends(get_settings),
) -> dict[Path, Model]:
    try:
        context = await get_loaded_context(settings)
    except Exception:
        logger.exception("Error creating a context")
        return {}
    return _get_path_to_model_mapping(context)


async def get_loaded_context(settings: Settings = Depends(get_settings)) -> Context:
    loop = asyncio.get_running_loop()

    try:
        async with get_context_lock:
            return await loop.run_in_executor(
                None,
                _get_loaded_context,
                settings.project_path,
                settings.config,
                settings.gateway,
            )
    except Exception:
        raise ApiException(
            message="Unable to create a loaded context",
            origin="API -> settings -> get_loaded_context",
        )


async def get_context(settings: Settings = Depends(get_settings)) -> Context:
    try:
        async with get_context_lock:
            return _get_context(settings.project_path, settings.config, settings.gateway)
    except Exception:
        raise ApiException(
            message="Unable to create a context",
            origin="API -> settings -> get_context",
        )
