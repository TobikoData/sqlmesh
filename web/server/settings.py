from functools import lru_cache
from pathlib import Path

from fastapi import Depends
from pydantic import BaseSettings

from sqlmesh.core.console import ApiConsole
from sqlmesh.core.context import Context


class Settings(BaseSettings):
    project_path: Path = Path("examples/sushi")
    config: str = "local_config"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


@lru_cache()
def _get_context(path: str) -> Context:
    return Context(path=path, load=False)


@lru_cache()
def _get_loaded_context(path: str, config: str) -> Context:
    return Context(path=path, config=config, console=ApiConsole())


def get_loaded_context(settings: Settings = Depends(get_settings)) -> Context:
    return _get_loaded_context(settings.project_path, settings.config)


def get_context(settings: Settings = Depends(get_settings)) -> Context:
    return _get_context(settings.project_path)
