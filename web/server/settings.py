import typing as t
from functools import lru_cache
from pathlib import Path

from fastapi import Depends
from pydantic import BaseSettings

from sqlmesh.core.context import Context


class Settings(BaseSettings):
    project_path: Path = Path("examples/sushi")


@lru_cache()
def get_settings() -> Settings:
    return Settings()


@lru_cache()
def _get_context(path: str) -> Context:
    return Context(path=path)


def get_context(settings: Settings = Depends(get_settings)) -> Context:
    return _get_context(settings.project_path)


def get_context_or_none(
    settings: Settings = Depends(get_settings),
) -> t.Optional[Context]:
    try:
        return _get_context(settings.project_path)
    except Exception:
        return None
