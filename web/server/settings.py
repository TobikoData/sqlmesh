import typing as t
from functools import lru_cache
from pathlib import Path

from pydantic import BaseSettings

from sqlmesh.core.context import Context


class Settings(BaseSettings):
    project_path: Path = Path("examples/sushi")
    _context: t.Optional[Context] = None

    class Config:
        underscore_attrs_are_private = True

    @property
    def context(self) -> Context:
        if not self._context:
            self._context = Context(path=str(self.project_path))
        return self._context


@lru_cache()
def get_settings() -> Settings:
    return Settings()
