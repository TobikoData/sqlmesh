from functools import lru_cache
from pathlib import Path

from pydantic import BaseSettings, Extra

from sqlmesh.core.context import Context


class Settings(BaseSettings):
    project_path: Path = Path("examples/sushi")

    class Config:
        extra = Extra.allow

    @property
    def context(self) -> Context:
        if not hasattr(self, "_context"):
            self._context = Context(path=str(self.project_path))
        return self._context


@lru_cache()
def get_settings() -> Settings:
    return Settings()
