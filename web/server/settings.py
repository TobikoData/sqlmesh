from functools import lru_cache
from pathlib import Path

from pydantic import BaseSettings

from sqlmesh.core.context import Context


class Settings(BaseSettings):
    project_path: Path = Path("examples/sushi")

    @property
    def context(self) -> Context:
        return Context(path=str(self.project_path))


@lru_cache()
def get_settings() -> Settings:
    return Settings()
