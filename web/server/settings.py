from functools import lru_cache
from pathlib import Path

from pydantic import BaseSettings


class Settings(BaseSettings):
    project_path: Path = Path("../../example")


@lru_cache()
def get_settings() -> Settings:
    return Settings()
