from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    project_path: str = "../../example"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
