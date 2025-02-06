from pathlib import Path

import pytest
from fastapi import FastAPI

from sqlmesh.core.context import Context
from sqlmesh.core.console import set_console

from web.server.console import api_console
from web.server.settings import Settings, get_loaded_context, get_settings


@pytest.fixture
def web_app() -> FastAPI:
    from web.server.main import create_app

    return create_app()


@pytest.fixture
def project_tmp_path(web_app: FastAPI, tmp_path: Path):
    def get_settings_override() -> Settings:
        return Settings(project_path=tmp_path)

    config = tmp_path / "config.py"
    config.write_text(
        """from sqlmesh.core.config import Config, ModelDefaultsConfig
config = Config(model_defaults=ModelDefaultsConfig(dialect=''))
    """
    )

    web_app.dependency_overrides[get_settings] = get_settings_override
    yield tmp_path
    web_app.dependency_overrides = {}


@pytest.fixture
def project_context(web_app: FastAPI, project_tmp_path: Path):
    set_console(api_console)
    context = Context(paths=project_tmp_path)

    def get_loaded_context_override() -> Context:
        return context

    web_app.dependency_overrides[get_loaded_context] = get_loaded_context_override
    yield context
    web_app.dependency_overrides = {}


@pytest.fixture
def web_sushi_context(web_app: FastAPI, sushi_context: Context):
    def get_context_override() -> Context:
        sushi_context.console = api_console
        return sushi_context

    web_app.dependency_overrides[get_loaded_context] = get_context_override
    yield sushi_context
    web_app.dependency_overrides = {}
