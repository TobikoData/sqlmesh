from pathlib import Path

import pytest

from sqlmesh.core.context import Context
from web.server.main import api_console, app
from web.server.settings import Settings, get_loaded_context, get_settings


@pytest.fixture
def project_tmp_path(tmp_path: Path) -> Path:
    def get_settings_override() -> Settings:
        return Settings(project_path=tmp_path)

    config = tmp_path / "config.py"
    config.write_text(
        """from sqlmesh.core.config import Config, ModelDefaultsConfig
config = Config(model_defaults=ModelDefaultsConfig(dialect=''))
    """
    )

    app.dependency_overrides[get_settings] = get_settings_override
    return tmp_path


@pytest.fixture
def project_context(project_tmp_path: Path) -> Context:
    context = Context(paths=project_tmp_path, console=api_console)

    def get_loaded_context_override() -> Context:
        return context

    app.dependency_overrides[get_loaded_context] = get_loaded_context_override
    return context


@pytest.fixture
def web_sushi_context(sushi_context: Context) -> Context:
    def get_context_override() -> Context:
        sushi_context.console = api_console
        return sushi_context

    app.dependency_overrides[get_loaded_context] = get_context_override
    return sushi_context
