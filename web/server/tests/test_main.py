from fastapi.testclient import TestClient

from web.server.main import app
from web.server.settings import Settings, get_settings

client = TestClient(app)


def get_settings_override() -> Settings:
    return Settings(project_path="example")


app.dependency_overrides[get_settings] = get_settings_override


def test_get_files() -> None:
    response = client.get("/api/files")
    assert response.status_code == 200
