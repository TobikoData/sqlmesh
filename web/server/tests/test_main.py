from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from web.server.main import app
from web.server.settings import Settings, get_settings

client = TestClient(app)


@pytest.fixture
def project_tmp_path(tmp_path: Path) -> Path:
    def get_settings_override() -> Settings:
        return Settings(project_path=tmp_path)

    config = tmp_path / "config.py"
    config.write_text(
        """from sqlmesh.core.config import Config
config = Config()
    """
    )

    app.dependency_overrides[get_settings] = get_settings_override
    return tmp_path


def test_get_files(project_tmp_path: Path) -> None:
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    sql_file = models_dir / "foo.sql"
    sql_file.write_text("MODEL (name foo); SELECT ds;")

    response = client.get("/api/files")
    assert response.status_code == 200
    assert response.json() == {
        "name": project_tmp_path.name,
        "path": "",
        "directories": [
            {
                "name": "models",
                "path": "models",
                "directories": [],
                "files": [
                    {
                        "name": "foo.sql",
                        "path": "models/foo.sql",
                        "extension": ".sql",
                        "is_supported": True,
                        "content": None,
                    }
                ],
            }
        ],
        "files": [
            {
                "name": "config.py",
                "path": "config.py",
                "extension": ".py",
                "is_supported": True,
                "content": None,
            }
        ],
    }


def test_get_file(project_tmp_path: Path) -> None:
    txt_file = project_tmp_path / "foo.txt"
    txt_file.write_text("bar")

    response = client.get("/api/files/foo.txt")
    assert response.status_code == 200
    assert response.json() == {
        "name": "foo.txt",
        "path": "foo.txt",
        "extension": ".txt",
        "is_supported": False,
        "content": "bar",
    }


def test_get_file_not_found() -> None:
    response = client.get("/api/files/not_found.txt")
    assert response.status_code == 404


def test_get_file_invalid_path(project_tmp_path: Path) -> None:
    config = project_tmp_path / "config.py"
    config.write_text(
        """from sqlmesh.core.config import Config
config = Config(ignore_patterns=["*.txt"])
    """
    )
    foo_txt = project_tmp_path / "foo.txt"
    foo_txt.touch()

    response = client.get("/api/files/foo.txt")
    assert response.status_code == 404


def test_write_file(project_tmp_path: Path) -> None:
    response = client.post("/api/files/foo.txt", data='"bar"')
    assert response.status_code == 200
    assert response.json() == {
        "name": "foo.txt",
        "path": "foo.txt",
        "extension": ".txt",
        "is_supported": False,
        "content": "bar",
    }
    assert (project_tmp_path / "foo.txt").read_text() == "bar"


def test_update_file(project_tmp_path: Path) -> None:
    txt_file = project_tmp_path / "foo.txt"
    txt_file.write_text("bar")

    response = client.post("/api/files/foo.txt", data='"baz"')
    assert response.status_code == 200
    assert response.json() == {
        "name": "foo.txt",
        "path": "foo.txt",
        "extension": ".txt",
        "is_supported": False,
        "content": "baz",
    }
    assert (project_tmp_path / "foo.txt").read_text() == "baz"


def test_delete_file(project_tmp_path: Path) -> None:
    txt_file = project_tmp_path / "foo.txt"
    txt_file.write_text("bar")

    response = client.delete("/api/files/foo.txt")
    assert response.status_code == 204
    assert not txt_file.exists()


def test_delete_file_not_found() -> None:
    response = client.delete("/api/files/not_found.txt")
    assert response.status_code == 404
