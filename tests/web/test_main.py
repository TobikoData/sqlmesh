import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from sqlmesh.core.context import Context
from web.server.main import app
from web.server.settings import Settings, get_loaded_context, get_settings

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


@pytest.fixture
def web_sushi_context(sushi_context: Context) -> Context:
    def get_context_override() -> Context:
        return sushi_context

    app.dependency_overrides[get_loaded_context] = get_context_override
    return sushi_context


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
    response = client.post("/api/files/foo.txt", json={"content": "bar"})
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

    response = client.post("/api/files/foo.txt", json={"content": "baz"})
    assert response.status_code == 200
    assert response.json() == {
        "name": "foo.txt",
        "path": "foo.txt",
        "extension": ".txt",
        "is_supported": False,
        "content": "baz",
    }
    assert (project_tmp_path / "foo.txt").read_text() == "baz"


def test_rename_file(project_tmp_path: Path) -> None:
    txt_file = project_tmp_path / "foo.txt"
    txt_file.write_text("bar")

    response = client.post("/api/files/foo.txt", json={"new_path": "baz.txt"})
    assert response.status_code == 200
    assert response.json() == {
        "name": "baz.txt",
        "path": "baz.txt",
        "extension": ".txt",
        "is_supported": False,
        "content": "bar",
    }
    assert not txt_file.exists()
    assert (project_tmp_path / "baz.txt").read_text() == "bar"


def test_rename_and_update_file(project_tmp_path: Path) -> None:
    txt_file = project_tmp_path / "foo.txt"
    txt_file.write_text("bar")

    response = client.post(
        "/api/files/foo.txt", json={"content": "hello world", "new_path": "baz.txt"}
    )
    assert response.status_code == 200
    assert response.json() == {
        "name": "baz.txt",
        "path": "baz.txt",
        "extension": ".txt",
        "is_supported": False,
        "content": "hello world",
    }
    assert not txt_file.exists()
    assert (project_tmp_path / "baz.txt").read_text() == "hello world"


def test_rename_file_not_found(project_tmp_path: Path) -> None:
    response = client.post("/api/files/foo.txt", json={"new_path": "baz.txt"})
    assert response.status_code == 404


def test_rename_file_already_exists(project_tmp_path: Path) -> None:
    foo_file = project_tmp_path / "foo.txt"
    foo_file.write_text("foo")
    bar_file = project_tmp_path / "bar.txt"
    bar_file.write_text("bar")

    response = client.post("/api/files/foo.txt", json={"new_path": "bar.txt"})
    assert response.status_code == 200
    assert response.json() == {
        "name": "bar.txt",
        "path": "bar.txt",
        "extension": ".txt",
        "is_supported": False,
        "content": "foo",
    }
    assert not foo_file.exists()


def test_rename_file_to_existing_directory(project_tmp_path: Path) -> None:
    foo_file = project_tmp_path / "foo.txt"
    foo_file.touch()
    existing_dir = project_tmp_path / "existing_dir"
    existing_dir.mkdir()

    response = client.post("/api/files/foo.txt", json={"new_path": "existing_dir"})
    assert response.status_code == 422
    assert foo_file.exists()


def test_write_file_empty_body(project_tmp_path: Path) -> None:
    response = client.post("/api/files/foo.txt", json={})
    assert response.status_code == 404


def test_delete_file(project_tmp_path: Path) -> None:
    txt_file = project_tmp_path / "foo.txt"
    txt_file.write_text("bar")

    response = client.delete("/api/files/foo.txt")
    assert response.status_code == 204
    assert not txt_file.exists()


def test_delete_file_not_found() -> None:
    response = client.delete("/api/files/not_found.txt")
    assert response.status_code == 404


def test_create_directory(project_tmp_path: Path) -> None:
    response = client.post("/api/directories/new_dir")
    assert response.status_code == 200
    assert (project_tmp_path / "new_dir").exists()
    assert response.json() == {"directories": [], "files": [], "name": "new_dir", "path": "new_dir"}


def test_create_directory_already_exists(project_tmp_path: Path) -> None:
    new_dir = project_tmp_path / "new_dir"
    new_dir.mkdir()

    response = client.post("/api/directories/new_dir")
    assert response.status_code == 422
    assert response.json() == {"detail": "Directory already exists"}


def test_rename_directory(project_tmp_path: Path) -> None:
    new_dir = project_tmp_path / "new_dir"
    new_dir.mkdir()

    response = client.post("/api/directories/new_dir", json={"new_path": "renamed_dir"})
    assert response.status_code == 200
    assert not new_dir.exists()
    assert (project_tmp_path / "renamed_dir").exists()
    assert response.json() == {
        "directories": [],
        "files": [],
        "name": "renamed_dir",
        "path": "renamed_dir",
    }


def test_rename_directory_already_exists_empty(project_tmp_path: Path) -> None:
    new_dir = project_tmp_path / "new_dir"
    new_dir.mkdir()
    existing_dir = project_tmp_path / "renamed_dir"
    existing_dir.mkdir()

    response = client.post("/api/directories/new_dir", json={"new_path": "renamed_dir"})
    assert response.status_code == 200
    assert not new_dir.exists()
    assert (project_tmp_path / "renamed_dir").exists()
    assert response.json() == {
        "directories": [],
        "files": [],
        "name": "renamed_dir",
        "path": "renamed_dir",
    }


def test_rename_directory_already_exists_not_empty(project_tmp_path: Path) -> None:
    new_dir = project_tmp_path / "new_dir"
    new_dir.mkdir()
    existing_dir = project_tmp_path / "renamed_dir"
    existing_dir.mkdir()
    existing_file = existing_dir / "foo.txt"
    existing_file.touch()

    response = client.post("/api/directories/new_dir", json={"new_path": "renamed_dir"})
    assert response.status_code == 422
    assert new_dir.exists()


def test_rename_directory_to_existing_file(project_tmp_path: Path) -> None:
    new_dir = project_tmp_path / "new_dir"
    new_dir.mkdir()
    existing_file = project_tmp_path / "foo.txt"
    existing_file.touch()

    response = client.post("/api/directories/new_dir", json={"new_path": "foo.txt"})
    assert response.status_code == 422
    assert new_dir.exists()


def test_delete_directory(project_tmp_path: Path) -> None:
    new_dir = project_tmp_path / "new_dir"
    new_dir.mkdir()

    response = client.delete("/api/directories/new_dir")
    assert response.status_code == 204
    assert not new_dir.exists()


def test_delete_directory_not_found(project_tmp_path: Path) -> None:
    response = client.delete("/api/directories/fake_dir")
    assert response.status_code == 404


def test_delete_directory_not_a_directory(project_tmp_path: Path) -> None:
    txt_file = project_tmp_path / "foo.txt"
    txt_file.touch()

    response = client.delete("/api/directories/foo.txt")
    assert response.status_code == 422
    assert response.json() == {"detail": "Not a directory"}


def test_delete_directory_not_empty(project_tmp_path: Path) -> None:
    new_dir = project_tmp_path / "new_dir"
    new_dir.mkdir()
    (new_dir / "foo.txt").touch()

    response = client.delete("/api/directories/new_dir")
    assert response.status_code == 204
    assert not new_dir.exists()


def test_apply() -> None:
    response = client.post("/api/apply?environment=dev", params={"environment": "dev"})
    assert response.status_code == 200


def test_tasks() -> None:
    response = client.get("/api/tasks")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_cancel() -> None:
    app.state.task = asyncio.create_task(asyncio.sleep(1))
    response = client.post("/api/plan/cancel")
    await asyncio.sleep(0.1)
    assert response.status_code == 204
    assert app.state.task.cancelled()


def test_cancel_no_task() -> None:
    response = client.post("/api/plan/cancel")
    assert response.status_code == 422
    assert response.json() == {"detail": "No active task found."}


def test_evaluate(web_sushi_context: Context) -> None:
    response = client.post(
        "/api/evaluate",
        json={
            "model": "sushi.top_waiters",
            "start": "2022-01-01",
            "end": "now",
            "latest": "now",
            "limit": 100,
        },
    )
    assert response.status_code == 200
    assert response.json()


def test_fetchdf(web_sushi_context: Context) -> None:
    response = client.post("/api/fetchdf", json={"sql": "SELECT * from sushi.top_waiters"})
    assert response.status_code == 200
    assert response.json()
