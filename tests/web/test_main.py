from __future__ import annotations

import threading
from pathlib import Path

import pyarrow as pa  # type: ignore
import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient
from pytest_mock.plugin import MockerFixture

from sqlmesh.core.context import Context
from sqlmesh.core.environment import Environment
from sqlmesh.utils.errors import PlanError
from web.server.api.endpoints.files import _get_file_with_content
from web.server.main import api_console, app
from web.server.settings import Settings, get_loaded_context, get_settings

pytestmark = pytest.mark.web


client = TestClient(app)


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


def test_get_files(project_tmp_path: Path) -> None:
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    sql_file = models_dir / "foo.sql"
    sql_file.write_text("MODEL (name foo); SELECT 1 ds;")

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
        "content": "bar",
    }


def test_get_file_not_found() -> None:
    response = client.get("/api/files/not_found.txt")
    assert response.status_code == 404


def test_get_file_invalid_path(project_tmp_path: Path) -> None:
    config = project_tmp_path / "config.py"
    config.write_text(
        """from sqlmesh.core.config import Config, ModelDefaultsConfig
config = Config(ignore_patterns=["*.txt"], model_defaults=ModelDefaultsConfig(dialect=''))
    """
    )
    foo_txt = project_tmp_path / "foo.txt"
    foo_txt.touch()

    response = client.get("/api/files/foo.txt")
    assert response.status_code == 404


def test_write_file(project_tmp_path: Path) -> None:
    response = client.post("/api/files/foo.txt", json={"content": "bar"})
    file = _get_file_with_content(project_tmp_path / "foo.txt", "foo.txt")
    assert response.status_code == 204
    assert file.dict() == {
        "name": "foo.txt",
        "path": "foo.txt",
        "extension": ".txt",
        "content": "bar",
    }


def test_update_file(project_tmp_path: Path) -> None:
    txt_file = project_tmp_path / "foo.txt"
    txt_file.write_text("bar")

    response = client.post("/api/files/foo.txt", json={"content": "baz"})
    file = _get_file_with_content(project_tmp_path / "foo.txt", "foo.txt")
    assert response.status_code == 204
    assert file.dict() == {
        "name": "foo.txt",
        "path": "foo.txt",
        "extension": ".txt",
        "content": "baz",
    }


def test_rename_file(project_tmp_path: Path) -> None:
    txt_file = project_tmp_path / "foo.txt"
    txt_file.write_text("bar")

    response = client.post("/api/files/foo.txt", json={"new_path": "baz.txt"})
    file = _get_file_with_content(project_tmp_path / "baz.txt", "baz.txt")
    assert response.status_code == 204
    assert file.dict() == {
        "name": "baz.txt",
        "path": "baz.txt",
        "extension": ".txt",
        "content": "bar",
    }
    assert not txt_file.exists()


def test_rename_file_and_keep_content(project_tmp_path: Path) -> None:
    txt_file = project_tmp_path / "foo.txt"
    txt_file.write_text("bar")

    response = client.post(
        "/api/files/foo.txt", json={"content": "hello world", "new_path": "baz.txt"}
    )
    file = _get_file_with_content(project_tmp_path / "baz.txt", "baz.txt")
    assert response.status_code == 204
    assert file.dict() == {
        "name": "baz.txt",
        "path": "baz.txt",
        "extension": ".txt",
        "content": "bar",
    }
    assert not txt_file.exists()


def test_rename_file_not_found() -> None:
    response = client.post("/api/files/foo.txt", json={"new_path": "baz.txt"})
    assert response.status_code == 404


def test_rename_file_already_exists(project_tmp_path: Path) -> None:
    foo_file = project_tmp_path / "foo.txt"
    foo_file.write_text("foo")
    bar_file = project_tmp_path / "bar.txt"
    bar_file.write_text("bar")

    response = client.post("/api/files/foo.txt", json={"new_path": "bar.txt"})
    file = _get_file_with_content(project_tmp_path / "bar.txt", "bar.txt")
    assert response.status_code == 204
    assert file.dict() == {
        "name": "bar.txt",
        "path": "bar.txt",
        "extension": ".txt",
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


def test_write_file_empty_body() -> None:
    response = client.post("/api/files/foo.txt", json={})
    assert response.status_code == 204


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
    assert response.json()["message"] == "Directory already exists"


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
    assert response.json()["message"] == "Unable to move a file"
    assert new_dir.exists()


def test_rename_directory_to_existing_file(project_tmp_path: Path) -> None:
    new_dir = project_tmp_path / "new_dir"
    new_dir.mkdir()
    existing_file = project_tmp_path / "foo.txt"
    existing_file.touch()

    response = client.post("/api/directories/new_dir", json={"new_path": "foo.txt"})
    assert response.status_code == 422
    assert response.json()["message"] == "Unable to move a file"
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
    assert response.json()["message"] == "Not a directory"


def test_delete_directory_not_empty(project_tmp_path: Path) -> None:
    new_dir = project_tmp_path / "new_dir"
    new_dir.mkdir()
    (new_dir / "foo.txt").touch()

    response = client.delete("/api/directories/new_dir")
    assert response.status_code == 204
    assert not new_dir.exists()


def test_apply(project_tmp_path: Path) -> None:
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    sql_file = models_dir / "foo.sql"
    sql_file.write_text("MODEL (name foo); SELECT 1;")

    client.app.state.circuit_breaker = threading.Event()  # type: ignore
    response = client.post("/api/commands/apply", json={"environment": "dev"})
    assert response.status_code == 204


@pytest.mark.skip(
    reason="needs to be fixed: plan tests are failing inside coroutine and won't throw 422"
)
def test_apply_test_failures(web_sushi_context: Context, mocker: MockerFixture) -> None:
    mocker.patch.object(web_sushi_context, "_run_plan_tests", side_effect=PlanError())
    response = client.post("/api/commands/apply", json={"environment": "dev"})
    assert response.status_code == 422
    assert response.json()["message"] == "Unable to run a plan"


def test_plan(web_sushi_context: Context) -> None:
    client.app.state.circuit_breaker = threading.Event()  # type: ignore
    response = client.post("/api/plan", json={"environment": "dev"})
    assert response.status_code == 204


@pytest.mark.skip(
    reason="needs to be fixed: plan tests are failing inside coroutine and won't throw 422"
)
def test_plan_test_failures(web_sushi_context: Context, mocker: MockerFixture) -> None:
    mocker.patch.object(web_sushi_context, "_run_plan_tests", side_effect=PlanError())
    response = client.post("/api/plan", json={"environment": "dev"})
    assert response.status_code == 422
    assert response.json()["message"] == "Unable to run a plan"


@pytest.mark.asyncio
async def test_cancel() -> None:
    client.app.state.circuit_breaker = threading.Event()  # type: ignore
    async with AsyncClient(app=app, base_url="http://testserver") as _client:
        await _client.post("/api/plan", json={"environment": "dev"})
        response = await _client.post("/api/plan/cancel")
    assert response.status_code == 204
    assert app.state.task.cancelled()


def test_cancel_no_task() -> None:
    response = client.post("/api/plan/cancel")
    assert response.status_code == 422
    assert response.json()["message"] == "Plan/apply is not running"


def test_evaluate(web_sushi_context: Context) -> None:
    response = client.post(
        "/api/commands/evaluate",
        json={
            "model": "sushi.top_waiters",
            "start": "2022-01-01",
            "end": "now",
            "execution_time": "now",
            "limit": 100,
        },
    )
    assert response.status_code == 200
    with pa.ipc.open_stream(response.content) as reader:
        df = reader.read_pandas()
    assert not df.empty


def test_fetchdf(web_sushi_context: Context) -> None:
    response = client.post("/api/commands/fetchdf", json={"sql": "SELECT * from sushi.top_waiters"})
    assert response.status_code == 200
    with pa.ipc.open_stream(response.content) as reader:
        df = reader.read_pandas()
    assert not df.empty


def test_get_model(web_sushi_context: Context) -> None:
    response = client.get("/api/models/sushi.customers")

    assert response.status_code == 200

    test_model = response.json()

    assert test_model.get("name") == "sushi.customers"
    assert test_model.get("path") == "models/customers.sql"
    assert test_model.get("dialect") == "duckdb"
    assert test_model.get("columns") == [
        {
            "name": "customer_id",
            "type": "INT",
            "description": "customer_id uniquely identifies customers",
        },
        {"name": "status", "type": "TEXT"},
        {"name": "zip", "type": "TEXT"},
    ]


# TODO: add better tests for this endpoint
def test_get_models(web_sushi_context: Context) -> None:
    response = client.get("/api/models")

    assert response.status_code == 200

    test_model = response.json()[0]

    assert test_model.get("name")
    assert test_model.get("path")
    assert test_model.get("dialect")
    assert test_model.get("columns")


def test_render(web_sushi_context: Context) -> None:
    response = client.post("/api/commands/render", json={"model": "sushi.items"})
    assert response.status_code == 200
    assert response.json()["sql"]


def test_render_invalid_model(web_sushi_context: Context) -> None:
    response = client.post("/api/commands/render", json={"model": "foo.bar"})
    assert response.status_code == 422
    assert response.json()["message"] == "Unable to find a model"


def test_get_environments(project_context: Context) -> None:
    response = client.get("/api/environments")
    assert response.status_code == 200
    response_json = response.json()
    assert len(response_json["environments"]) == 1

    environment = Environment.parse_obj(response_json["environments"]["prod"])
    assert environment == Environment(
        name="prod", snapshots=[], start_at="1970-01-01", plan_id="", suffix_target="schema"
    )
    assert response_json["pinned_environments"] == list(project_context.config.pinned_environments)
    assert (
        response_json["default_target_environment"]
        == project_context.config.default_target_environment
    )


def test_delete_environment_success(web_sushi_context: Context):
    response = client.delete("/api/environments/test")

    assert response.status_code == 204


def test_delete_environment_failure(web_sushi_context: Context, mocker: MockerFixture):
    mocker.patch.object(
        web_sushi_context.state_sync, "invalidate_environment", side_effect=Exception("Some error")
    )

    response = client.delete("/api/environments/test")

    assert response.status_code == 422
    assert response.json()["message"] == "Unable to delete environments"


def test_get_lineage(web_sushi_context: Context) -> None:
    response = client.get("/api/lineage/sushi.waiters/event_date")

    assert response.status_code == 200
    assert response.json() == {
        '"memory"."sushi"."waiters"': {
            "event_date": {
                "source": """SELECT DISTINCT
  CAST(o.event_date AS DATE) AS event_date
FROM (
  SELECT
    CAST(NULL AS INT) AS id,
    CAST(NULL AS INT) AS customer_id,
    CAST(NULL AS INT) AS waiter_id,
    CAST(NULL AS INT) AS start_ts,
    CAST(NULL AS INT) AS end_ts,
    CAST(NULL AS DATE) AS event_date
  FROM (VALUES
    (1)) AS t(dummy)
) AS o /* source: memory.sushi.orders */
WHERE
  o.event_date <= CAST('1970-01-01' AS DATE)
  AND o.event_date >= CAST('1970-01-01' AS DATE)""",
                "expression": "CAST(o.event_date AS DATE) AS event_date",
                "models": {'"memory"."sushi"."orders"': ["event_date"]},
            }
        },
        '"memory"."sushi"."orders"': {
            "event_date": {
                "source": "SELECT\n  CAST(NULL AS DATE) AS event_date\nFROM (VALUES\n  (1)) AS t(dummy)",
                "expression": "CAST(NULL AS DATE) AS event_date",
                "models": {},
            }
        },
    }


def test_get_lineage_external_model(project_context: Context) -> None:
    project_tmp_path = project_context.path
    models_dir = project_tmp_path / "models"
    models_dir.mkdir()
    foo_sql_file = models_dir / "foo.sql"
    foo_sql_file.write_text("MODEL (name foo); SELECT id FROM bar;")
    bar_sql_file = models_dir / "bar.sql"
    bar_sql_file.write_text("MODEL (name bar); SELECT * FROM baz;")
    baz_sql_file = models_dir / "baz.sql"
    baz_sql_file.write_text("MODEL (name baz); SELECT * FROM external_table;")
    project_context.load()

    response = client.get("/api/lineage/foo/id")
    assert response.status_code == 200, response.json()
    assert response.json() == {
        '"foo"': {
            "id": {
                "source": """SELECT
  bar.id AS id
FROM (
  SELECT
    *
  FROM (
    SELECT
      *
    FROM external_table AS external_table
  ) AS baz /* source: baz */
) AS bar /* source: bar */""",
                "expression": "bar.id AS id",
                "models": {'"bar"': ["id"]},
            }
        },
        '"bar"': {
            "id": {
                "source": """SELECT
  *
FROM (
  SELECT
    *
  FROM external_table AS external_table
) AS baz /* source: baz */""",
                "expression": "*",
                "models": {},
            }
        },
    }


def test_get_lineage_managed_columns(web_sushi_context: Context) -> None:
    # Get lineage with upstream managed columns
    response = client.get("/api/lineage/sushi.customers/customer_id")
    assert response.status_code == 200
    assert "valid_from" in response.text
    assert "valid_to" in response.text

    # Get lineage of managed column
    response = client.get("/api/lineage/sushi.marketing/valid_from")
    assert response.status_code == 200
    assert response.json() == {
        '"memory"."sushi"."marketing"': {
            "valid_from": {
                "source": """SELECT
  CAST(NULL AS TIMESTAMP) AS valid_from
FROM (
  SELECT
    CAST(NULL AS INT) AS customer_id,
    CAST(NULL AS TEXT) AS status,
    CAST(NULL AS TIMESTAMP) AS updated_at
  FROM (VALUES
    (1)) AS t(dummy)
) AS raw_marketing /* source: memory.sushi.raw_marketing */""",
                "expression": "CAST(NULL AS TIMESTAMP) AS valid_from",
                "models": {},
            }
        }
    }


def test_table_diff(web_sushi_context: Context) -> None:
    web_sushi_context.plan(
        "dev",
        no_prompts=True,
        auto_apply=True,
        skip_tests=True,
        include_unmodified=True,
    )
    response = client.get(
        "/api/table_diff",
        params={
            "source": "prod",
            "target": "dev",
            "model_or_snapshot": "sushi.customer_revenue_by_day",
        },
    )
    assert response.status_code == 200
    assert "schema_diff" in response.json()
    assert "row_diff" in response.json()


def test_test(web_sushi_context: Context) -> None:
    response = client.get("/api/commands/test")
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["tests_run"] == 2
    assert response_json["failures"] == []

    # Single test
    response = client.get("/api/commands/test", params={"test": "tests/test_order_items.yaml"})
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["tests_run"] == 1
    assert response_json["failures"] == []


def test_test_failure(project_context: Context) -> None:
    models_dir = project_context.path / "models"
    models_dir.mkdir()
    sql_file = models_dir / "foo.sql"
    sql_file.write_text("MODEL (name foo); SELECT 1 ds;")

    tests_dir = project_context.path / "tests"
    tests_dir.mkdir()
    test_file = tests_dir / "test_foo.yaml"
    test_file.write_text(
        """test_foo:
  model: foo
  outputs:
    query:
      - ds: 2
  vars:
    start: 2022-01-01
    end: 2022-01-01
    latest: 2022-01-01"""
    )

    project_context.load()
    response = client.get("/api/commands/test")
    assert response.status_code == 200
    response_json = response.json()
    assert response_json["tests_run"] == 1
    assert response_json["failures"] == [
        {
            "name": "test_foo",
            "path": "tests/test_foo.yaml",
            "tb": "AssertionError: Data differs (exp: expected, act: actual)\n\n   ds    \n  exp act\n0   2   1\n",
        }
    ]
