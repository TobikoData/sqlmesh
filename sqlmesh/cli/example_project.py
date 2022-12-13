import typing as t
from enum import Enum
from pathlib import Path

import click

DEFAULT_CONFIG = """import duckdb
from sqlmesh.core.config import Config

config = Config(
    engine_connection_factory=duckdb.connect,
    engine_dialect="duckdb",
)


test_config = config
"""


DEFAULT_AIRFLOW_CONFIG = """import duckdb
from sqlmesh.core.config import AirflowSchedulerBackend, Config

config = Config(
    scheduler_backend=AirflowSchedulerBackend(
        airflow_url="http://localhost:8080/",
        username="airflow",
        password="airflow",
    ),
    backfill_concurrent_tasks=4,
    ddl_concurrent_tasks=4,
)


test_config = Config(
    engine_connection_factory=duckdb.connect,
    engine_dialect="duckdb",
)
"""


EXAMPLE_MODEL_NAME = "sqlmesh_example.example_model"


EXAMPLE_MODEL = f"""MODEL (
  name {EXAMPLE_MODEL_NAME},
  kind full,
  cron '@daily'
);

SELECT
  'dummy_id' AS id
"""


EXAMPLE_AUDIT = f"""AUDIT (
  name assert_dummy_id_exists,
  model {EXAMPLE_MODEL_NAME}
);

SELECT *
FROM {EXAMPLE_MODEL_NAME}
WHERE
  id != 'dummy_id'
"""


EXAMPLE_TEST = f"""test_example_model:
  model: {EXAMPLE_MODEL_NAME}
  outputs:
    query:
      rows:
      - id: 'dummy_id'
"""


class ProjectTemplate(Enum):
    AIRFLOW = "airflow"
    DEFAULT = "default"


def init_example_project(
    path: t.Union[str, Path], template: ProjectTemplate = ProjectTemplate.DEFAULT
) -> None:
    root_path = Path(path)
    config_path = root_path / "config.py"
    audits_path = root_path / "audits"
    macros_path = root_path / "macros"
    models_path = root_path / "models"
    tests_path = root_path / "tests"

    if config_path.exists():
        raise click.ClickException(f"Found an existing config in '{config_path}'")

    _create_folders([audits_path, macros_path, models_path, tests_path])
    _create_config(config_path, template)
    _create_audits(audits_path)
    _create_models(models_path)
    _create_tests(tests_path)


def _create_folders(target_folders: t.Sequence[Path]) -> None:
    for folder_path in target_folders:
        folder_path.mkdir()
        (folder_path / ".gitkeep").touch()


def _create_config(config_path: Path, template: ProjectTemplate) -> None:
    _write_file(
        config_path,
        DEFAULT_AIRFLOW_CONFIG
        if template == ProjectTemplate.AIRFLOW
        else DEFAULT_CONFIG,
    )


def _create_audits(audits_path: Path) -> None:
    _write_file(audits_path / f"example_model.sql", EXAMPLE_AUDIT)


def _create_models(models_path: Path) -> None:
    _write_file(models_path / "example_model.sql", EXAMPLE_MODEL)


def _create_tests(tests_path: Path) -> None:
    _write_file(tests_path / "test_example_model.yaml", EXAMPLE_TEST)


def _write_file(path: Path, payload: str) -> None:
    with open(path, "w", encoding="utf-8") as fd:
        fd.write(payload)
