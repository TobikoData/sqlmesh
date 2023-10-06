import typing as t
from enum import Enum
from pathlib import Path

import click
from sqlglot import Dialect


class ProjectTemplate(Enum):
    AIRFLOW = "airflow"
    DBT = "dbt"
    DEFAULT = "default"


def _gen_config(dialect: t.Optional[str], template: ProjectTemplate) -> str:
    default_configs = {
        ProjectTemplate.DEFAULT: f"""gateways:
    local:
        connection:
            type: duckdb
            database: db.db

default_gateway: local

model_defaults:
    dialect: {dialect}
""",
        ProjectTemplate.AIRFLOW: f"""gateways:
    local:
        connection:
            type: duckdb
            database: db.db

default_gateway: local

default_scheduler:
    type: airflow
    airflow_url: http://localhost:8080/
    username: airflow
    password: airflow

model_defaults:
    dialect: {dialect}
""",
        ProjectTemplate.DBT: """from pathlib import Path

from sqlmesh.dbt.loader import sqlmesh_config

config = sqlmesh_config(Path(__file__).parent)
""",
    }

    return default_configs[template]


EXAMPLE_SCHEMA_NAME = "sqlmesh_example"
EXAMPLE_FULL_MODEL_NAME = f"{EXAMPLE_SCHEMA_NAME}.full_model"
EXAMPLE_INCREMENTAL_MODEL_NAME = f"{EXAMPLE_SCHEMA_NAME}.incremental_model"
EXAMPLE_SEED_MODEL_NAME = f"{EXAMPLE_SCHEMA_NAME}.seed_model"

EXAMPLE_FULL_MODEL_DEF = f"""MODEL (
  name {EXAMPLE_FULL_MODEL_NAME},
  kind FULL,
  cron '@daily',
  grain item_id,
  audits [assert_positive_order_ids],
);

SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    {EXAMPLE_INCREMENTAL_MODEL_NAME}
GROUP BY item_id
ORDER BY item_id
"""

EXAMPLE_INCREMENTAL_MODEL_DEF = f"""MODEL (
    name {EXAMPLE_INCREMENTAL_MODEL_NAME},
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column ds
    ),
    start '2020-01-01',
    cron '@daily',
    grain [id, ds]
);

SELECT
    id,
    item_id,
    ds,
FROM
    {EXAMPLE_SEED_MODEL_NAME}
WHERE
    ds between @start_ds and @end_ds
"""

EXAMPLE_SEED_MODEL_DEF = f"""MODEL (
    name {EXAMPLE_SEED_MODEL_NAME},
    kind SEED (
        path '../seeds/seed_data.csv'
    ),
    columns (
        id INTEGER,
        item_id INTEGER,
        ds TEXT
    ),
    grain [id, ds]
);
"""

EXAMPLE_AUDIT = """AUDIT (
  name assert_positive_order_ids,
);

SELECT *
FROM @this_model
WHERE
  item_id < 0
"""

EXAMPLE_SEED_DATA = """id,item_id,ds
1,2,2020-01-01
2,1,2020-01-01
3,3,2020-01-03
4,1,2020-01-04
5,1,2020-01-05
6,1,2020-01-06
7,1,2020-01-07
"""

EXAMPLE_TEST = f"""test_example_full_model:
  model: {EXAMPLE_FULL_MODEL_NAME}
  inputs:
    {EXAMPLE_INCREMENTAL_MODEL_NAME}:
        rows:
        - id: 1
          item_id: 1
          ds: '2020-01-01'
        - id: 2
          item_id: 1
          ds: '2020-01-02'
        - id: 3
          item_id: 2
          ds: '2020-01-03'
  outputs:
    query:
      rows:
      - item_id: 1
        num_orders: 2
      - item_id: 2
        num_orders: 1
"""


def init_example_project(
    path: t.Union[str, Path],
    dialect: t.Optional[str],
    template: ProjectTemplate = ProjectTemplate.DEFAULT,
) -> None:
    root_path = Path(path)
    config_extension = "py" if template == ProjectTemplate.DBT else "yaml"
    config_path = root_path / f"config.{config_extension}"
    audits_path = root_path / "audits"
    macros_path = root_path / "macros"
    models_path = root_path / "models"
    seeds_path = root_path / "seeds"
    tests_path = root_path / "tests"

    if config_path.exists():
        raise click.ClickException(f"Found an existing config in '{config_path}'")

    if not dialect and template != ProjectTemplate.DBT:
        raise click.ClickException(
            "Default SQL dialect is a required argument for SQLMesh projects"
        )

    _create_config(config_path, dialect, template)
    if template == ProjectTemplate.DBT:
        return

    _create_folders([audits_path, macros_path, models_path, seeds_path, tests_path])
    _create_macros(macros_path)
    _create_audits(audits_path)
    _create_models(models_path)
    _create_seeds(seeds_path)
    _create_tests(tests_path)


def _create_folders(target_folders: t.Sequence[Path]) -> None:
    for folder_path in target_folders:
        folder_path.mkdir(exist_ok=True)
        (folder_path / ".gitkeep").touch()


def _create_config(config_path: Path, dialect: t.Optional[str], template: ProjectTemplate) -> None:
    if dialect:
        Dialect.get_or_raise(dialect)

    project_config = _gen_config(dialect, template)

    _write_file(
        config_path,
        project_config,
    )


def _create_macros(macros_path: Path) -> None:
    (macros_path / "__init__.py").touch()


def _create_audits(audits_path: Path) -> None:
    _write_file(audits_path / "assert_positive_order_ids.sql", EXAMPLE_AUDIT)


def _create_models(models_path: Path) -> None:
    for model_name, model_def in [
        (EXAMPLE_FULL_MODEL_NAME, EXAMPLE_FULL_MODEL_DEF),
        (EXAMPLE_INCREMENTAL_MODEL_NAME, EXAMPLE_INCREMENTAL_MODEL_DEF),
        (EXAMPLE_SEED_MODEL_NAME, EXAMPLE_SEED_MODEL_DEF),
    ]:
        _write_file(models_path / f"{model_name.split('.')[-1]}.sql", model_def)


def _create_seeds(seeds_path: Path) -> None:
    _write_file(seeds_path / "seed_data.csv", EXAMPLE_SEED_DATA)


def _create_tests(tests_path: Path) -> None:
    _write_file(tests_path / "test_full_model.yaml", EXAMPLE_TEST)


def _write_file(path: Path, payload: str) -> None:
    with open(path, "w", encoding="utf-8") as fd:
        fd.write(payload)
