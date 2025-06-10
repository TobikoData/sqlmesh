import typing as t
from enum import Enum
from pathlib import Path
from dataclasses import dataclass

from sqlglot import Dialect
from sqlmesh.integrations.dlt import generate_dlt_models_and_settings
from sqlmesh.utils.date import yesterday_ds
from sqlmesh.utils.errors import SQLMeshError

from sqlmesh.core.config.connection import CONNECTION_CONFIG_TO_TYPE, DIALECT_TO_TYPE


PRIMITIVES = (str, int, bool, float)


class ProjectTemplate(Enum):
    DEFAULT = "default"
    EMPTY = "empty"
    DBT = "dbt"
    DLT = "dlt"


class InitCliMode(Enum):
    DEFAULT = "default"
    SIMPLE = "simple"


def _gen_config(
    engine_type: t.Optional[str],
    settings: t.Optional[str],
    start: t.Optional[str],
    template: ProjectTemplate,
    cli_mode: InitCliMode,
) -> str:
    connection_settings = (
        settings
        or """      type: duckdb
      database: db.db"""
    )

    engine = "mssql" if engine_type == "tsql" else engine_type

    if not settings and template != ProjectTemplate.DBT:
        doc_link = "https://sqlmesh.readthedocs.io/en/stable/integrations/engines{engine_link}"
        engine_link = ""

        if engine in CONNECTION_CONFIG_TO_TYPE:
            required_fields = []
            non_required_fields = []

            for name, field in CONNECTION_CONFIG_TO_TYPE[engine].model_fields.items():
                field_name = field.alias or name
                if field_name in ("dialect", "display_name"):
                    continue

                default_value = field.get_default()

                if isinstance(default_value, Enum):
                    default_value = default_value.value
                elif not isinstance(default_value, PRIMITIVES):
                    default_value = ""

                required = field.is_required() or field_name == "type"
                option_str = f"      {'# ' if not required else ''}{field_name}: {default_value}\n"

                # specify the DuckDB database field so quickstart runs out of the box
                if engine == "duckdb" and field_name == "database":
                    option_str = "      database: db.db\n"
                    required = True

                if required:
                    required_fields.append(option_str)
                else:
                    non_required_fields.append(option_str)

            connection_settings = "".join(required_fields + non_required_fields)

            engine_link = f"/{engine}/#connection-options"

        connection_settings = (
            "      # For more information on configuring the connection to your execution engine, visit:\n"
            "      # https://sqlmesh.readthedocs.io/en/stable/reference/configuration/#connection\n"
            f"      # {doc_link.format(engine_link=engine_link)}\n{connection_settings}"
        )

    default_configs = {
        ProjectTemplate.DEFAULT: f"""# --- Gateway Connection ---
gateways:
  {engine}:
    connection:
{connection_settings}
default_gateway: {engine}

# --- Model Defaults ---
# https://sqlmesh.readthedocs.io/en/stable/reference/model_configuration/#model-defaults

model_defaults:
  dialect: {DIALECT_TO_TYPE[engine]}
  start: {start or yesterday_ds()} # Start date for backfill history
  cron: '@daily'    # Run models daily at 12am UTC (can override per model)

# --- Linting Rules ---
# Enforce standards for your team
# https://sqlmesh.readthedocs.io/en/stable/guides/linter/

linter:
  enabled: true
  rules:
    - ambiguousorinvalidcolumn
    - invalidselectstarexpansion
""",
        ProjectTemplate.DBT: """from pathlib import Path

from sqlmesh.dbt.loader import sqlmesh_config

config = sqlmesh_config(Path(__file__).parent)
""",
    }

    default_configs[ProjectTemplate.EMPTY] = default_configs[ProjectTemplate.DEFAULT]
    default_configs[ProjectTemplate.DLT] = default_configs[ProjectTemplate.DEFAULT]

    simple_cli_mode = """
# --- SIMPLE CLI MODE ---
# Minimal prompts, automatic changes, summary output
# https://sqlmesh.readthedocs.io/en/stable/reference/configuration/#plan

plan:
  no_diff: true             # Hide detailed text differences for changed models
  use_finalized_state: true # Compare only against finalized snapshots
  no_prompts: true          # No interactive prompts
  auto_apply: true          # Apply changes automatically

# --- Optional: Set a default target environment ---
# default_target_environment: dev_{{ env_var('USER', 'your_name') }}

# Example usage:
# export USER=your_name
# sqlmesh plan            # Resolves to: sqlmesh plan dev_your_name
# sqlmesh plan prod       # To apply changes to production
"""

    return default_configs[template] + (simple_cli_mode if cli_mode == InitCliMode.SIMPLE else "")


@dataclass
class ExampleObjects:
    schema_name: str
    full_model_name: str
    full_model_def: str
    incremental_model_name: str
    incremental_model_def: str
    seed_model_name: str
    seed_model_def: str
    seed_data: str
    audit_def: str
    test_def: str

    def models(self) -> t.Set[t.Tuple[str, str]]:
        return {
            (self.full_model_name, self.full_model_def),
            (self.incremental_model_name, self.incremental_model_def),
            (self.seed_model_name, self.seed_model_def),
        }


def _gen_example_objects(schema_name: str) -> ExampleObjects:
    full_model_name = f"{schema_name}.full_model"
    incremental_model_name = f"{schema_name}.incremental_model"
    seed_model_name = f"{schema_name}.seed_model"

    full_model_def = f"""MODEL (
  name {full_model_name},
  kind FULL,
  cron '@daily',
  grain item_id,
  audits (assert_positive_order_ids),
);

SELECT
  item_id,
  COUNT(DISTINCT id) AS num_orders,
FROM
  {incremental_model_name}
GROUP BY item_id
  """

    incremental_model_def = f"""MODEL (
  name {incremental_model_name},
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date
  ),
  start '2020-01-01',
  cron '@daily',
  grain (id, event_date)
);

SELECT
  id,
  item_id,
  event_date,
FROM
  {seed_model_name}
WHERE
  event_date BETWEEN @start_date AND @end_date
  """

    seed_model_def = f"""MODEL (
  name {seed_model_name},
  kind SEED (
    path '../seeds/seed_data.csv'
  ),
  columns (
    id INTEGER,
    item_id INTEGER,
    event_date DATE
  ),
  grain (id, event_date)
);
  """

    audit_def = """AUDIT (
  name assert_positive_order_ids,
);

SELECT *
FROM @this_model
WHERE
  item_id < 0
  """

    seed_data = """id,item_id,event_date
1,2,2020-01-01
2,1,2020-01-01
3,3,2020-01-03
4,1,2020-01-04
5,1,2020-01-05
6,1,2020-01-06
7,1,2020-01-07
"""

    test_def = f"""test_example_full_model:
  model: {full_model_name}
  inputs:
    {incremental_model_name}:
      rows:
      - id: 1
        item_id: 1
      - id: 2
        item_id: 1
      - id: 3
        item_id: 2
  outputs:
    query:
      rows:
      - item_id: 1
        num_orders: 2
      - item_id: 2
        num_orders: 1
  """

    return ExampleObjects(
        schema_name=schema_name,
        full_model_name=full_model_name,
        full_model_def=full_model_def,
        incremental_model_name=incremental_model_name,
        incremental_model_def=incremental_model_def,
        seed_model_name=seed_model_name,
        seed_model_def=seed_model_def,
        seed_data=seed_data,
        audit_def=audit_def,
        test_def=test_def,
    )


def init_example_project(
    path: t.Union[str, Path],
    dialect: t.Optional[str],
    engine_type: t.Optional[str],
    template: ProjectTemplate = ProjectTemplate.DEFAULT,
    pipeline: t.Optional[str] = None,
    dlt_path: t.Optional[str] = None,
    schema_name: str = "sqlmesh_example",
    cli_mode: InitCliMode = InitCliMode.DEFAULT,
) -> t.Union[str, Path]:
    root_path = Path(path)
    config_extension = "py" if template == ProjectTemplate.DBT else "yaml"
    config_path = root_path / f"config.{config_extension}"
    audits_path = root_path / "audits"
    macros_path = root_path / "macros"
    models_path = root_path / "models"
    seeds_path = root_path / "seeds"
    tests_path = root_path / "tests"

    if config_path.exists():
        raise SQLMeshError(
            f"Found an existing config file '{config_path}'.\n\nPlease change to another directory or remove the existing file."
        )

    if not engine_type and template != ProjectTemplate.DBT:
        if not dialect:
            raise SQLMeshError("Please provide a default SQL dialect for your project's models.")
        Dialect.get_or_raise(dialect)

    models: t.Set[t.Tuple[str, str]] = set()
    settings = None
    start = None
    if template == ProjectTemplate.DLT:
        if pipeline and dialect:
            models, settings, start = generate_dlt_models_and_settings(
                pipeline_name=pipeline, dialect=dialect, dlt_path=dlt_path
            )
        else:
            raise SQLMeshError(
                "Please provide a DLT pipeline with the `--dlt-pipeline` flag to generate a SQLMesh project from DLT."
            )

    # config generation chooses engine based on ConnectionConfig.type_
    # - if user passes a SQL dialect, we always generate the engine whose type_ == dialect
    #   - example: if users passes `postgres` we will always choose `postgres` engine and never `gcp_postgres`
    # - if user interactively chooses an engine, we will pass the correct ConnectionConfig.type_
    _create_config(config_path, engine_type or dialect, settings, start, template, cli_mode)
    if template == ProjectTemplate.DBT:
        return config_path

    _create_folders([audits_path, macros_path, models_path, seeds_path, tests_path])

    if template == ProjectTemplate.DLT:
        _create_models(models_path, models)
        return config_path

    example_objects = _gen_example_objects(schema_name=schema_name)

    if template != ProjectTemplate.EMPTY:
        _create_macros(macros_path)
        _create_audits(audits_path, example_objects)
        _create_models(models_path, example_objects.models())
        _create_seeds(seeds_path, example_objects)
        _create_tests(tests_path, example_objects)

    return config_path


def _create_folders(target_folders: t.Sequence[Path]) -> None:
    for folder_path in target_folders:
        folder_path.mkdir(exist_ok=True)
        (folder_path / ".gitkeep").touch()


def _create_config(
    config_path: Path,
    engine_type: t.Optional[str],
    settings: t.Optional[str],
    start: t.Optional[str],
    template: ProjectTemplate,
    cli_mode: InitCliMode,
) -> None:
    project_config = _gen_config(engine_type, settings, start, template, cli_mode)

    _write_file(
        config_path,
        project_config,
    )


def _create_macros(macros_path: Path) -> None:
    (macros_path / "__init__.py").touch()


def _create_audits(audits_path: Path, example_objects: ExampleObjects) -> None:
    _write_file(audits_path / "assert_positive_order_ids.sql", example_objects.audit_def)


def _create_models(models_path: Path, models: t.Set[t.Tuple[str, str]]) -> None:
    for model_name, model_def in models:
        _write_file(models_path / f"{model_name.split('.')[-1]}.sql", model_def)


def _create_seeds(seeds_path: Path, example_objects: ExampleObjects) -> None:
    _write_file(seeds_path / "seed_data.csv", example_objects.seed_data)


def _create_tests(tests_path: Path, example_objects: ExampleObjects) -> None:
    _write_file(tests_path / "test_full_model.yaml", example_objects.test_def)


def _write_file(path: Path, payload: str) -> None:
    with open(path, "w", encoding="utf-8") as fd:
        fd.write(payload)
