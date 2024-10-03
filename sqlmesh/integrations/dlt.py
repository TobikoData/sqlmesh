import typing as t
import click
from pydantic import ValidationError
from sqlglot import exp
from sqlmesh.core.config.connection import parse_connection_config


def generate_dlt_models_and_settings(
    pipeline_name: str, dialect: str
) -> t.Tuple[t.Set[t.Tuple[str, str]], str]:
    """This function attaches to a DLT pipeline and retrieves the connection configs and
    SQLMesh models based on the tables present in the pipeline's default schema.
    """

    import dlt
    from dlt.common.schema.utils import has_table_seen_data, is_complete_column
    from dlt.pipeline.exceptions import CannotRestorePipelineException

    try:
        pipeline = dlt.attach(pipeline_name=pipeline_name)
    except CannotRestorePipelineException:
        raise click.ClickException("Could not attach to pipeline {pipeline_name}")

    schema = pipeline.default_schema
    dataset = pipeline.dataset_name

    client = pipeline._sql_job_client(pipeline.default_schema)
    config = client.config
    credentials = config.credentials
    db_type = pipeline.destination.to_name(pipeline.destination)
    configs = {
        key: value
        for key in dir(credentials)
        if not key.startswith("_")
        and not callable(value := getattr(credentials, key))
        and value is not None
    }

    dlt_tables = {
        name: table
        for name, table in schema.tables.items()
        if (has_table_seen_data(table) and not name.startswith(schema._dlt_tables_prefix))
        or name == schema.loads_table_name
    }

    sqlmesh_models = set()
    sqlmesh_schema_name = f"{dataset}_sqlmesh"
    for table_name, table in dlt_tables.items():
        dlt_columns = {
            c["name"]: exp.DataType.build(str(c["data_type"]), dialect=dialect)
            for c in filter(is_complete_column, table["columns"].values())
            if c.get("name") and c.get("data_type")
        }
        primary_key = [
            str(c["name"])
            for c in filter(is_complete_column, table["columns"].values())
            if c.get("primary_key") and c.get("name")
        ]
        model_def_columns = format_columns(dlt_columns, dialect)
        select_columns = format_columns_for_select(dlt_columns)
        grain = format_grain(primary_key)
        incremental_model_name = f"{sqlmesh_schema_name}.incremental_{table_name}"
        full_model_name = f"{sqlmesh_schema_name}.full_{table_name}"

        incremental_model_sql = generate_incremental_model(
            incremental_model_name,
            model_def_columns,
            select_columns,
            grain,
            dataset + "." + table_name,
        )
        full_model_sql = generate_full_model(
            full_model_name, model_def_columns, select_columns, grain, incremental_model_name
        )

        sqlmesh_models.update(
            {(incremental_model_name, incremental_model_sql), (full_model_name, full_model_sql)}
        )

    return sqlmesh_models, format_config(configs, db_type)


def generate_incremental_model(
    model_name: str, model_def_columns: str, select_columns: str, grain: str, from_table: str
) -> str:
    """Generate the SQL definition for an incremental model."""

    key = "load_id" if from_table.endswith("_dlt_loads") else "_dlt_id"

    return f"""MODEL (
  name {model_name},
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key {key},
  ),{model_def_columns}{grain}
);

SELECT
{select_columns}
FROM
  {from_table}
"""


def generate_full_model(
    model_name: str, model_def_columns: str, select_columns: str, grain: str, from_table: str
) -> str:
    """Generate the SQL definition for a full model."""

    return f"""MODEL (
  name {model_name},
  kind FULL,
  cron '@daily',{model_def_columns}{grain}
);

SELECT
{select_columns}
FROM
  {from_table}
"""


def format_config(configs: t.Dict[str, str], db_type: str) -> str:
    """Generate a string for the gateway connection config."""
    config = {
        "type": db_type,
    }

    for key, value in configs.items():
        if key == "password":
            config[key] = f'"{value}"'
        elif key == "username":
            config["user"] = value
        else:
            config[key] = value

    # Validate the connection config fields
    invalid_fields = []
    try:
        parse_connection_config(config)
    except ValidationError as e:
        for error in e.errors():
            invalid_fields.append(error.get("loc", [])[0])

    return "\n".join(
        [f"      {key}: {value}" for key, value in config.items() if key not in invalid_fields]
    )


def format_columns(dlt_columns: t.Dict[str, exp.DataType], dialect: str) -> str:
    """Format the columns for the SQLMesh model definition."""
    if not dlt_columns:
        return ""

    columns_str = ",\n    ".join(
        f"{name} {data_type.sql(dialect=dialect)}" for name, data_type in dlt_columns.items()
    )
    return f"\n  columns ({columns_str}\n  ),"


def format_grain(primary_key: t.List[str]) -> str:
    """Format the grain for the SQLMesh model definition."""
    if primary_key:
        return f"\n  grain ({', '.join(primary_key)}),"
    return ""


def format_columns_for_select(dlt_columns: t.Dict[str, exp.DataType]) -> str:
    """Format the columns for the SELECT statement in the model."""
    return ",\n".join(f"  {column_name}" for column_name in dlt_columns) if dlt_columns else ""
