import typing as t
import click
from pydantic import ValidationError
from sqlglot import exp, parse_one
from sqlmesh.core.config.connection import parse_connection_config


def generate_dlt_models_and_settings(
    pipeline_name: str, dialect: str
) -> t.Tuple[t.List[t.Tuple[str, str]], str]:
    """This function attaches to a DLT pipeline and retrieves the connection configs and
    SQLMesh models based on the tables present in the pipeline's default schema.
    """

    import dlt
    from dlt.common.schema.utils import has_table_seen_data
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
        key: getattr(credentials, key)
        for key in dir(credentials)
        if not key.startswith("_") and not callable(getattr(credentials, key))
    }

    dlt_tables = {
        name: table
        for name, table in schema.tables.items()
        if (has_table_seen_data(table) and not name.startswith(schema._dlt_tables_prefix))
        or name == schema.loads_table_name
    }

    sqlmesh_models = []
    SQLMESH_SCHEMA_NAME = f"{dataset}_sqlmesh"
    for table_name, table in dlt_tables.items():
        dlt_columns, primary_key = extract_columns_and_primary_key(table, dialect)
        model_def_columns = format_columns(dlt_columns, dialect)
        select_columns = format_columns_for_select(dlt_columns)
        grain = format_grain(primary_key)
        INCREMENTAL_MODEL_NAME = f"{SQLMESH_SCHEMA_NAME}.incremental_{table_name}"
        FULL_MODEL_NAME = f"{SQLMESH_SCHEMA_NAME}.full_{table_name}"

        incremental_model_sql = generate_incremental_model(
            INCREMENTAL_MODEL_NAME,
            model_def_columns,
            select_columns,
            grain,
            dataset + "." + table_name,
        )
        full_model_sql = generate_full_model(
            FULL_MODEL_NAME, model_def_columns, select_columns, grain, INCREMENTAL_MODEL_NAME
        )

        sqlmesh_models.extend(
            [(INCREMENTAL_MODEL_NAME, incremental_model_sql), (FULL_MODEL_NAME, full_model_sql)]
        )
    return sqlmesh_models, format_config(configs, db_type)


def extract_columns_and_primary_key(
    table: t.Dict[str, t.Any], dialect: str
) -> t.Tuple[t.Dict[str, exp.DataType], t.List[str]]:
    """Extract column information and primary key for a given DLT table."""

    from dlt.common.schema.utils import is_complete_column

    dlt_columns = {
        c["name"]: parse_one(c["data_type"], into=exp.DataType, dialect=dialect)
        for c in filter(is_complete_column, table["columns"].values())
    }
    primary_key = [
        c["name"]
        for c in filter(is_complete_column, table["columns"].values())
        if c.get("primary_key")
    ]
    return dlt_columns, primary_key


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


def format_config(configs: t.Dict[str, str | None], db_type: str) -> str:
    """Generate a string for the gateway connection config."""
    config = {
        "type": db_type,
    }

    for key, value in configs.items():
        # We want to retain "False" values
        if value is not None:
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
    return (
        f"\n  columns ({',\n    '.join(f'{name} {data_type.sql(dialect=dialect)}' for name, data_type in dlt_columns.items())}\n  ),"
        if dlt_columns
        else ""
    )


def format_grain(primary_key: t.List[str]) -> str:
    """Format the grain for the SQLMesh model definition."""
    if primary_key:
        return f"\n  grain ({', '.join(primary_key)}),"
    return ""


def format_columns_for_select(dlt_columns: t.Dict[str, exp.DataType]) -> str:
    """Format the columns for the SELECT statement in the model."""
    return ",\n".join(f"  {column_name}" for column_name in dlt_columns) if dlt_columns else ""
