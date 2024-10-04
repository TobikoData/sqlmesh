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
        raise click.ClickException(f"Could not attach to pipeline {pipeline_name}")

    schema = pipeline.default_schema
    dataset = pipeline.dataset_name

    client = pipeline._sql_job_client(schema)
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
    for table_name, table in dlt_tables.items():
        dlt_columns = {}
        primary_key = []

        # is_complete_column returns true if column contains a name and a data type
        for col in filter(is_complete_column, table["columns"].values()):
            dlt_columns[col["name"]] = exp.DataType.build(str(col["data_type"]), dialect=dialect)
            if col.get("primary_key"):
                primary_key.append(str(col["name"]))

        columns_str = ",\n    ".join(
            f"{name} {data_type.sql(dialect=dialect)}" for name, data_type in dlt_columns.items()
        )
        model_def_columns = f"\n  columns ({columns_str}\n  )," if columns_str else ""

        select_columns = (
            ",\n".join(f"  {column_name}" for column_name in dlt_columns) if dlt_columns else ""
        )
        grain = f"\n  grain ({', '.join(primary_key)})," if primary_key else ""
        incremental_model_name = f"{dataset}_sqlmesh.incremental_{table_name}"

        incremental_model_sql = generate_incremental_model(
            incremental_model_name,
            model_def_columns,
            select_columns,
            grain,
            dataset + "." + table_name,
        )

        sqlmesh_models.add((incremental_model_name, incremental_model_sql))

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
