import typing as t
import click
from datetime import datetime, timedelta, timezone
from pydantic import ValidationError
from sqlglot import exp, parse_one
from sqlmesh.core.config.connection import parse_connection_config
from sqlmesh.core.context import Context
from sqlmesh.utils.date import yesterday_ds


def generate_dlt_models_and_settings(
    pipeline_name: str, dialect: str, tables: t.Optional[t.List[str]] = None
) -> t.Tuple[t.Set[t.Tuple[str, str]], str, str]:
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
    storage_ids = list(pipeline._get_load_storage().list_loaded_packages())
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
        if (
            (has_table_seen_data(table) and not name.startswith(schema._dlt_tables_prefix))
            or name == schema.loads_table_name
        )
        and (name in tables if tables else True)
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

        select_columns = (
            ",\n".join(f"  {column_name}" for column_name in dlt_columns) if dlt_columns else ""
        )

        dlt_columns["_dlt_load_time"] = exp.DataType.build("TIMESTAMP", dialect=dialect)
        columns_str = ",".join(
            f"\n    {name} {data_type.sql(dialect=dialect)}"
            for name, data_type in dlt_columns.items()
        )
        model_def_columns = f"\n  columns ({columns_str}\n  )," if columns_str else ""

        grain = f"\n  grain ({', '.join(primary_key)})," if primary_key else ""
        incremental_model_name = f"{dataset}_sqlmesh.incremental_{table_name}"

        incremental_model_sql = generate_incremental_model(
            incremental_model_name,
            model_def_columns,
            select_columns,
            grain,
            dataset + "." + table_name,
            dialect,
        )

        sqlmesh_models.add((incremental_model_name, incremental_model_sql))

    return sqlmesh_models, format_config(configs, db_type), get_start_date(storage_ids)


def generate_dlt_models(
    context: Context, pipeline_name: str, tables: t.List[str], force: bool
) -> t.List[str]:
    from sqlmesh.cli.example_project import _create_models

    sqlmesh_models, _, _ = generate_dlt_models_and_settings(
        pipeline_name=pipeline_name,
        dialect=context.config.dialect or "",
        tables=tables if tables else None,
    )

    if not tables and not force:
        existing_models = [m.name for m in context.models.values()]
        sqlmesh_models = {model for model in sqlmesh_models if model[0] not in existing_models}

    if sqlmesh_models:
        _create_models(models_path=context.path / "models", models=sqlmesh_models)
        return [model[0] for model in sqlmesh_models]
    return []


def generate_incremental_model(
    model_name: str,
    model_def_columns: str,
    select_columns: str,
    grain: str,
    from_table: str,
    dialect: str,
) -> str:
    """Generate the SQL definition for an incremental model."""

    load_id = "load_id" if from_table.endswith("_dlt_loads") else "_dlt_load_id"
    time_column = parse_one(f"to_timestamp(CAST({load_id} AS DOUBLE))").sql(dialect=dialect)

    return f"""MODEL (
  name {model_name},
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column _dlt_load_time,
  ),{model_def_columns}{grain}
);

SELECT
{select_columns},
  {time_column} as _dlt_load_time
FROM
  {from_table}
WHERE
  {time_column} BETWEEN @start_ds AND @end_ds
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


def get_start_date(load_ids: t.List[str]) -> str:
    """Convert the earliest load_id to UTC timestamp, subtract a day and format as 'YYYY-MM-DD'."""

    timestamps = [datetime.fromtimestamp(float(id), tz=timezone.utc) for id in load_ids]
    if timestamps:
        start_timestamp = min(timestamps) - timedelta(days=1)
        return start_timestamp.strftime("%Y-%m-%d")
    else:
        return yesterday_ds()
