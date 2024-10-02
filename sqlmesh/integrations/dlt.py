import typing as t

def extract_dlt_models(pipeline_name: str, dialect: str) -> t.List[t.Tuple[str,str]]:

    import dlt
    from dlt.common.schema.utils import has_table_seen_data
    from dlt.pipeline.exceptions import CannotRestorePipelineException
    
    try:
        pipeline = dlt.attach(pipeline_name=pipeline_name)
    except CannotRestorePipelineException:
        raise "Could not attach to pipeline {pipeline_name}"

    schema = pipeline.default_schema
    dataset = pipeline.dataset_name

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
        model_def_columns = format_columns(dlt_columns)
        select_columns = format_columns_for_select(dlt_columns)
        grain = format_grain(primary_key)
        INCREMENTAL_MODEL_NAME = f"{SQLMESH_SCHEMA_NAME}.incremental_{table_name}"
        FULL_MODEL_NAME = f"{SQLMESH_SCHEMA_NAME}.full_{table_name}"
        
        incremental_model_sql = generate_incremental_model(INCREMENTAL_MODEL_NAME, model_def_columns, select_columns, grain, dataset+"."+table_name)
        full_model_sql = generate_full_model(FULL_MODEL_NAME, model_def_columns, select_columns, grain, INCREMENTAL_MODEL_NAME)

        sqlmesh_models.extend([
            (INCREMENTAL_MODEL_NAME, incremental_model_sql),
            (FULL_MODEL_NAME, full_model_sql)
        ])

    return sqlmesh_models
       


def extract_columns_and_primary_key(table: t.Dict[str, t.Any], dialect: str):
    """Extract column information and primary key for a given DLT table."""
    from sqlglot import exp, parse_one
    from dlt.common.schema.utils import is_complete_column

    dlt_columns = {
        c["name"]: parse_one(c["data_type"], into=exp.DataType, dialect=dialect)
        for c in filter(is_complete_column, table["columns"].values())
    }
    primary_key = [
        c["name"] for c in filter(is_complete_column, table["columns"].values()) if c.get("primary_key")
    ]
    return dlt_columns, primary_key


def generate_incremental_model(model_name: str, model_def_columns:str, select_columns: str, grain: str, from_table: str) -> str:
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

def generate_full_model(model_name: str, model_def_columns:str, select_columns: str, grain: str, from_table: str) -> str:
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


def format_columns(dlt_columns: t.Dict[str, str]) -> str:
    """Format the columns for the SQLMesh model definition."""
    return f"\n  columns ({',\n    '.join(f'{name} {data_type}' for name, data_type in dlt_columns.items())}\n  ),"  if dlt_columns else ""


def format_grain(primary_key: t.List[str]) -> str:
    """Format the grain for the SQLMesh model definition."""
    if primary_key:
        return f"\n  grain ({', '.join(primary_key)}),"
    return ""


def format_columns_for_select(dlt_columns: t.Dict[str, str]) -> str:
    """Format the columns for the SELECT statement in the model."""
    return ',\n'.join(f'  {column_name}' for column_name in dlt_columns)  if dlt_columns else ""