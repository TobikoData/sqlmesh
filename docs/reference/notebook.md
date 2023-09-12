# Notebook

SQLMesh supports Jupyter and Databricks Notebooks. Magics are loaded automatically when `sqlmesh` or one of its modules is imported.

## SQLMesh project setup

Notebooks locate a SQLMesh project by setting a `context` with either the Python API or a notebook magic.

Set the context with the Python `Context` function as follows:

```python
from sqlmesh import Context

context = Context(paths="path_to_sqlmesh_project")
```

Alternatively, set the context with a notebook magic:

``` python
import sqlmesh

%context path_to_sqlmesh_project
```

### Quick start project

If desired, you can create the [quick start example project](../quick_start.md) with the Python `init_example_project` function. The function requires a default SQL dialect for the project's models; this example uses `snowflake`:

```python
from sqlmesh.cli.example_project import init_example_project

init_example_project("path_to_project_directory", dialect="snowflake")
```

Alternatively, create the project with a notebook magic:

```python
%init path_to_project_directory snowflake
```

### Databricks notebooks
To use your Databricks cluster, update the `default_connection` and `test_connection` in the `config.yaml` file.

See the [Execution Engines](../integrations/engines/databricks.md) page for information on configuring a Databricks connection.

## context
```
%context paths [paths ...]

Sets the context in the user namespace.

positional arguments:
  paths  The path(s) to the SQLMesh project(s).
```

## init
```
%init path sql_dialect [--template TEMPLATE]

Creates a SQLMesh project scaffold. Argument `sql_dialect` is required unless the dbt
template option is specified.

positional arguments:
  path                  The path where the new SQLMesh project should be
                        created.
  sql_dialect           Default model SQL dialect. Supported values: '',
                        'bigquery', 'clickhouse', 'databricks', 'drill',
                        'duckdb', 'hive', 'mysql', 'oracle', 'postgres',
                        'presto', 'redshift', 'snowflake', 'spark', 'spark2',
                        'sqlite', 'starrocks', 'tableau', 'teradata', 'trino',
                        'tsql'.

options:
  --template TEMPLATE, -t TEMPLATE
                        Project template. Supported values: airflow, dbt,
                        default.
```

## plan
```
%plan environment [--start START] [--end END] [--execution-time EXECUTION_TIME]
            [--create-from CREATE_FROM] [--skip-tests]
            [--restate-model [RESTATE_MODEL ...]] [--no-gaps]
            [--skip-backfill] [--forward-only]
            [--effective-from EFFECTIVE_FROM] [--no-prompts] [--auto-apply]
            [--no-auto-categorization]


Goes through a set of prompts to both establish a plan and apply it

positional arguments:
  environment           The environment to run the plan against

options:
  --start START, -s START
                        Start date to backfill.
  --end END, -e END     End date to backfill.
  --execution-time EXECUTION_TIME
                        Execution time.
  --create-from CREATE_FROM
                        The environment to create the target environment from
                        if it doesn't exist. Default: prod.
  --skip-tests, -t      Skip the unit tests defined for the model.
  --restate-model <[RESTATE_MODEL ...]>, -r <[RESTATE_MODEL ...]>
                        Restate data for specified models (and models
                        downstream from the one specified). For production
                        environment, all related model versions will have
                        their intervals wiped, but only the current versions
                        will be backfilled. For development environment, only
                        the current model versions will be affected.
  --no-gaps, -g         Ensure that new snapshots have no data gaps when
                        comparing to existing snapshots for matching models in
                        the target environment.
  --skip-backfill       Skip the backfill step.
  --forward-only        Create a plan for forward-only changes.
  --effective-from EFFECTIVE_FROM
                        The effective date from which to apply forward-only
                        changes on production.
  --no-prompts          Disables interactive prompts for the backfill time
                        range. Please note that if this flag is set and there
                        are uncategorized changes, plan creation will fail.
  --auto-apply          Automatically applies the new plan after creation.
  --no-auto-categorization
                        Disable automatic change categorization.
```

## evaluate
```
%evaluate model [--start START] [--end END] [--execution-time EXECUTION_TIME] [--limit LIMIT]

Evaluate a model query and fetches a dataframe.

positional arguments:
  model                 The model.

options:
  --start START, -s START
                        Start date to render.
  --end END, -e END     End date to render.
  --execution-time EXECUTION_TIME
                        Execution time.
  --limit LIMIT         The number of rows which the query should be limited
                        to.
```

## render
```
%render model [--start START] [--end END] [--execution-time EXECUTION_TIME] [--expand EXPAND]
              [--dialect DIALECT]


Renders a model's query, optionally expanding referenced models.

positional arguments:
  model                 The model.

options:
  --start START, -s START
                        Start date to render.
  --end END, -e END     End date to render.
  --execution-time EXECUTION_TIME
                        Execution time.
  --expand EXPAND       Whether or not to use expand materialized models,
                        defaults to False. If True, all referenced models are
                        expanded as raw queries. If a list, only referenced
                        models are expanded as raw queries.
  --dialect DIALECT     SQL dialect to render.
```

## fetchdf
```
%%fetchdf [df_var]

Fetch a dataframe with a cell's SQL query, optionally storing it in a variable.

positional arguments:
  df_var                An optional variable name to store the resulting
                        dataframe.
```

## test
```
%test model [test_name] [--ls]

Allow the user to list tests for a model, output a specific test, and
then write their changes back.

positional arguments:
  model      The model.
  test_name  The test name to display.

options:
  --ls       List tests associated with a model.
```

## table_diff
```
%table_diff --source SOURCE --target TARGET --on ON [ON ...]
              [--model MODEL] [--where WHERE] [--limit LIMIT]

Show the diff between two tables.

Can either be two tables or two environments and a model.

options:
  --source SOURCE, -s SOURCE
                        The source environment or table.
  --target TARGET, -t TARGET
                        The target environment or table.
  --on <ON [ON ...]>    The SQL join condition or list of columns to use as
                        keys. Table aliases must be "s" and "t" for source and
                        target.
  --model MODEL         The model to diff against when source and target are
                        environments and not tables.
  --where WHERE         An optional where statement to filter results.
  --limit LIMIT         The limit of the sample dataframe.
```

## rewrite
```
%rewrite [--read READ] [--write WRITE]

Rewrite a sql expression with semantic references into an executable query.

https://sqlmesh.readthedocs.io/en/latest/concepts/metrics/overview/

options:
  --read READ           The input dialect of the sql string.
  --write WRITE, -t WRITE
                        The output dialect of the sql string.
```
