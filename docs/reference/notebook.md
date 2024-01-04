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

### Magic Commands

All available magic commands can be listed with `%lsmagic` and then the docstring for any given magic can be displayed with `%magic_name?` for line magics and `%%magic_name?` for call magics.

#### context
```
%context [--config CONFIG] [--gateway GATEWAY] [--ignore-warnings]
               [--debug]
               paths [paths ...]

Sets the context in the user namespace.

positional arguments:
  paths              The path(s) to the SQLMesh project(s).

options:
  --config CONFIG    Name of the config object. Only applicable to
                     configuration defined using Python script.
  --gateway GATEWAY  The name of the gateway.
  --ignore-warnings  Ignore warnings.
  --debug            Enable debug mode.
```

#### init
```
%init [--template TEMPLATE] path sql_dialect

Creates a SQLMesh project scaffold with a default SQL dialect.

positional arguments:
  path                  The path where the new SQLMesh project should be
                        created.
  sql_dialect           Default model SQL dialect. Supported values: '',
                        'bigquery', 'clickhouse', 'databricks', 'doris',
                        'drill', 'duckdb', 'hive', 'mysql', 'oracle',
                        'postgres', 'presto', 'redshift', 'snowflake',
                        'spark', 'spark2', 'sqlite', 'starrocks', 'tableau',
                        'teradata', 'trino', 'tsql'.

options:
  --template TEMPLATE, -t TEMPLATE
                        Project template. Supported values: airflow, dbt,
                        default, empty.
```

#### plan
```
%plan [--start START] [--end END] [--execution-time EXECUTION_TIME]
            [--create-from CREATE_FROM] [--skip-tests]
            [--restate-model [RESTATE_MODEL ...]] [--no-gaps]
            [--skip-backfill] [--forward-only]
            [--effective-from EFFECTIVE_FROM] [--no-prompts] [--auto-apply]
            [--no-auto-categorization] [--include-unmodified]
            [--select-model [SELECT_MODEL ...]]
            [--backfill-model [BACKFILL_MODEL ...]] [--no-diff] [--run]
            [environment]

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
  --include-unmodified  Include unmodified models in the target environment.
  --select-model <[SELECT_MODEL ...]>
                        Select specific model changes that should be included
                        in the plan.
  --backfill-model <[BACKFILL_MODEL ...]>
                        Backfill only the models whose names match the
                        expression. This is supported only when targeting a
                        development environment.
  --no-diff             Hide text differences for changed models.
  --run                 Run latest intervals as part of the plan application
                        (prod environment only).
```

#### run_dag
```
%run_dag [--start START] [--end END] [--skip-janitor] [--ignore-cron]
               [environment]

Evaluate the DAG of models using the built-in scheduler.

positional arguments:
  environment           The environment to run against

options:
  --start START, -s START
                        Start date to evaluate.
  --end END, -e END     End date to evaluate.
  --skip-janitor        Skip the janitor task.
  --ignore-cron         Run for all missing intervals, ignoring individual
                        cron schedules.
```

#### evaluate
```
%evaluate [--start START] [--end END] [--execution-time EXECUTION_TIME]
                [--limit LIMIT]
                model

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

#### render
```
%render [--start START] [--end END] [--execution-time EXECUTION_TIME]
              [--expand EXPAND] [--dialect DIALECT] [--no-format]
              model

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
  --no-format           Disable fancy formatting of the query.
```

#### dag
```
%dag [--file FILE]

Displays the HTML DAG.

options:
  --file FILE, -f FILE  An optional file path to write the HTML output to.
```

#### fetchdf
```
%%fetchdf [df_var]

Fetches a dataframe from sql, optionally storing it in a variable.

positional arguments:
  df_var  An optional variable name to store the resulting dataframe.
```

#### test
```
%test [--ls] model [test_name]

Allow the user to list tests for a model, output a specific test, and then write their changes back

positional arguments:
  model      The model.
  test_name  The test name to display

options:
  --ls       List tests associated with a model
```

#### migrate
```
%migrate

Migrate SQLMesh to the current running version
```

#### create_external_models
```
%create_external_models

Create a schema file containing external model schemas.
```

#### table_diff
```
%table_diff [--on [ON ...]] [--model MODEL] [--where WHERE]
                  [--limit LIMIT] [--show-sample]
                  SOURCE:TARGET

Show the diff between two tables.

Can either be two tables or two environments and a model.

positional arguments:
  SOURCE:TARGET    Source and target in `SOURCE:TARGET` format

options:
  --on <[ON ...]>  The column to join on. Can be specified multiple times. The
                   model grain will be used if not specified.
  --model MODEL    The model to diff against when source and target are
                   environments and not tables.
  --where WHERE    An optional where statement to filter results.
  --limit LIMIT    The limit of the sample dataframe.
  --show-sample    Show a sample of the rows that differ. With many columns,
                   the output can be very wide.
```

#### model
```
%model [--start START] [--end END] [--execution-time EXECUTION_TIME]
             [--dialect DIALECT]
             model

Renders the model and automatically fills in an editable cell with the model definition.

positional arguments:
  model                 The model.

options:
  --start START, -s START
                        Start date to render.
  --end END, -e END     End date to render.
  --execution-time EXECUTION_TIME
                        Execution time.
  --dialect DIALECT, -d DIALECT
                        The rendered dialect.
```

#### diff
```
%diff environment

Show the diff between the local state and the target environment.

positional arguments:
  environment  The environment to diff local state against.
```

#### invalidate
```
%invalidate environment

Invalidate the target environment, forcing its removal during the next run of the janitor process.

positional arguments:
  environment  The environment to invalidate.
```

#### create_test
```
%create_test --query QUERY [QUERY ...] [--overwrite]
                   [--var VAR [VAR ...]] [--path PATH] [--name NAME]
                   model

Generate a unit test fixture for a given model.

positional arguments:
  model

options:
  --query <QUERY [QUERY ...]>, -q <QUERY [QUERY ...]>
                        Queries that will be used to generate data for the
                        model's dependencies.
  --overwrite, -o       When true, the fixture file will be overwritten in
                        case it already exists.
  --var <VAR [VAR ...]>, -v <VAR [VAR ...]>
                        Key-value pairs that will define variables needed by
                        the model.
  --path PATH, -p PATH  The file path corresponding to the fixture, relative
                        to the test directory. By default, the fixture will be
                        created under the test directory and the file name
                        will be inferred based on the test's name.
  --name NAME, -n NAME  The name of the test that will be created. By default,
                        it's inferred based on the model's name.
```

#### run_test
```
%run_test [--pattern [PATTERN ...]] [--verbose] [tests ...]

Run unit test(s).

positional arguments:
  tests

options:
  --pattern <[PATTERN ...]>, -k <[PATTERN ...]>
                        Only run tests that match the pattern of substring.
  --verbose, -v         Verbose output.
```

#### audit
```
%audit [--start START] [--end END] [--execution-time EXECUTION_TIME]
             [models ...]

Run audit(s)

positional arguments:
  models                A model to audit. Multiple models can be audited.

options:
  --start START, -s START
                        Start date to audit.
  --end END, -e END     End date to audit.
  --execution-time EXECUTION_TIME
                        Execution time.
```

#### rollback
```
%rollback

Rollback SQLMesh to the previous migration.
```

#### rewrite
```
%rewrite [--read READ] [--write WRITE]

Rewrite a sql expression with semantic references into an executable query.

https://sqlmesh.readthedocs.io/en/latest/concepts/metrics/overview/

options:
  --read READ    The input dialect of the sql string.
  --write WRITE  The output dialect of the sql string.
```
