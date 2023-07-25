# CLI

```
Usage: sqlmesh [OPTIONS] COMMAND [ARGS]...

  SQLMesh command line tool.

Options:
  --version         Show the version and exit.
  -p, --paths TEXT  Path(s) to the SQLMesh config/project.
  --config TEXT     Name of the config object. Only applicable to
                    configuration defined using Python script.
  --gateway TEXT    The name of the gateway.
  --help            Show this message and exit.

Commands:
  audit                   Run audits.
  create_external_models  Create a schema file containing external model...
  dag                     Renders the dag using graphviz.
  diff                    Show the diff between the current context and a...
  evaluate                Evaluate a model and return a dataframe with a...
  fetchdf                 Runs a sql query and displays the results.
  format                  Format all models in a given directory.
  ide                     Start a browser-based SQLMesh IDE.
  info                    Print info.
  init                    Create a new SQLMesh repository.
  migrate                 Migrate SQLMesh to the current running version.
  plan                    Plan a migration of the current context's...
  prompt                  Uses LLM to generate a SQL query from a prompt.
  render                  Renders a model's query, optionally expanding...
  rollback                Rollback SQLMesh to the previous migration.
  run                     Evaluates the DAG of models using the built-in...
  table_diff              Show the diff between two tables.
  test                    Run model unit tests.
  ui                      Start a browser-based SQLMesh UI.
```

## audit
```
Usage: sqlmesh audit [OPTIONS]

  Run audits.

Options:
  --model TEXT       A model to audit. Multiple models can be audited.
  -s, --start TEXT   The start datetime of the interval for which this command
                     will be applied.
  -e, --end TEXT     The end datetime of the interval for which this command
                     will be applied.
  --execution-time TEXT The execution time (defaults to now).
  --help             Show this message and exit.
```

## create_external_models
```
Usage: sqlmesh create_external_models [OPTIONS]

  Create a schema file containing external model schemas.
```

## dag
```
Usage: sqlmesh dag [OPTIONS]

  Renders the dag using graphviz.

  This command requires a manual install of both the python and system
  graphviz package.

Options:
  --file TEXT  The file to which the dag image should be written.
  --help       Show this message and exit.
```

## diff
```
Usage: sqlmesh diff [OPTIONS] ENVIRONMENT

  Show the diff between the current context and a given environment.

Options:
  --help  Show this message and exit.
```

## evaluate
```
Usage: sqlmesh evaluate [OPTIONS] MODEL

  Evaluate a model and return a dataframe with a default limit of 1000.

Options:
  -s, --start TEXT   The start datetime of the interval for which this
                     command will be applied.
  -e, --end TEXT     The end datetime of the interval for which this
                     command will be applied.
  --execution-time TEXT The execution time (defaults to now).
  --limit INTEGER    The number of rows the query should be limited to.
  --help             Show this message and exit.
```

## fetchdf
```
Usage: sqlmesh fetchdf [OPTIONS] SQL

  Runs a sql query and displays the results.

Options:
  --help  Show this message and exit.
```

## format
```
Usage: sqlmesh format [OPTIONS]

  Format all models in a given directory.

Options:
  --help  Show this message and exit.
```

## ide
```
Usage: sqlmesh ide [OPTIONS]

  Start a browser-based SQLMesh IDE.

  WARNING: soft-deprecated, please use `sqlmesh ui` instead.

Options:
  --host TEXT     Bind socket to this host. Default: 127.0.0.1
  --port INTEGER  Bind socket to this port. Default: 8000
  --help          Show this message and exit.
```  

## info
```
Usage: sqlmesh info [OPTIONS]

  Print information about a SQLMesh project.

  Includes counts of project models and macros and connection tests for the data 
  warehouse and test runner.

Options:
  --help  Show this message and exit.
```

## init
```
Usage: sqlmesh init [SQL_DIALECT] [OPTIONS]

  Create a new SQLMesh repository. Argument SQL_DIALECT is required unless the dbt 
  template option is specified.

Options:
  -t, --template TEXT  Project template. Support values: airflow, dbt,
                       default.
  --help               Show this message and exit.
```

## migrate
```
Usage: sqlmesh migrate

  Migrates SQLMesh to the current running version.

  Please contact your SQLMesh administrator before doing this.
```

## plan
```
Usage: sqlmesh plan [OPTIONS] [ENVIRONMENT]

  Plan a migration of the current context's models with the given environment.

Options:
  -s, --start TEXT          The start datetime of the interval for which this
                            command will be applied.
  -e, --end TEXT            The end datetime of the interval for which this
                            command will be applied.
  --execution-time TEXT     The execution time (defaults to now).
  --create-from TEXT        The environment to create the target environment
                            from if it doesn't exist. Default: prod.
  --skip-tests TEXT         Skip tests prior to generating the plan if they
                            are defined.
  -r, --restate-model TEXT  Restate data for specified models and models
                            downstream from the one specified. For production
                            environment, all related model versions will have
                            their intervals wiped, but only the current
                            versions will be backfilled. For development
                            environment, only the current model versions will
                            be affected.
  --no-gaps                 Ensure that new snapshots have no data gaps when
                            comparing to existing snapshots for matching
                            models in the target environment.
  --skip-backfill           Skip the backfill step.
  --forward-only            Create a plan for forward-only changes.
  --effective-from TEXT     The effective date from which to apply forward-
                            only changes on production.
  --no-prompts              Disable interactive prompts for the backfill time
                            range. Please note that if this flag is set and
                            there are uncategorized changes, plan creation
                            will fail.
  --auto-apply              Automatically apply the new plan after creation.
  --no-auto-categorization  Disable automatic change categorization.
  --help                    Show this message and exit.
```

## prompt
```
Usage: sqlmesh prompt [OPTIONS] PROMPT

  Uses LLM to generate a SQL query from a prompt.

Options:
  -e, --evaluate           Evaluate the generated SQL query and display the
                           results.
  -t, --temperature FLOAT  Sampling temperature. 0.0 - precise and
                           predictable, 0.5 - balanced, 1.0 - creative.
                           Default: 0.7
  -v, --verbose            Verbose output.
  --help                   Show this message and exit.
```

## render
```
Usage: sqlmesh render [OPTIONS] MODEL

  Renders a model's query, optionally expanding referenced models.

Options:
  -s, --start TEXT   The start datetime of the interval for which this command
                     will be applied.
  -e, --end TEXT     The end datetime of the interval for which this command
                     will be applied.
  --execution-time TEXT The execution time used (defaults to now).
  --expand TEXT      Whether or not to expand materialized models (defaults to
                     False). If True, all referenced models are expanded as
                     raw queries. Multiple model names can also be specified,
                     in which case only they will be expanded as raw queries.
  --dialect TEXT     The SQL dialect to render the query as.
  --help             Show this message and exit.
```

## rollback
```
Usage: sqlmesh rollback [OPTIONS]

  Rollback SQLMesh to the previous migration.

  Please contact your SQLMesh administrator before doing this.
```

## run
```
Usage: sqlmesh run [OPTIONS] [ENVIRONMENT]

  Evaluates the DAG of models using the built-in scheduler.

Options:
  -s, --start TEXT  The start datetime of the interval for which this command
                    will be applied.
  -e, --end TEXT    The end datetime of the interval for which this command
                    will be applied.
  --skip-janitor    Skip the janitor task.
  --help            Show this message and exit.
```

## table_diff
```
Usage: sqlmesh table_diff [OPTIONS] SOURCE:TARGET [MODEL]

  Show the diff between two tables.

Options:
  -o, --on TEXT    The column to join on. Can be specified multiple times. The
                   model grain will be used if not specified.
  --where TEXT     An optional where statement to filter results.
  --limit INTEGER  The limit of the sample dataframe.
  --help           Show this message and exit.
```

## test
```
Usage: sqlmesh test [OPTIONS] [TESTS]...

  Run model unit tests.

Options:
  -k TEXT        Only run tests that match the pattern of substring.
  -v, --verbose  Verbose output.
  --help         Show this message and exit.
```

## ui
```
Usage: sqlmesh ui [OPTIONS]

  Start a browser-based SQLMesh UI.

Options:
  --host TEXT     Bind socket to this host. Default: 127.0.0.1
  --port INTEGER  Bind socket to this port. Default: 8000
  --help          Show this message and exit.
```
