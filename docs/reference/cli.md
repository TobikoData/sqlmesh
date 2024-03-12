# CLI

```
Usage: sqlmesh [OPTIONS] COMMAND [ARGS]...

  SQLMesh command line tool.

Options:
  --version          Show the version and exit.
  -p, --paths TEXT   Path(s) to the SQLMesh config/project.
  --config TEXT      Name of the config object. Only applicable to
                     configuration defined using Python script.
  --gateway TEXT     The name of the gateway.
  --ignore-warnings  Ignore warnings.
  --debug            Enable debug mode.
  --log-to-stdout    Display logs in stdout.
  --help             Show this message and exit.

Commands:
  audit                   Run audits for the target model(s).
  clean                   Clears the SQLMesh cache and any build artifacts.
  create_external_models  Create a schema file containing external model...
  create_test             Generate a unit test fixture for a given model.
  dag                     Render the DAG as an html file.
  diff                    Show the diff between the local state and the...
  evaluate                Evaluate a model and return a dataframe with a...
  fetchdf                 Run a SQL query and display the results.
  format                  Format all SQL models.
  info                    Print information about a SQLMesh project.
  init                    Create a new SQLMesh repository.
  invalidate              Invalidate the target environment, forcing its...
  migrate                 Migrate SQLMesh to the current running version.
  plan                    Apply local changes to the target environment.
  prompt                  Uses LLM to generate a SQL query from a prompt.
  render                  Render a model's query, optionally expanding...
  rewrite                 Rewrite a SQL expression with semantic...
  rollback                Rollback SQLMesh to the previous migration.
  run                     Evaluate missing intervals for the target...
  table_diff              Show the diff between two tables.
  test                    Run model unit tests.
  ui                      Start a browser-based SQLMesh UI.
```

## audit
```
Usage: sqlmesh audit [OPTIONS]

  Run audits for the target model(s).

Options:
  --model TEXT           A model to audit. Multiple models can be audited.
  -s, --start TEXT       The start datetime of the interval for which this
                         command will be applied.
  -e, --end TEXT         The end datetime of the interval for which this
                         command will be applied.
  --execution-time TEXT  The execution time (defaults to now).
  --help                 Show this message and exit.
```

## clean
```
Usage: sqlmesh clean [OPTIONS]

  Clears the SQLMesh cache and any build artifacts.
```

## create_external_models
```
Usage: sqlmesh create_external_models [OPTIONS]

  Create a schema file containing external model schemas.
```

## create_test
```
Usage: sqlmesh create_test [OPTIONS] MODEL

  Generate a unit test fixture for a given model.

Options:
  -q, --query <TEXT TEXT>...  Queries that will be used to generate data for
                              the model's dependencies.  [required]
  -o, --overwrite             When true, the fixture file will be overwritten
                              in case it already exists.
  -v, --var <TEXT TEXT>...    Key-value pairs that will define variables
                              needed by the model.
  -p, --path TEXT             The file path corresponding to the fixture,
                              relative to the test directory. By default, the
                              fixture will be created under the test directory
                              and the file name will be inferred based on the
                              test's name.
  -n, --name TEXT             The name of the test that will be created. By
                              default, it's inferred based on the model's
                              name.
  --help                      Show this message and exit.
```

## dag
```
Usage: sqlmesh dag [OPTIONS] FILE

  Render the DAG as an html file.

Options:
  --help               Show this message and exit.
  --select-model TEXT  Select specific models to include in the dag.
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

  Run a SQL query and displays the results.

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
Usage: sqlmesh init [OPTIONS] [SQL_DIALECT]

  Create a new SQLMesh repository.

Options:
  -t, --template TEXT  Project template. Supported values: airflow, dbt,
                       default, empty.
  --help               Show this message and exit.
```

## invalidate
```
Usage: sqlmesh invalidate [OPTIONS] ENVIRONMENT

  Invalidate the target environment, forcing its removal during the next run
  of the janitor process.

Options:
  -s, --sync  Wait for the environment to be deleted before returning. If not
              specified, the environment will be deleted asynchronously by the
              janitor process. This option requires a connection to the data
              warehouse.
  --help      Show this message and exit.
```

## migrate
```
Usage: sqlmesh migrate

  Migrate SQLMesh to the current running version.

  Please contact your SQLMesh administrator before doing this.
```

## plan
```
Usage: sqlmesh plan [OPTIONS] [ENVIRONMENT]

  Apply local changes to the target environment.

Options:
  -s, --start TEXT          The start datetime of the interval for which this
                            command will be applied.
  -e, --end TEXT            The end datetime of the interval for which this
                            command will be applied.
  --execution-time TEXT     The execution time (defaults to now).
  --create-from TEXT        The environment to create the target environment
                            from if it doesn't exist. Default: prod.
  --skip-tests              Skip tests prior to generating the plan if they
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
  --include-unmodified      Include unmodified models in the target
                            environment.
  --select-model TEXT       Select specific model changes that should be
                            included in the plan.
  --backfill-model TEXT     Backfill only the models whose names match the
                            expression. This is supported only when targeting
                            a development environment.
  --no-diff                 Hide text differences for changed models.
  --run                     Run latest intervals as part of the plan
                            application (prod environment only).
  --enable-preview          Enable preview for forward-only models when
                            targeting a development environment.
  -v, --verbose             Verbose output.
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

  Render a model's query, optionally expanding referenced models.

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

## rewrite
```
Usage: sqlmesh rewrite [OPTIONS] SQL

  Rewrite a SQL expression with semantic references into an executable query.

  https://sqlmesh.readthedocs.io/en/latest/concepts/metrics/overview/

Options:
  --read TEXT   The input dialect of the sql string.
  --write TEXT  The output dialect of the sql string.
  --help        Show this message and exit.
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

  Evaluate missing intervals for the target environment.

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
