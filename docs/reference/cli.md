# CLI

```
Usage: sqlmesh [OPTIONS] COMMAND [ARGS]...

  SQLMesh command line tool.

Options:
  --path TEXT    Path to the models directory.
  --config TEXT  Name of the config object.
  --help         Show this message and exit.

Commands:
  audit     Run audits.
  dag       Renders the dag using graphviz.
  diff      Show the diff between the current context and a given...
  evaluate  Evaluate a model and return a dataframe with a default limit...
  fetchdf   Runs a sql query and displays the results.
  format    Format all models in a given directory.
  init      Create a new SQLMesh repository.
  plan      Plan a migration of the current context's models with the...
  render    Renders a model's query, optionally expanding referenced models.
  run       Evaluates the DAG of models using the built-in scheduler.
  test      Run model unit tests.
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
  --no-prompts              Disable interactive prompts for the backfill time
                            range. Note that if this flag is set and
                            there are uncategorized changes, plan creation
                            will fail.
  --auto-apply              Automatically apply the new plan after creation.
  --no-auto-categorization  Disable automatic change categorization.
  --help                    Show this message and exit.
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
  -l, --latest TEXT  The latest time used for non-incremental datasets
                     (defaults to now).
  --limit INTEGER    The number of rows the query should be limited to.
  --help             Show this message and exit.
```

## render
```
Usage: sqlmesh render [OPTIONS] MODEL

  Renders a model's query, optionally expanding referenced models.

Options:
  -s, --start TEXT   The start datetime of the interval for which this 
                     command will be applied.
  -e, --end TEXT     The end datetime of the interval for which this 
                     command will be applied.
  -l, --latest TEXT  The latest time used for non-incremental datasets
                     (defaults to now).
  --expand TEXT      Whether or not to expand materialized models (defaults to
                     False). If True, all referenced models are expanded as
                     raw queries. Multiple model names can also be specified,
                     in which case only they will be expanded as raw queries.
  --help             Show this message and exit.
```

## fetchdf
```
Usage: sqlmesh fetchdf [OPTIONS] SQL

  Runs a sql query and displays the results.

Options:
  --help  Show this message and exit.
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

## audit
```
Usage: sqlmesh audit [OPTIONS]

  Run audits.

Options:
  --model TEXT       A model to audit. Multiple models can be audited.
  -s, --start TEXT   The start datetime of the interval for which this 
                     command will be applied.
  -e, --end TEXT     The end datetime of the interval for which this 
                     command will be applied.
  -l, --latest TEXT  The latest time used for non-incremental datasets
                     (defaults to now).
  --help             Show this message and exit.
```

## format
```
Usage: sqlmesh format [OPTIONS]

  Format all models in a given directory.

Options:
  --help  Show this message and exit.
```

## diff
```
Usage: sqlmesh diff [OPTIONS] ENVIRONMENT

  Show the diff between the current context and a given environment.

Options:
  --help  Show this message and exit.
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
