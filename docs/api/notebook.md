# Notebook

SQLMesh supports JupyterLabs and Databricks Notebooks. Magics are loaded automatically and use the variable `context`.

```python
from sqlmesh import Context

context = Context(path="example")
```

## plan
```
%plan [--start START] [--end END] [--from FROM_] [--skip-tests]
        [--restate-model [RESTATE_MODEL ...]] [--no-gaps]
        [--skip-backfill] [--forward-only] [--no-prompts] [--auto-apply]
        [environment]

Iterates through a set of prompts to both establish a plan and apply it.

positional arguments:
  environment           The environment to run the plan against.

options:
  --start START, -s START
                        Start date to backfill.
  --end END, -e END     End date to backfill.
  --from FROM_, -f FROM_
                        The environment to base the plan on rather than local
                        files.
  --skip-tests, -t      Skip the unit tests defined for the model.
  --restate-model <[RESTATE_MODEL ...]>, -r <[RESTATE_MODEL ...]>
                        Restate data for specified models (and models
                        downstream from the one specified). For production
                        environment, all related model versions will have their
                        intervals wiped, but only the current versions will be
                        backfilled. For development environment, only the
                        current model versions will be affected.
  --no-gaps, -g         Ensure that new snapshots have no data gaps when
                        comparing to existing snapshots for matching models in
                        the target environment.
  --skip-backfill       Skip the backfill step.
  --forward-only        Create a plan for forward-only changes.
  --no-prompts          Disables interactive prompts for the backfill time
                        range. Please note that if this flag is set and there
                        are uncategorized changes, plan creation will fail.
  --auto-apply          Automatically applies the new plan after creation.
```

## evaluate
```

%evaluate [--start START] [--end END] [--latest LATEST] [--limit LIMIT]
            model

Evaluate a model query and fetch a dataframe.

positional arguments:
  model                 The model.

options:
  --start START, -s START
                        Start date to render.
  --end END, -e END     End date to render.
  --latest LATEST, -l LATEST
                        Latest date to render.
  --limit LIMIT         The number of rows for which which the query 
                        should be limited.
```

## render
TODO

## fetchdf
```
%fetchdf [df_var]

Fetches a dataframe from sql, optionally storing it in a variable.

positional arguments:
  df_var  An optional variable name to store the resulting dataframe.
```

## test
```
  %test [--ls] model [test_name]

Allow the user to list tests for a model, output a specific test, and 
then write their changes back.

positional arguments:
  model      The model.
  test_name  The test name to display.

options:
  --ls       List tests associated with a model.
```

## audit
TODO

## format
TODO

## diff
TODO

## dag
```
 %dag

Displays the dag.
```
