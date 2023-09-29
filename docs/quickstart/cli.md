# CLI

This page works through the SQLMesh example project using the SQLMesh command-line interface.

## 1. Create the SQLMesh project
First, create a project directory and navigate to it:

```bash
mkdir sqlmesh-example
```
```bash
cd sqlmesh-example
```

If using a python virtual environment, ensure it's activated first by running the `source .env/bin/activate` command from the folder used during [installation](../installation.md).

Create a SQLMesh scaffold with the following command, specifying a default SQL dialect for your models. The dialect should correspond to the dialect most of your models are written in; it can be overridden for specific models in the model's `MODEL` specification. All SQL dialects [supported by the SQLGlot library](https://github.com/tobymao/sqlglot/blob/main/sqlglot/dialects/dialect.py) are allowed.

In this example, we specify the `duckdb` dialect:

```bash
sqlmesh init duckdb
```

See the [quick start overview](../quick_start.md#project-directories-and-files) for more information about the project directories, files, data, and models.

## 2. Create a prod environment

SQLMesh's key actions are creating and applying *plans* to *environments*. At this point, the only environment is the empty `prod` environment.

The first SQLMesh plan must execute every model to populate the production environment. Running `sqlmesh plan` will generate the plan and the following output:

```bash linenums="1"
$ sqlmesh plan
======================================================================
Successfully Ran 1 tests against duckdb
----------------------------------------------------------------------
New environment `prod` will be created from `prod`
Summary of differences against `prod`:
└── Added Models:
    ├── sqlmesh_example.seed_model
    ├── sqlmesh_example.incremental_model
    └── sqlmesh_example.full_model
Models needing backfill (missing dates):
├── sqlmesh_example.full_model: 2020-01-01 - 2023-05-31
├── sqlmesh_example.incremental_model: 2020-01-01 - 2023-05-31
└── sqlmesh_example.seed_model: 2023-05-31 - 2023-05-31
Apply - Backfill Tables [y/n]:
```

Line 3 of the output notes that `sqlmesh plan` successfully executed the project's test `tests/test_full_model.yaml` with duckdb.

Line 5 describes what environments the plan will affect when applied - a new `prod` environment in this case.

Lines 7-10 of the output show that SQLMesh detected three new models relative to the current empty environment.

Lines 11-14 list each model that will be executed by the plan, along with the date intervals that will be run. Note that `full_model` and `incremental_model` both show `2020-01-01` as their start date because:

1. The incremental model specifies that date in the `start` property of its `MODEL` statement and
2. The full model depends on the incremental model.

The `seed_model` date range begins on the same day the plan was made because `SEED` models have no temporality associated with them other than whether they have been modified since the previous SQLMesh plan.

Line 15 asks you whether to proceed with executing the model backfills described in lines 11-14. Enter `y` and press `Enter`, and SQLMesh will execute the models and return this output:

```bash linenums="1"
Apply - Backfill Tables [y/n]: y
       sqlmesh_example.seed_model ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00
sqlmesh_example.incremental_model ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00
       sqlmesh_example.full_model ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00

All model batches have been executed successfully

Virtually Updating 'prod' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully
```

Lines 2-4 show the completion percentage and run time for each model (very fast in this simple example). Line 8 shows that the `prod` environment now points to the tables created during model execution.

You've now created a new production environment with all of history backfilled.

## 3. Update a model

Now that we have have populated the `prod` environment, let's modify one of the SQL models.

We modify the incremental SQL model by adding a new column to the query. Open the `models/incremental_model.sql` file and add `#!sql 'z' AS new_column` below `item_id` as follows:

```sql linenums="1" hl_lines="14"
MODEL (
    name sqlmesh_example.incremental_model,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column ds
    ),
    start '2020-01-01',
    cron '@daily',
    grain [id, ds]
);

SELECT
    id,
    item_id,
	'z' AS new_column, -- Added column
    ds,
FROM
    sqlmesh_example.seed_model
WHERE
    ds between @start_ds and @end_ds
```

## 4. Work with a development environment

### 4.1 Create a dev environment
Now that you've modified a model, it's time to create a development environment so that you can validate the model change without affecting production.

Run `sqlmesh plan dev` to create a development environment called `dev`:

```bash linenums="1"
$ sqlmesh plan dev
======================================================================
Successfully Ran 1 tests against duckdb
----------------------------------------------------------------------
New environment `dev` will be created from `prod`
Summary of differences against `dev`:
├── Directly Modified:
│   └── sqlmesh_example.incremental_model
└── Indirectly Modified:
    └── sqlmesh_example.full_model
---

+++

@@ -1,6 +1,7 @@

 SELECT
   id,
   item_id,
+  'z' AS new_column,
   ds
 FROM sqlmesh_example.seed_model
 WHERE
Directly Modified: sqlmesh_example.incremental_model (Non-breaking)
└── Indirectly Modified Children:
    └── sqlmesh_example.full_model
Models needing backfill (missing dates):
└── sqlmesh_example__dev.incremental_model: 2020-01-01 - 2023-07-16
Enter the backfill start date (eg. '1 year', '2020-01-01') or blank to backfill from the beginning of history:
```

Line 5 of the output states that a new environment `dev` will be created from the existing `prod` environment.

Lines 6-10 summarize the differences between the modified model and the `prod` environment, detecting that we directly modified `incremental_model` and that `full_model` was indirectly modified because it selects from the incremental model.

On line 24, we see that SQLMesh automatically classified the change as `Non-breaking` because understood that the change was additive (added a column not used by `full_model`) and did not invalidate any data already in `prod`.

Hit `Enter` at the prompt to backfill data from our start date `2020-01-01`. Another prompt will appear asking for a backfill end date; hit `Enter` to backfill until now. Finally, enter `y` and press `Enter` to apply the plan and execute the backfill:

```bash linenums="1"
Enter the backfill start date (eg. '1 year', '2020-01-01') or blank to backfill from the beginning of history:
Enter the backfill end date (eg. '1 month ago', '2020-01-01') or blank to backfill up until now:
Apply - Backfill Tables [y/n]: y
Creating new model versions for 'dev' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 2/2 • 0:00:00

All model versions have been created successfully

sqlmesh_example__dev.incremental_model ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00

All model batches have been executed successfully

Virtually Updating 'dev' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully
```

Line 8 of the output shows that SQLMesh applied the change to `sqlmesh_example__dev.incremental_model`. In the model schema, the suffix "`__dev`" indicates that it is in the `dev` environment.

SQLMesh did not need to backfill anything for the `full_model` since the change was `Non-breaking`.

### 4.2 Validate updates in dev
You can now view this change by querying data from `incremental_model` with `sqlmesh fetchdf "select * from sqlmesh_example__dev.incremental_model"`.

Note that the environment name `__dev` is appended to the schema namespace `sqlmesh_example` in the query:

```bash
$ sqlmesh fetchdf "select * from sqlmesh_example__dev.incremental_model"

   id  item_id new_column         ds
0   1        2          z 2020-01-01
1   2        1          z 2020-01-01
2   3        3          z 2020-01-03
3   4        1          z 2020-01-04
4   5        1          z 2020-01-05
5   6        1          z 2020-01-06
6   7        1          z 2020-01-07
```

You can see that `new_column` was added to the dataset. The production table was not modified; you can validate this by querying the production table using `sqlmesh fetchdf "select * from sqlmesh_example.incremental_model"`.

Note that nothing has been appended to the schema namespace `sqlmesh_example` because `prod` is the default environment.

```bash
$ sqlmesh fetchdf "select * from sqlmesh_example.incremental_model"

   id  item_id          ds
0   1        2  2020-01-01
1   2        1  2020-01-01
2   3        3  2020-01-03
3   4        1  2020-01-04
4   5        1  2020-01-05
5   6        1  2020-01-06
6   7        1  2020-01-07
```

The production table does not have `new_column` because the changes to `dev` have not yet been applied to `prod`.

## 5. Update the prod environment

### 5.1 Apply updates to prod
Now that we've tested the changes in dev, it's time to move them to production. Run `sqlmesh plan` to plan and apply your changes to the `prod` environment.

Enter `y` and press `Enter` at the `Apply - Virtual Update [y/n]:` prompt to apply the plan and execute the backfill:

```bash
$ sqlmesh plan
======================================================================
Successfully Ran 1 tests against duckdb
----------------------------------------------------------------------
Summary of differences against `prod`:
├── Directly Modified:
│   └── sqlmesh_example.incremental_model
└── Indirectly Modified:
    └── sqlmesh_example.full_model
---

+++

@@ -1,6 +1,7 @@

 SELECT
   id,
   item_id,
+  'z' AS new_column,
   ds
 FROM sqlmesh_example.seed_model
 WHERE
Directly Modified: sqlmesh_example.incremental_model (Non-breaking)
└── Indirectly Modified Children:
    └── sqlmesh_example.full_model
Apply - Virtual Update [y/n]: y
Virtually Updating 'prod' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully

Virtual Update executed successfully
```

Note that a backfill was not necessary and only a Virtual Update occurred.

### 5.2 Validate updates in prod
Double-check that the data updated in `prod` by running `sqlmesh fetchdf "select * from sqlmesh_example.incremental_model"`:

```bash
$ sqlmesh fetchdf "select * from sqlmesh_example.incremental_model"

   id  item_id new_column         ds
0   1        2          z 2020-01-01
1   2        1          z 2020-01-01
2   3        3          z 2020-01-03
3   4        1          z 2020-01-04
4   5        1          z 2020-01-05
5   6        1          z 2020-01-06
6   7        1          z 2020-01-07
```

## 6. Next steps

Congratulations, you've now conquered the basics of using SQLMesh!

From here, you can:

* [Set up a connection to a database or SQL engine](../guides/connections.md)
* [Learn more about SQLMesh concepts](../concepts/overview.md)
* [Join our Slack community](https://tobikodata.com/slack)
