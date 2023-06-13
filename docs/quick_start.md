# Quick start
In this quick start guide, you'll get up and running with SQLMesh's scaffold generator. This example project will run locally on your computer using [DuckDB](https://duckdb.org/) as an embedded SQL engine.

Before beginning, ensure that you meet all the [prerequisites](prerequisites.md) for using SQLMesh.

## Project structure

This project demonstrates key SQLMesh features by walking through the SQLMesh workflow on a simple data pipeline. This section describes the project structure and the SQLMesh concepts you will encounter as you work through it.

The project contains three models with a CSV file as the only data source: 

```
┌─────────────┐
│seed_data.csv│
└────────────┬┘
             │
            ┌▼─────────────┐
            │seed_model.sql│
            └─────────────┬┘
                          │
                         ┌▼────────────────────┐
                         │incremental_model.sql│
                         └────────────────────┬┘
                                              │
                                             ┌▼─────────────┐
                                             │full_model.sql│
                                             └──────────────┘
```

Although the project is simple, it touches on all the primary concepts needed to use SQLMesh productively. 

### Plans

SQLMesh's key actions are creating and applying *plans* to *environments*. 

A [SQLMesh environment](./concepts/environments.md) is an isolated namespace containing models and the data they generated. The most important environment is `prod` ("production"), which consists of the databases behind the applications your business uses to operate each day. Environments other than `prod` provide a place where you can test and preview changes to model code before they go live and affect business operations. 

A [SQLMesh plan](./concepts/plans.md) contains a comparison of one environment to another and the set of changes needed to bring them into alignment. For example, if a new SQL model was added, tested, and run in the `dev` environment, it would need to be added and run in the `prod` environment to bring them into alignment. SQLMesh identifies all such changes and classifies them as either breaking or non-breaking. 

Breaking changes are those that invalidate data already existing in an environment. For example, if a `WHERE` clause was added to a model in the `dev` environment, existing data created by that model in the `prod` environment are now invalid because they may contain rows that would be filtered out by the new `WHERE` clause. Other changes, like adding a new column to a model in `dev`, are non-breaking because all the existing data in `prod` are still valid to use - only new data must be added to align the environments.

After SQLMesh creates a plan, it summarizes the breaking and non-breaking changes so you can understand what will happen if you apply the plan. It will prompt you to "backfill" data to apply the plan - in this context, backfill is a generic term for updating or adding to a table's data.

### Model kinds

A plan's actions are determined by the [kinds](./concepts/models/model_kinds.md) of models the project uses. This example project uses three model kinds:

1. [`SEED` models](./concepts/models/model_kinds.md#seed) read data from CSV files stored in the SQLMesh project directory.
2. [`FULL` models](./concepts/models/model_kinds.md#full) fully refresh (rewrite) the data associated with the model every time the model is run.
3. [`INCREMENTAL_BY_TIME_RANGE` models](./concepts/models/model_kinds.md#incremental_by_time_range) use a date/time data column to track which time intervals are affected by a plan and process only the affected intervals when a model is run.


## 1. Create the SQLMesh project
Create a project directory and navigate to it, as in the following example:

```bash
mkdir sqlmesh-example
```
```bash
cd sqlmesh-example
```

When using a python virtual environment, you must ensure it's activated first by running the `source .env/bin/activate` command from the folder used during [installation](installation.md).

Create a SQLMesh scaffold by using the following command:

```bash
sqlmesh init
```

This will create the directories that you can use to organize your SQLMesh project code:

- config.yaml
    - The file for project configuration. Refer to [configuration](reference/configuration.md).
- ./models
    - SQL and Python models. Refer to [models](concepts/models/overview.md).
- ./seeds
    - Seed files. Refer to [seeds](concepts/models/seed_models.md).
- ./audits
    - Shared audit files. Refer to [auditing](concepts/audits.md).
- ./tests
    - Unit test files. Refer to [testing](concepts/tests.md).
- ./macros
    - Macro files. Refer to [macros](concepts/macros/overview.md).

It will also create the files needed for this quickstart:

- ./models
    - full_model.sql
    - incremental_model.sql
    - seed_model.sql
- ./seeds
    - seed_data.csv
- ./audits
    - assert_positive_order_ids.sql
- ./tests
    - test_full_model.yaml

### Project data

The data used in this example project is contained in the `seed_data.csv` file in the `/seeds` project directory. The data reflects sales of 3 items over 7 days in January 2020.

The file contains three columns, `id`, `item_id`, and `ds`, which correspond to each row's unique ID, the sold item's ID number, and the date the item was sold, respectively.

This is the complete dataset:

```linenums="1"
id,item_id,ds
1,2,2020-01-01
2,1,2020-01-01
3,3,2020-01-03
4,1,2020-01-04
5,1,2020-01-05
6,1,2020-01-06
7,1,2020-01-07
```

### Project configuration

SQLMesh project-level configuration parameters are specified in the `config.yaml` file in the project directory.

This example project uses the embedded DuckDB SQL engine, so its configuration specifies `duckdb` as the local gateway's connection and the `local` gateway as the default:

```yaml linenums="1"
gateways:
    local:
        connection:
            type: duckdb
            database: db.db

default_gateway: local
```

Learn more about SQLMesh project configuration [here](./reference/configuration.md).

### Project models

We now briefly review each model in the project. 

The first model is a `SEED` model that imports `seed_data.csv`. This model consists of only a `MODEL` statement because `SEED` models do not query a database. In addition to specifying the model name and CSV path relative to the model file, it includes the column names and data types of the columns in the CSV:

```sql linenums="1"
MODEL (
    name sqlmesh_example.seed_model,
    kind SEED (
        path '../seeds/seed_data.csv'
    ),
    columns (
        id INTEGER,
        item_id INTEGER,
        ds VARCHAR
    )
);
```

The second model is an `INCREMENTAL_BY_TIME_RANGE` model that includes both a `MODEL` statement and a SQL query selecting from the first seed model. 

The `MODEL` statement's `kind` property includes the required specification of the data column containing each record's timestamp. It also includes the optional `start` property specifying the earliest date/time for which the model should process data and the `cron` property specifying that the model should run daily.

The SQL query includes a `WHERE` clause that SQLMesh uses to filter the data to a specific date/time interval when loading data incrementally:

```sql linenums="1"
MODEL (
    name sqlmesh_example.incremental_model,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column ds
    ),
    start '2020-01-01',
    cron '@daily',
);

SELECT
    id,
    item_id,
    ds,
FROM
    sqlmesh_example.seed_model
WHERE
    ds between @start_ds and @end_ds
```

The final model in the project is a `FULL` model. In addition to properties used in the other models, its `MODEL` statement includes the [`audits`](./concepts/audits.md) property. The project includes a custom `assert_positive_order_ids` audit in the project `audits` directory that verifies that all `item_id` values are positive numbers. It will be run every time the model is executed.

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  audits [assert_positive_order_ids],
);

SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
GROUP BY item_id
```

## 2. Plan and apply environments
### 2.1 Create a prod environment

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

Lines 11-14 list each model that will be executed by the plan, along with the date intervals that will be run. Note that `full_model` and `incremental_model` both show `2020-01-01` as their start date because (i) the incremental model specifies that date in the `start` property of its `MODEL` statement and (ii) the full model depends on the incremental model. The `seed_model` date range begins on the same day the plan was made because `SEED` models have no temporality associated with them other than whether they have been modified since the previous SQLMesh plan.

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

### 2.2 Create a dev environment
Now that you've created a production environment, it's time to create a development environment so that you can modify models without affecting production. Run `sqlmesh plan dev` to create a development environment called `dev`:

```bash linenums="1"
$ sqlmesh plan dev
======================================================================
Successfully Ran 1 tests against duckdb
----------------------------------------------------------------------
New environment `dev` will be created from `prod`
Apply - Virtual Update [y/n]: 
```

The output does not list any added or modified models because `dev` is being created from the existing `prod` environment without modification. 

Line 6 shows that when you apply the plan creating the `dev` environment, it will only involve a Virtual Update. This is because SQLMesh is able to safely reuse the tables you've already backfilled in the `prod` environment. Enter `y` and press `Enter` to perform the Virtual Update:

```bash linenums="1"
Apply - Virtual Update [y/n]: y
Virtually Updating 'dev' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully


Virtual Update executed successfully
```

The output confirms that the `dev` environment has been updated successfully.

## 3. Make your first update

Now that we have have populated both `prod` and `dev` environments, let's modify one of the SQL models, validate it in `dev`, and push it to `prod`.

### 3.1 Edit the configuration
We modify the incremental SQL model by adding a new column to the query. Open the `models/incremental_model.sql` file and add `#!sql 'z' AS new_column` below `item_id` as follows:

```sql linenums="1" hl_lines="13"
MODEL (
    name sqlmesh_example.incremental_model,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column ds
    ),
    start '2020-01-01',
    cron '@daily',
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

## 4. Plan and apply updates
We can preview the impact of the change using the `sqlmesh plan dev` command:

```bash linenums="1"
$ sqlmesh plan dev
======================================================================
Successfully Ran 1 tests against duckdb
----------------------------------------------------------------------
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
└── sqlmesh_example__dev.incremental_model: 2020-01-01 - 2023-05-31
Enter the backfill start date (eg. '1 year', '2020-01-01') or blank to backfill from the beginning of history:
```

Lines 5-9 of the output summarize the differences between the modified project components and the existing `dev` environment, detecting that we directly modified `incremental_model` and that `full_model` was indirectly modified because it selects from the incremental model. 

On line 23, we see that SQLMesh understood that the change was additive (added a column not used by `full_model`) and was automatically classified as a non-breaking change.

Hit `Enter` at the prompt to backfill data from our start date `2020-01-01`. Another prompt will appear asking for a backfill end date; hit `Enter` to backfill until now. Finally, enter `y` and press `Enter` to apply the plan and execute the backfill:

```bash linenums="1"
Enter the backfill start date (eg. '1 year', '2020-01-01') or blank to backfill from the beginning of history:
Enter the backfill end date (eg. '1 month ago', '2020-01-01') or blank to backfill up until now: 
Apply - Backfill Tables [y/n]: y
sqlmesh_example__dev.incremental_model ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00

All model batches have been executed successfully

Virtually Updating 'dev' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully
```

SQLMesh applies the change to `sqlmesh_example.incremental_model` and backfills the model. SQLMesh did not need to backfill `sqlmesh_example.full_model` since the change was `non-breaking`.

### 4.1 Validate updates in dev
You can now view this change by querying data from `incremental_model` with `sqlmesh fetchdf "select * from sqlmesh_example__dev.incremental_model"`. 

Note that the environment name `__dev` is appended to the schema namespace `sqlmesh_example` in the query: `select * from sqlmesh_example__dev.incremental_model`.

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

### 4.2 Apply updates to prod
Now that we've tested the changes in dev, it's time to move them to prod. Run `sqlmesh plan` to plan and apply your changes to the prod environment. 

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
 FROM (VALUES
   (1, 1, '2020-01-01'),
Directly Modified: sqlmesh_example.incremental_model (Non-breaking)
└── Indirectly Modified Children:
    └── sqlmesh_example.full_model
Apply - Virtual Update [y/n]: y
Virtually Updating 'prod' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully


Virtual Update executed successfully
```

Note that a backfill was not necessary and only a Virtual Update occurred.

### 4.3. Validate updates in prod
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

## 5. Next steps

Congratulations, you've now conquered the basics of using SQLMesh!

* [Learn more about SQLMesh concepts](concepts/overview.md)
* [Join our Slack community](https://tobikodata.com/slack)
