# Quick start
In this quick start guide, you'll get up and running with SQLMesh's scaffold generator. This example project will run locally on your computer using [DuckDB](https://duckdb.org/) as an embedded SQL engine.

Before beginning, ensure that you meet all the [prerequisites](prerequisites.md) for using SQLMesh.

## 1. Create a SQLMesh project
Create a project directory and navigate to it, as in the following example:

```bash
mkdir sqlmesh-example
```
```bash
cd sqlmesh-example
```

When using a virtual environment, you must ensure it's activated first by running the `source .env/bin/activate` command from the folder used during [installation](installation.md).

Create a SQLMesh scaffold by using the following command:

```bash
sqlmesh init
```

This will create the directories and files that you can use to organize your SQLMesh project code:

- config.yaml
    - The file for project configuration. Refer to [configuration](reference/configuration.md).
- ./models
    - SQL and Python models. Refer to [models](concepts/models/overview.md).
- ./audits
    - Shared audit files. Refer to [auditing](concepts/audits.md).
- ./tests
    - Unit test files. Refer to [testing](concepts/tests.md).
- ./macros
    - Macro files. Refer to [macros](concepts/macros.md).

## 2. Plan and apply environments
### 2.1 Create a prod environment
This example project structure is a two-model pipeline, where `sqlmesh_example.example_full_model` depends on `sqlmesh_example.example_incremental_model`.

To materialize this pipeline into DuckDB, run `sqlmesh plan` to get started with the plan/apply flow. The prompt will ask you what date to backfill; you can leave those blank for now (hit `Enter`) to backfill all of history. Finally, it will ask you whether or not you want backfill the plan. Enter `y`:

```bash
(.env) [user@computer sqlmesh-example]$ sqlmesh plan
======================================================================
Successfully Ran 1 tests against duckdb
----------------------------------------------------------------------
New environment `prod` will be created from `prod`
Summary of differences against `prod`:
└── Added Models:
    ├── sqlmesh_example.example_incremental_model
    └── sqlmesh_example.example_full_model
Models needing backfill (missing dates):
├── sqlmesh_example.example_incremental_model: (2020-01-01, 2023-03-22)
└── sqlmesh_example.example_full_model: (2023-03-22, 2023-03-22)
Apply - Backfill Tables [y/n]: y
sqlmesh_example.example_incremental_model ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00
       sqlmesh_example.example_full_model ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00

All model batches have been executed successfully

Virtually Updating 'prod' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully
```

You've now created a new production environment with all of history backfilled.

### 2.2 Create a dev environment
Now that you've created a production environment, it's time to create a development environment so that you can make changes without affecting production. Run `sqlmesh plan dev` to create a development environment called `dev`.

Although the summary of changes is similar, by showing that you've added two new models to this environment, the prompt notes that no backfills are needed and you're only required to perform a Virtual Update. This is because SQLMesh is able to safely reuse the tables you've already backfilled. Enter `y` to perform the Virtual Update:

```bash
(.env) [user@computer sqlmesh-example]$ sqlmesh plan dev
======================================================================
Successfully Ran 1 tests against duckdb
----------------------------------------------------------------------
New environment `dev` will be created from `prod`
Apply - Virtual Update [y/n]: y
Virtually Updating 'dev' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully


Virtual Update executed successfully
```

## 3. Make your first update
### 3.1 Edit the configuration
Let's add a new column. Open the `models/example_incremental_model.sql` file and add `#!sql 'z' AS new_column` under `item_id` as follows:

```bash
diff --git a/models/example_incremental_model.sql b/models/example_incremental_model.sql
index e1407e6..8154da2 100644
--- a/models/example_incremental_model.sql
+++ b/models/example_incremental_model.sql
@@ -10,6 +10,7 @@ MODEL (
 SELECT
     id,
     item_id,
+    'z' AS new_column,
     ds,
 FROM
     (VALUES
```

## 4. Plan and apply updates
Once this change is made, we can preview it using the `sqlmesh plan` command to understand the impact it had.

Run `sqlmesh plan dev` and hit `Enter` to leave the backfill start and end dates empty:

```bash
(.env) [user@computer sqlmesh-example]$ sqlmesh plan dev
======================================================================
Successfully Ran 1 tests against duckdb
----------------------------------------------------------------------
Summary of differences against `dev`:
├── Directly Modified:
│   └── sqlmesh_example.example_incremental_model
└── Indirectly Modified:
    └── sqlmesh_example.example_full_model
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
Directly Modified: sqlmesh_example.example_incremental_model (Non-breaking)
└── Indirectly Modified Children:
    └── sqlmesh_example.example_full_model
Models needing backfill (missing dates):
└── sqlmesh_example.example_incremental_model: (2020-01-01, 2023-03-22)
Enter the backfill start date (eg. '1 year', '2020-01-01') or blank for the beginning of history:
Enter the backfill end date (eg. '1 month ago', '2020-01-01') or blank to backfill up until now:
Apply - Backfill Tables [y/n]: y
sqlmesh_example.example_incremental_model ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00

All model batches have been executed successfully

Virtually Updating 'dev' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully
```

Notice that SQLMesh has detected that you've added `new_column`. It also shows you that the downstream model `sqlmesh_example.example_full_model` was indirectly modified. SQLMesh semantically understood that your change was additive (added an unused column) and so it was automatically classified as a non-breaking change.

SQLMesh now applies the change to `sqlmesh_example.example_incremental_model` and backfills the model. SQLMesh did not need to backfill `sqlmesh_example.example_full_model`, since it was `non-breaking`.

### 4.1 Validate updates in dev
You can now view this change by running `sqlmesh fetchdf "select * from sqlmesh_example__dev.example_incremental_model"`:

```bash
(.env) [user@computer sqlmesh-example]$ sqlmesh fetchdf "select * from sqlmesh_example__dev.example_incremental_model"

   id  item_id  new_column          ds
0   1        1           1  2020-01-01
1   1        2           1  2020-01-01
2   2        1           1  2020-01-01
3   3        3           1  2020-01-03
4   4        1           1  2020-01-04
5   5        1           1  2020-01-05
6   6        1           1  2020-01-06
7   7        1           1  2020-01-07
```

You can see that `new_column` was added to your dataset. The production table was not modified; you can validate this by querying the table using `sqlmesh fetchdf "select * from sqlmesh_example.example_incremental_model"`:

```bash
(.env) [user@computer sqlmesh-example]$ sqlmesh fetchdf "select * from sqlmesh_example.example_incremental_model"

   id  item_id          ds
0   1        1  2020-01-01
1   1        2  2020-01-01
2   2        1  2020-01-01
3   3        3  2020-01-03
4   4        1  2020-01-04
5   5        1  2020-01-05
6   6        1  2020-01-06
7   7        1  2020-01-07
```

Notice that the production table does not have `new_column`.

### 4.2 Apply updates to prod
Now that you've tested your changes in dev, it's time to move your changes to prod. Run `sqlmesh plan` to plan and apply your changes to the prod environment:

```bash
(.env) [toby@muc sqlmesh-example]$ sqlmesh plan
======================================================================
Successfully Ran 1 tests against duckdb
----------------------------------------------------------------------
Summary of differences against `prod`:
├── Directly Modified:
│   └── sqlmesh_example.example_incremental_model
└── Indirectly Modified:
    └── sqlmesh_example.example_full_model
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
Directly Modified: sqlmesh_example.example_incremental_model (Non-breaking)
└── Indirectly Modified Children:
    └── sqlmesh_example.example_full_model
Apply - Virtual Update [y/n]: y
Virtually Updating 'prod' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully


Virtual Update executed successfully
```

Notice that a backfill was not necessary and only a Virtual Update occurred.

### 4.3. Validate updates in prod
Double-check that the data did indeed update in prod by running `sqlmesh fetchdf "select * from sqlmesh_example.example_incremental_model"`:

```bash
(.env) [user@computer sqlmesh-example]$ sqlmesh fetchdf "select * from sqlmesh_example.example_incremental_model"

   id  item_id  new_column          ds
0   1        1           1  2020-01-01
1   1        2           1  2020-01-01
2   2        1           1  2020-01-01
3   3        3           1  2020-01-03
4   4        1           1  2020-01-04
5   5        1           1  2020-01-05
6   6        1           1  2020-01-06
7   7        1           1  2020-01-07
```

## 5. Next steps

Congratulations, you've now conquered the basics of using SQLMesh!

* [Learn more about SQLMesh concepts](concepts/overview.md)
* [Join our Slack community](https://join.slack.com/t/tobiko-data/shared_invite/zt-1ma66d79v-a4dbf4DUpLAQJ8ptQrJygg)
