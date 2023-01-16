# Quick start
In this quick start guide, you'll learn how to get up and running with SQLMesh's scaffold generator. This example project will run locally on your computer using [DuckDB](https://duckdb.org/) as an embedded SQL engine.

## Prerequisites

You'll need Python 3.7 or higher to use SQLMesh. You can check your python version by running the following command:
```
python3 --version
```

or

```
python --version
```

If `python --version` returns 2.x, replace all `python` commands with `python3`, and `pip` with `pip3`.

## Create a SQLMesh project
Create a project directory and navigate to it, as in the following example:

```
mkdir sqlmesh-example
cd sqlmesh-example
```

It is recommended, but not required, that you use a virtual environment:

```
python -m venv .env
source .env/bin/active
pip install sqlmesh
```

When using a virtual environment, you must ensure it's activated: you should see `(.env)` in your command line. If you don't, run `source .env/bin/activate` from your project directory to reactivate the environment.

Now, we can create a SQLMesh scaffold by using the following command:

```
sqlmesh init
```

This will create directories and files that you can use to organize your SQLMesh project code.

- config.py
    - The file for database configuration.
- ./models
    - The place for sql and python models.  # TODO add links
- ./audits
    - The place for shared audits.
- ./tests
    - The place for unit tests.
- ./macros
    - The place for macros.

## Plan and apply environments
### Create a prod environment
The starter project structure is a two-model pipeline, where example_full_model depends on example_incremental_model. To materialize this pipeline into DuckDB, run `sqlmesh plan` to get started with the plan/apply flow. The prompt will ask you what date to backfill; you can leave those blank for now to backfill all of history. Finally, it will ask you whether or not you want backfill the plan. Type 'y'.

```
(.env) [user@computer sqlmesh-example]$ sqlmesh plan
======================================================================
Successfully Ran 1 tests against duckdb
----------------------------------------------------------------------
Summary of differences against `prod`:
└── Added Models:
    ├── sqlmesh_example.example_incremental_model
    └── sqlmesh_example.example_full_model
Models needing backfill (missing dates):
├── sqlmesh_example.example_incremental_model: (2020-01-01, 2022-12-29)
└── sqlmesh_example.example_full_model: (2022-12-29, 2022-12-29)
Enter the backfill start date (eg. '1 year', '2020-01-01') or blank for the beginning of history:
Apply - Backfill Tables [y/n]: y

All model batches have been executed successfully

sqlmesh_example.example_incremental_model ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00
       sqlmesh_example.example_full_model ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00
```

You've now created a new production environment with all of history backfilled.

### Create a dev environment
Now that you've created a production environment, it's time to create a development environment so that you can make changes without affecting production. Run `sqlmesh plan dev` to create a development environment called 'dev'. Notice that although the summary of changes is similar, by showing that you've added two new models to this environment, the prompt notes that no backfills are needed and you only require a logical update. This is because SQLMesh is able to safely reuse the tables you've already backfilled. Type 'y' to perform the logical update.

```
(.env) [user@computer sqlmesh-example]$ sqlmesh plan dev
======================================================================
Successfully Ran 1 tests against duckdb
----------------------------------------------------------------------
Summary of differences against `dev`:
└── Added Models:
    ├── sqlmesh_example.example_incremental_model
    └── sqlmesh_example.example_full_model
Apply - Logical Update [y/n]: y

Logical Update executed successfully
```

## Make your first update
### Edit the configuration
Let's add a new column to example_incremental_model. Open models/example_incremental_model.sql and add `1 AS new_column` under item_id.

```
diff --git a/models/example_incremental_model.sql b/models/example_incremental_model.sql
index e1407e6..8154da2 100644
--- a/models/example_incremental_model.sql
+++ b/models/example_incremental_model.sql
@@ -10,6 +10,7 @@ MODEL (
 SELECT
     id,
     item_id,
+    1 AS new_column,
     ds,
 FROM
     (VALUES
```

### Plan and apply changes
Once this change is created, we can preview it with plan and understand the impact it had. Run `sqlmesh plan dev`.

```
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
+  1 AS new_column,
   ds
 FROM (VALUES
   (1, 1, '2020-01-01'),
Directly Modified: sqlmesh_example.example_incremental_model
└── Indirectly Modified Children:
    └── sqlmesh_example.example_full_model
[1] [Breaking] Backfill sqlmesh_example.example_incremental_model and indirectly modified children
[2] [Non-breaking] Backfill sqlmesh_example.example_incremental_model but not indirectly modified children: 2
Models needing backfill (missing dates):
└── sqlmesh_example.example_incremental_model: (2020-01-01, 2022-12-29)
Enter the backfill start date (eg. '1 year', '2020-01-01') or blank for the beginning of history:
Enter the backfill end date (eg. '1 month ago', '2020-01-01') or blank to backfill up until now: 2022-01-05
Apply - Backfill Tables [y/n]: y


All model batches have been executed successfully

sqlmesh_example.example_incremental_model ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00
```

As you can see, SQLMesh detected that you've added new_column. It also shows you that the downstream model 'sqlmesh_example.example_full_model' was indirectly modified. It asks you to classify the changes as `Breaking` or `Non-Breaking`. Because we've only added a new column, which shouldn't affect the full model, this change should be classified as `Non-Breaking`. Type '2' and apply the change.

SQLMesh now applies the change to 'sqlmesh_example.example_incremental_model' and backfilled it. SQLMesh did not need to backfill 'sqlmesh_example.example_full_model', because `Non-Breaking` change was selected.

### Validate the changes in dev
You can now view this change by running `sqlmesh fetchdf`

```
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

You can see that new_column was added to your dataset. The production table was not modified; you can validate this by querying the table:

```
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

Notice that the production table does not have new_column.

### Apply changes to prod
Now that you've tested your changes in dev, it's time to move this change to prod. Run `sqlmesh plan` to plan and apply your changes to the prod environment.

```
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
+  1 AS new_column,
   ds
 FROM (VALUES
   (1, 1, '2020-01-01'),
Directly Modified: sqlmesh_example.example_incremental_model (Non-breaking)
└── Indirectly Modified Children:
    └── sqlmesh_example.example_full_model
Models needing backfill (missing dates):
└── sqlmesh_example.example_incremental_model: (2022-01-06, 2022-12-29)
Enter the backfill start date (eg. '1 year', '2020-01-01') or blank for the beginning of history:
Enter the backfill end date (eg. '1 month ago', '2020-01-01') or blank to backfill up until now:
Apply - Backfill Tables [y/n]: y

All model batches have been executed successfully

sqlmesh_example.example_incremental_model ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00
```

### Validate the changes in prod
Finally, double-check that the data did indeed land in prod:

```
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
Congratulations, you've now conquered the basics of using SQLMesh!

## Next steps

* For more information about how SQLMesh works, refer to [concepts](concepts/overview.md).
* For API documentation, refer to [API](api/overview.md).
* For information about integrations with SQLMesh, refer to [integrations](integrations/overview.md).
* For resources such as how to get involved in our community, refer to [community](community.md).
