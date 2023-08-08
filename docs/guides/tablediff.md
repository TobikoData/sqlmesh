# Table Diff Guide

SQLMesh's table diff tool allows you to compare the schema and data of two data objects. It supports comparison of a SQLMesh model across two environments or direct comparison of database tables or views.

It provides a method of validating models that can be used along with [evaluating a model](./models.md#evaluating-a-model) and [testing a model with unit tests](./testing.md#testing-changes-to-models).

**Note:** Table diff requires the two objects to already exist in your project's underlying database or engine. If comparing models, this means you should have already planned and applied your changes to an environment.

## Table diff comparisons

Table diff executes two types of comparison on the source and target objects: a schema diff and a row diff.

The schema diff identifies whether fields have been added, removed, or changed data types in the target object relative to the source object.

The row diff identifies changes in data values across columns with the same name and data type in both tables. It does this by performing an `OUTER JOIN` of the two tables then, for each column with the same name and data type, comparing data values from one table to those from the other.

The table diff tool can be called in two ways: comparison of a SQLMesh model across two project environments or direct comparison of tables/views. It executes the comparison using the database or engine specified in the SQLMesh [project configuration](../reference/configuration.md).

## Diffing models across environments

Compare a SQLMesh model across environments with the SQLMesh CLI interface by using the command `sqlmesh table_diff [source environment]:[target environment] [model name]`.

For example, we could add a `WHERE` clause to the [SQLMesh quickstart](../quick_start.md) model `sqlmesh_example.full_model`:

```sql linenums="1"
MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  grain item_id,
  audits [assert_positive_order_ids],
);

SELECT
  item_id,
  count(distinct id) AS num_orders,
FROM
    sqlmesh_example.incremental_model
WHERE
  item_id < 3 -- Added `WHERE` clause
GROUP BY item_id
```

After running `sqlmesh plan dev` and applying the plan, the updated model will be present in the `dev` environment but not in `prod`.

Compare the two versions of the model with the table diff tool by running `sqlmesh table_diff prod:dev sqlmesh_example.full_model`.

The first argument `prod:dev` specifies that `prod` is the baseline environment to which we will compare `dev`. The second argument `sqlmesh_example.full_model` is the name of the model to compare across the `prod` and `dev` environments.

Because the `grain` is set to `item_id` in the `MODEL` statement, SQLMesh knows how to perform the join between the two models. If `grain` were not set, the command would need to include the `-o item_id` option to specify that the tables should be joined on column `item_id`. If more than one column should be used for the join, specify `-o` once for each join column.

Table diff returns this output:

```bash linenums="1"
$ sqlmesh table_diff prod:dev sqlmesh_example.full_model

Schema Diff Between 'PROD' and 'DEV' environments for model 'sqlmesh_example.full_model':
└── Schemas match


Outer Join Row Counts:
├──  JOINED: 2 rows
├──  PROD ONLY: 1 rows
└──  DEV ONLY: 0 rows

JOINED ROWS comparison stats:
            pct_match
num_orders      100.0
```

The "Schema Diff" section shows that the `PROD` and `DEV` schemas match.

The "Outer Join Row Counts" section shows that 2 rows were successfully joined and 1 row is only present in the `PROD` model.

The `JOINED ROWS comparison stats` section shows that the `num_orders` column values had a 100% match for the two joined rows. All non-join columns with the same data type in both tables are included in the comparison stats.

If we include the `--show-sample` option in the command, the output also includes rows from the different join components.

```bash linenums="1"
$ sqlmesh table_diff prod:dev sqlmesh_example.full_model --show-sample

Schema Diff Between 'PROD' and 'DEV' environments for model 'sqlmesh_example.full_model':
└── Schemas match


Outer Join Row Counts:
├──  JOINED: 2 rows
├──  PROD ONLY: 1 rows
└──  DEV ONLY: 0 rows

JOINED ROWS comparison stats:
            pct_match
num_orders      100.0


JOINED ROWS data differences:
  All joined rows match!

PROD ONLY sample rows:
 s__item_id  s__num_orders
          3              1
```

The "JOINED ROWS data differences" section does not display any rows because all columns matched between the two tables.

The "PROD ONLY sample rows" section shows the one row that is present in `PROD` but not in `DEV`. The column names begin with `s__` to indicate that they were specified as the "source" (as compared to "target") in the table diff comparison command.

## Diffing tables or views

Compare tables or views with the SQLMesh CLI interface by using the command `sqlmesh table_diff [source table]:[target table]`.

The source and target tables should be fully qualified with catalog or schema names such that a database query of the form `SELECT ... FROM [source table]` would execute correctly.

Recall that SQLMesh models are accessible via views in the database or engine. In the `prod` environment, the view has the same name as the model. For example, in the quickstart example project the `prod` seed model is represented by the view `sqlmesh_example.seed_model`.

In some situations, it might be appropriate to diff two different models. While this is not such a situation, as a demonstration we can compare the seed and full models with the model change made above.

First, run and apply `sqlmesh plan` to deploy the model change to `prod`. Diffing the seed and full models generates the following:

```bash linenums="1"
$ sqlmesh table_diff sqlmesh_example.seed_model:sqlmesh_example.full_model -o item_id --show-sample

Schema Diff Between 'SQLMESH_EXAMPLE.SEED_MODEL' and 'SQLMESH_EXAMPLE.FULL_MODEL':
├── Added Columns:
│   └── num_orders (BIGINT)
└── Removed Columns:
    ├── id (INT)
    └── ds (DATE)


Outer Join Row Counts:
├──  JOINED: 6 rows
├──  SQLMESH_EXAMPLE.SEED_MODEL ONLY: 1 rows
└──  SQLMESH_EXAMPLE.FULL_MODEL ONLY: 0 rows

JOINED ROWS comparison stats:
  No columns with same name and data type in both tables

JOINED ROWS data differences:
  All joined rows match

SQLMESH_EXAMPLE.SEED_MODEL ONLY sample rows:
 s__item_id  s__id      s__ds
          3      3 2020-01-03
```
