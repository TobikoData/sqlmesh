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

For example, we could make two modifications to the [SQLMesh quickstart](../quick_start.md) model `sqlmesh_example.incremental_model`:

1. Change the row whose `item_id` is `3` to `4` with a `CASE WHEN` statement
2. Remove row whose `item_id` is `1` by adding a `WHERE` clause

```sql linenums="1"
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
    CASE WHEN item_id = 3 THEN 4 ELSE item_id END as item_id, -- Change item_id 3 to 4
    ds,
FROM
    sqlmesh_example.seed_model
WHERE
    ds between @start_ds and @end_ds
    AND id != 1 -- Remove row whose item_id is 1
```

After running `sqlmesh plan dev` and applying the plan, the updated model will be present in the `dev` environment but not in `prod`.

Compare the two versions of the model with the table diff tool by running `sqlmesh table_diff prod:dev sqlmesh_example.incremental_model`.

The first argument `prod:dev` specifies that `prod` is the source environment to which we will compare the target environment `dev`. The second argument `sqlmesh_example.incremental_model` is the name of the model to compare across the `prod` and `dev` environments.

Because the `grain` is set to `[id, ds]` in the `MODEL` statement, SQLMesh knows how to perform the join between the two models. If `grain` were not set, the command would need to include the `-o id -o ds` option to specify that the tables should be joined on column `id` and `ds`. Specify `-o` once for each join column.

Table diff returns this output:

```bash linenums="1"
$ sqlmesh table_diff prod:dev sqlmesh_example.incremental_model

Schema Diff Between 'PROD' and 'DEV' environments for model 'sqlmesh_example.incremental_model':
└── Schemas match


Row Counts:
├──  COMMON: 6 rows
├──  PROD ONLY: 1 rows
└──  DEV ONLY: 0 rows

COMMON ROWS column comparison stats:
         pct_match
item_id       83.3
```

The "Schema Diff" section shows that the `PROD` and `DEV` schemas match because no columns have been added, removed, or change data type.

The "Row Counts" section shows that 6 rows were successfully joined and the 1 row we removed is only present in the `PROD` model.

The `COMMON ROWS column comparison stats` section shows that the `item_id` column values had an 83.3% match for the six joined rows (5 of the 6 row values were unchanged by our `CASE WHEN` statement). All non-join columns with the same data type in both tables are included in the comparison stats.

If we include the `--show-sample` option in the command, the output also includes rows from the different join components.

```bash linenums="1"
$ sqlmesh table_diff prod:dev sqlmesh_example.incremental_model --show-sample

Schema Diff Between 'PROD' and 'DEV' environments for model 'sqlmesh_example.incremental_model':
└── Schemas match


Row Counts:
├──  COMMON: 6 rows
├──  PROD ONLY: 1 rows
└──  DEV ONLY: 0 rows

COMMON ROWS column comparison stats:
         pct_match
item_id       83.3


COMMON ROWS sample data differences:
 id         ds  PROD__item_id  DEV__item_id
  3 2020-01-03              3           4.0


PROD ONLY sample rows:
 id         ds  item_id
  1 2020-01-01        2
```

The `COMMON ROWS sample data differences` section displays the row whose `item_id` value changed. The `PROD__item_id` column shows that `item_id` is 3 in the `PROD` table, and the `DEV__item_id` column shows that `item_id` is 4.0 in the `DEV` table.

The `PROD ONLY sample rows` section shows the one row that is present in `PROD` but not in `DEV`.

## Diffing tables or views

Compare specific tables or views with the SQLMesh CLI interface by using the command `sqlmesh table_diff [source table]:[target table]`.

The source and target tables should be fully qualified with catalog or schema names such that a SQL query of the form `SELECT ... FROM [source table]` would execute correctly.

Recall that SQLMesh models are accessible via views in the database. In the `prod` environment, the view has the same name as the model. For example, in the quickstart example project the `prod` incremental model is represented by the view `sqlmesh_example.incremental_model`. In the `dev` environment, `__dev` is appended to the schema name so the incremental model is represented by the view `sqlmesh_example__dev.incremental_model`.

We can replicate the comparison in the previous section by comparing the model views directly. Because we are passing the view names directly, the command needs to manually specify that the join should be on the `id` and `ds` columns with the `-o id -o ds` flags.

```bash linenums="1"
$ sqlmesh table_diff sqlmesh_example.incremental_model:sqlmesh_example__dev.incremental_model -o id -o ds --show-sample

Schema Diff Between 'SQLMESH_EXAMPLE.INCREMENTAL_MODEL' and 'SQLMESH_EXAMPLE__DEV.INCREMENTAL_MODEL':
└── Schemas match


Row Counts:
├──  COMMON: 6 rows
├──  SQLMESH_EXAMPLE.INCREMENTAL_MODEL ONLY: 1 rows
└──  SQLMESH_EXAMPLE__DEV.INCREMENTAL_MODEL ONLY: 0 rows

COMMON ROWS column comparison stats:
         pct_match
item_id       83.3


COMMON ROWS sample data differences:
 id         ds  s__item_id  t__item_id
  3 2020-01-03           3         4.0


SQLMESH_EXAMPLE.INCREMENTAL_MODEL ONLY sample rows:
 id         ds  item_id
  1 2020-01-01        2
```

The output matches, with the exception of the column labels in the `COMMON ROWS sample data differences`. The underlying table for each column is indicated by `s__` for "source" table (first table in the command's colon operator `:`) and `t__` for "target" table (second table in the command's colon operator `:`).
