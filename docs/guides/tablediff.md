# Table Diff Guide

SQLMesh's table diff tool allows you to compare the schema and data of two data objects. It supports comparison of a SQLMesh model across two environments or direct comparison of database tables or views.

It provides a method of validating models that can be used along with [evaluating a model](./models.md#evaluating-a-model) and [testing a model with unit tests](./testing.md#testing-changes-to-models).

**Note:** Table diff requires the two objects to already exist in your project's underlying database or engine. If comparing models, this means you should have already planned and applied your changes to an environment.

## Table diff comparisons

Table diff executes two types of comparison on the source and target objects: a schema diff and a row diff.

The schema diff identifies whether fields have been added, removed, or changed data types in the target object relative to the source object. 

The row diff identifies changes in data values across columns with the same name and data type in both tables. It does this by performing a `FULL JOIN` of the two tables then, for each column with the same name and data type, comparing data values from one table to those from the other.

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

Row Count:
 PROD: 3 rows
 DEV: 2 rows
 Change: -33.3%

Sample Rows:
 s__item_id  s__num_orders  t__item_id  t__num_orders
          3              1         NaN            NaN
```

The "Schema Diff" section shows that the `PROD` and `DEV` schemas match. 

The "Row Count" section shows that the `DEV` model has one fewer row than the `PROD` model, corresponding to a decrease of 33.3%. 

The "Sample Rows" section shows the row that is present in `PROD` but not in `DEV`. The data are from the joined table created to execute the comparison. Columns from `PROD` begin with `s__` ("source"), and columns from `DEV` begin with `t__` ("target"). 

The columns from `DEV` show `NaN` ("not a number") values because the row did not exist in `DEV` and its values are all `NULL` in the joined table. The values are displayed from a [pandas](https://pandas.pydata.org/) dataframe, which converts `NULL` values from a database to `NaN` values in Python. 

## Diffing tables or views

Compare tables or views with the SQLMesh CLI interface by using the command `sqlmesh table_diff [source table]:[target table]`.

The source and target tables should be fully qualified with catalog or schema names such that a database query of the form `SELECT ... FROM [source table]` would execute correctly.

Recall that SQLMesh models are accessible via views in the database or engine. In the `prod` environment, the view has the same name as the model. For example, in the quickstart example project the `prod` seed model is represented by the view `sqlmesh_example.seed_model`.

In non-prod environments, the environment name is appended to the view's schema. For example, in the quickstart project the `dev` full model is represented by the view `sqlmesh_example__dev.full_model`.

In some situations, it might be appropriate to diff two different models. While this is not such a situation, as a demonstration we can compare the quickstart `dev` seed and full models like this:

```bash linenums="1"
$ sqlmesh table_diff sqlmesh_example__dev.seed_model:sqlmesh_example__dev.full_model -o item_id

Schema Diff Between 'SQLMESH_EXAMPLE__DEV.SEED_MODEL' and 'SQLMESH_EXAMPLE__DEV.FULL_MODEL':
├── Added Columns:
│   └── num_orders (BIGINT)
└── Removed Columns:
    ├── id (INT)
    └── ds (DATE)

Row Count:
 SQLMESH_EXAMPLE__DEV.SEED_MODEL: 7 rows
 SQLMESH_EXAMPLE__DEV.FULL_MODEL: 6 rows
 Change: -14.3%

Sample Rows:
 s__id  s__item_id      s__ds  t__item_id  t__num_orders
     3           3 2020-01-03         NaN            NaN
```

