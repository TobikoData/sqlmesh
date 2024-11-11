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
    time_column event_date
  ),
  start '2020-01-01',
  cron '@daily',
  grain (id, event_date)
);

SELECT
  id,
  CASE WHEN item_id = 3 THEN 4 ELSE item_id END as item_id, -- Change item_id 3 to 4
  event_date,
FROM
  sqlmesh_example.seed_model
WHERE
  event_date between @start_ds and @end_ds
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
├──  FULL MATCH: 6 rows (92.31%)
└──  PROD ONLY: 1 rows

COMMON ROWS column comparison stats:
         pct_match
item_id      100.0


COMMON ROWS sample data differences:
  All joined rows match

PROD ONLY sample rows:
 id event_date  item_id
  1 2020-01-01        2
```

The `COMMON ROWS sample data differences` section displays the row whose `item_id` value changed. The `PROD__item_id` column shows that `item_id` is 3 in the `PROD` table, and the `DEV__item_id` column shows that `item_id` is 4.0 in the `DEV` table.

The `PROD ONLY sample rows` section shows the one row that is present in `PROD` but not in `DEV`.

If we add the `--skip-grain-check` option, the grain is not validated. By default without this flag, a warning is displayed to the user if rows contain null or duplicate grains.

```bash linenums="1"
$ sqlmesh table_diff prod:dev2 sqlmesh_example.incremental_model

Grain should have unique and not-null audits for accurate results.

```

Under the hood, SQLMesh stores temporary data in the database to perform the comparison.
The default schema for these temporary tables is `sqlmesh_temp` but can be changed with the `--temp-schema` option.
The schema can be specified as a `CATALOG.SCHEMA` or `SCHEMA`.

## Diffing tables or views

Compare specific tables or views with the SQLMesh CLI interface by using the command `sqlmesh table_diff [source table]:[target table]`.

The source and target tables should be fully qualified with catalog or schema names such that a SQL query of the form `SELECT ... FROM [source table]` would execute correctly.

Recall that SQLMesh models are accessible via views in the database. In the `prod` environment, the view has the same name as the model. For example, in the quickstart example project the `prod` incremental model is represented by the view `sqlmesh_example.incremental_model`. In the `dev` environment, `__dev` is appended to the schema name so the incremental model is represented by the view `sqlmesh_example__dev.incremental_model`.

We can replicate the comparison in the previous section by comparing the model views directly. Because we are passing the view names directly, the command needs to manually specify that the join should be on the `id` and `ds` columns with the `-o id -o ds` flags.

```bash linenums="1"
$ sqlmesh table_diff sqlmesh_example.incremental_model:sqlmesh_example__dev.incremental_model -o id -o event_date --show-sample

Schema Diff Between 'SQLMESH_EXAMPLE.INCREMENTAL_MODEL' and 'SQLMESH_EXAMPLE__DEV.INCREMENTAL_MODEL':
└── Schemas match


Row Counts:
├──  FULL MATCH: 6 rows (92.31%)
└──  SQLMESH_EXAMPLE.INCREMENTAL_MODEL ONLY: 1 rows

COMMON ROWS column comparison stats:
         pct_match
item_id      100.0


COMMON ROWS sample data differences:
  All joined rows match

SQLMESH_EXAMPLE.INCREMENTAL_MODEL ONLY sample rows:
 id event_date  item_id
  1 2020-01-01        2
```

The output matches, with the exception of the column labels in the `COMMON ROWS sample data differences`. The underlying table for each column is indicated by `s__` for "source" table (first table in the command's colon operator `:`) and `t__` for "target" table (second table in the command's colon operator `:`).

## Diffing tables or views across gateways

!!! info "Tobiko Cloud Feature"

    Cross-database table diffing is available in [Tobiko Cloud](./observer.md#installation).

SQLMesh executes a project's models with a single database system, specified as a [gateway](../guides/connections.md#overview) in the project configuration.

The within-database table diff tool described above compares tables or environments within such a system. Sometimes, however, you might want to compare tables that reside in two different data systems.

For example, you might migrate your data transformations from an on-premises SQL engine to a cloud SQL engine while setting up your SQLMesh project. To demonstrate equivalence between the systems you could run the transformations in both and compare the new tables to the old tables.

The [within-database table diff](#diffing-models-across-environments) tool cannot make those comparisons, for two reasons:

1. It must join the two tables being diffed, but with two systems no single database engine can access both tables.
2. It assumes that data values can be compared across tables without modification. If the systems use different SQL engines, however, the diff must account for differences in the engines' data types (e.g., whether timestamps should include time zone information).

SQLMesh's cross-database table diff tool is built for just this scenario. Its comparison algorithm efficiently diffs tables without moving them from one system to the other and automatically addresses differences in data types.

### Configuration and syntax

To diff tables across systems, first configure [Gateways](../reference/configuration#Gateways) for each database system in your SQLMesh configuration file.

This example configures `bigquery` and `snowflake` gateways:

```yaml linenums="1"
gateways:
  bigquery:
    connection:
      type: bigquery
      [other connection parameters]

  snowflake:
    connection:
      type: snowflake
      [other connection parameters]
```

Then, specify each table's gateway in the `table_diff` command with this syntax: `[source_gateway]|[source table]:[target_gateway]|[target table]`.

For example, we could diff the `landing.table` table across `bigquery` and `snowflake` gateways like this:

```sh
$ sqlmesh table_diff 'bigquery|landing.table:snowflake|lake.table'
```

This syntax tells SQLMesh to use the cross-database diffing algorithm instead of the normal within-database diffing algorithm.

After adding gateways to the table names, use `table_diff` as described above - the same options apply for specifying the join keys, decimal precision, etc. See `sqlmesh table_diff --help` for a [full list of options](../reference/cli.md#table_diff).

!!! warning

    Cross-database diff works for data objects (tables / views).

    Diffing _models_ is not supported because we do not assume that both the source and target databases are managed by SQLMesh.

### Example output

A cross-database diff is broken up into two stages.

The first stage is a schema diff. This example shows that differences in column name case across the two tables are identified as schema differences:

```bash
$ sqlmesh table_diff 'bigquery|sqlmesh_example.full_model:snowflake|sqlmesh_example.full_model' --on item_id --show-sample

Schema Diff Between 'BIGQUERY|SQLMESH_EXAMPLE.FULL_MODEL' and 'SNOWFLAKE|SQLMESH_EXAMPLE.FULL_MODEL':
├── Added Columns:
│   ├── ITEM_ID (DECIMAL(38, 0))
│   └── NUM_ORDERS (DECIMAL(38, 0))
└── Removed Columns:
    ├── item_id (BIGINT)
    └── num_orders (BIGINT)
Schema has differences; continue comparing rows? [y/n]:
```

SQLMesh prompts you before comparing data values across table rows. The prompt provides an opportunity to discontinue the comparison if the schemas are vastly different (potentially indicating a mistake) or you need to exclude columns from the diff because you know they won't match.

The second stage of the diff is comparing data values across tables. Within each system, SQLMesh divides the data into chunks, evaluates each chunk, and compares the outputs across systems. If a difference is found, it performs a row-level diff on that chunk by reading a sample of mismatched rows from each system.

This example shows that 2 rows were present in each system but had different values, one row was in Bigquery only, and one row was in Snowflake only:

```bash
Dividing source dataset into 10 chunks (based on 10947709 total records)
Checking chunks against target dataset
Chunk 1 hash mismatch!
Starting row-level comparison for the range (1 -> 3)
Identifying individual record hashes that don't match
Comparing

Row Counts:
├──  PARTIAL MATCH: 2 rows (66.67%)
├──  BIGQUERY ONLY: 1 rows (16.67%)
└──  SNOWFLAKE ONLY: 1 rows (16.67%)

COMMON ROWS column comparison stats:
            pct_match
num_orders        0.0


COMMON ROWS sample data differences:
Column: num_orders
┏━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┓
┃ item_id ┃ BIGQUERY ┃ SNOWFLAKE ┃
┡━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━┩
│ 1       │ 5        │ 7         │
│ 2       │ 1        │ 2         │
└─────────┴──────────┴───────────┘

BIGQUERY ONLY sample rows:
item_id num_orders
      7          4


SNOWFLAKE ONLY sample rows:
item_id num_orders
      4          6
```

If there are no differences found between chunks, the source and target datasets can be considered equal:

```bash
Chunk 1 (1094771 rows) matches!
Chunk 2 (1094771 rows) matches!
...
Chunk 10 (1094770 rows) matches!

All 10947709 records match between 'bigquery|sqlmesh_example.full_model' and 'snowflake|TEST.SQLMESH_EXAMPLE.FULL_MODEL'
```

!!! info

    Don't forget to specify the `--show-sample` option if you'd like to see a sample of the actual mismatched data!

    Otherwise, only high level statistics for the mismatched rows will be printed.

### Supported engines

Cross-database diffing is supported on all execution engines that [SQLMesh supports](../integrations/overview.md#execution-engines).