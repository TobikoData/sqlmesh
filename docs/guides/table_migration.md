# Table migration

SQLMesh projects can read directly from tables not managed by SQLMesh, but in some cases it may be desirable to migrate an existing table into a SQLMesh project. This guide describes two methods for migrating existing tables into a SQLMesh project.

We assume that downstream consumers will continue to select from an object with the original table name, so the migration task is to convert an existing table into an identically named SQLMesh model. We also assume that the table is incremental, as there would be no need for migration if the table could be rebuilt on every run.

## Do you need to migrate?

SQLMesh does not assume it manages all data sources: models can read from any data source accessible by the SQL engine, treating them as as [external models](../concepts/models/model_kinds.md#external) that include column-level lineage or as generic sources. This approach is preferred to migrating existing tables into a SQLMesh project.

You should only migrate tables if the source table you are working with is either too large to be rebuilt or cannot be rebuilt because the necessary historical data is unavailable.

We describe two migration methods below. The stage and union migration method is preferred and should be used if it is feasible to store both the original table and all its rows copied into another table. If the table is too large to duplicate, the snapshot replacement method can be used.

## Stage and union

The stage and union method works by treating historical and new data as separate sources. It requires creating a staging model to ingest new records and a primary model that unions those records with the existing table's historical records.

For example, consider an existing table named `my_schema.existing_table`. Migrating this table with the stage and union method consists of five steps:

1. Ensure `my_schema.existing_table` is up to date (has ingested all available source data)
2. Rename `my_schema.existing_table` to any other name, such as `my_schema.existing_table_historical`
3. Create a new staging [`INCREMENTAL_BY_TIME_RANGE` model](../concepts/models/model_kinds.md#incremental_by_time_range) named `my_schema.existing_table_staging` (see below for code).
4. Create a new primary [`INCREMENTAL_BY_TIME_RANGE` model](../concepts/models/model_kinds.md#incremental_by_time_range) named `my_schema.existing_table` (see below for code).
5. Run `sqlmesh plan` to create and backfill the models

The staging model would contain code similar to:

``` sql linenums="1"
MODEL(
    name my_schema.existing_table_staging,
    kind INCREMENTAL_BY_TIME_RANGE(
        time_column table_time_column
    )
)

SELECT
    col1,
    col2,
    col3
FROM
    [your model's ongoing data source]
WHERE
    table_time_column BETWEEN @start_ds and @end_ds;
```

The primary model would contain code similar to:

``` sql linenums="1"
MODEL(
    name my_schema.existing_table,
    kind INCREMENTAL_BY_TIME_RANGE(
        time_column table_time_column
    )
)

SELECT
    col1,
    col2,
    col3
FROM
    my_schema.existing_table_staging
WHERE
    table_time_column BETWEEN @start_ds and @end_ds
UNION
SELECT
    col1,
    col2,
    col3
FROM
    my_schema.existing_table_historical
WHERE
    table_time_column BETWEEN @start_ds and @end_ds;
```

When `sqlmesh plan` is first run on these models, the entire historical table will be read into the primary model. Future executions with `sqlmesh run` will return no rows because the historical table will not contain new data after the model is first created.

If the model is modified, `sqlmesh plan` will again read the entire historical table into the model so it can properly apply the updated model code.


## Snapshot replacement

The snapshot replacement method works by renaming an existing table to a name that SQLMesh recognizes as part of an existing SQLMesh model.

We begin this section by briefly describing how SQLMesh's virtual data environments work. Conceptually, SQLMesh divides the database into a "physical layer" where data is stored and a "virtual layer" where data is accessed by end users. The physical layer stores materialized objects like tables, and the virtual layer contains views that point to the physical layer objects.

Each time a SQLMesh `plan` adds or modifies a model, SQLMesh creates a physical layer "snapshot" object to which the virtual layer view points. The snapshot replacement method simply renames the existing table to migrate to the name of the appropriate snapshot table.

For example, consider an existing table named `my_schema.existing_table`. Migrating this table with the snapshot replacement method involves seven steps:

1. Ensure `my_schema.existing_table` is up to date (has ingested all available source data)
2. Rename `my_schema.existing_table` to any other name, such as `my_schema.existing_table_temp`.
3. Initialize an empty [`INCREMENTAL_BY_TIME_RANGE` model](../concepts/models/model_kinds.md#incremental_by_time_range) named `my_schema.existing_table`:
- Make the model [forward only](./incremental_time.md#forward-only-models) by setting the `MODEL` DDL key `forward_only` to `true`
- Specify a query that returns no rows, such as `SELECT 1 as a WHERE 1=2`
- Run `sqlmesh plan` to create the model in the SQLMesh project
4. Determine the name of the model's snapshot physical table by running `sqlmesh table_name my_schema.existing_table`. For example, it might return `sqlmesh__my_schema.existing_table_123456`.
5. Rename the original table `my_schema.existing_table_temp` to `sqlmesh__my_schema.existing_table_123456`
6. Change the model query to your actual incremental by time query
7. Run `sqlmesh plan`

During model initialization in step 3 above, the model would have code similar to:

``` sql linenums="1" hl_lines="6"
MODEL(
    name my_schema.existing_table,
    kind INCREMENTAL_BY_TIME_RANGE(
        time_column table_time_column
    ),
    forward_only true
)

SELECT
    1 as a
WHERE
    1=2;
```

Note that we specify the model as forward only on line 6.

After `sqlmesh plan` initialized the model and the model query has been updated in step 6 above, the model would have code similar to:

``` sql linenums="1" hl_lines="6"
MODEL(
    name my_schema.existing_table,
    kind INCREMENTAL_BY_TIME_RANGE(
        time_column table_time_column
    ),
    forward_only true
)

SELECT
    col1,
    col2,
    col3
FROM
    [your model's ongoing data source]
WHERE
    table_time_column BETWEEN @start_ds and @end_ds;
```

The model remains forward only in the updated version - it is critical that the model stays forward only so the table is not rebuilt when `sqlmesh plan` is run.
