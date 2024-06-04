# Table migration guide

SQLMesh projects can read directly from tables not managed by SQLMesh, but in some scenarios it may be useful to migrate an existing table into a SQLMesh project.

This guide describes two methods for migrating existing tables into a SQLMesh project.

## Do you need to migrate?

SQLMesh does not assume it manages all data sources: SQL models can read from any data source accessible by the SQL engine, treating them as [external models](../concepts/models/model_kinds.md#external) that include column-level lineage or as generic sources. This approach is preferred to migrating existing tables into a SQLMesh project.

You should only migrate a table if both of the following are true:

1. The table is ingesting from an upstream source that will continue generating new data
2. The table is either too large to be rebuilt or cannot be rebuilt because the necessary historical data is unavailable

If the table's upstream source will not generate more data, there is no ongoing activity for SQLMesh to manage. A SQLMesh model or any other downstream consumer can select directly from the table under its current name.

If the table's upstream source is generating new data, we assume that the table is already being loaded incrementally, as there is no need for migration if the table can be fully rebuilt.

We describe two migration methods below. The stage and union method is preferred and should be used if feasible.

## Migration methods

This section describes two methods for migrating tables into SQLMesh.

The method descriptions contain renaming steps that are only necessary if downstream consumers must select from the original table name (e.g., step 2 in the first example). If that is not the case, the original table can retain its name.

The table and model names in the examples below are arbitrary - you may name them whatever is appropriate for your project.

### Stage and union

The stage and union method works by treating new and historical data as separate sources.

It requires creating an incremental staging model to ingest new records and a `VIEW` model that unions those records with the existing table's static historical records.

#### Example

Consider an existing table named `my_schema.existing_table`. Migrating this table with the stage and union method consists of five steps:

1. Ensure `my_schema.existing_table` is up to date (has ingested all available source data)
2. Rename `my_schema.existing_table` to any other name, such as `my_schema.existing_table_historical`
    - Optionally, enable column-level lineage for the table by making it an [`EXTERNAL` model](../concepts/models/model_kinds.md#external) and adding it to the project's `external_models.yaml` file
3. Create a new incremental staging model named `my_schema.existing_table_staging` (see below for code)
4. Create a new [`VIEW` model](../concepts/models/model_kinds.md#view) named `my_schema.existing_table` (see below for code)
5. Run `sqlmesh plan` to create and backfill the models

The staging model would contain code similar to the following for an `INCREMENTAL_BY_TIME_RANGE` model. An `INCREMENTAL_BY_UNIQUE_KEY` model would have a different `kind` specification in the `MODEL` DDL and might not include the query's `WHERE` clause.

``` sql linenums="1"
MODEL(
  name my_schema.existing_table_staging,
  kind INCREMENTAL_BY_TIME_RANGE ( -- or INCREMENTAL_BY_UNIQUE_KEY
    time_column table_time_column
  )
);

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
  kind VIEW
)

SELECT
  col1,
  col2,
  col3
FROM
  my_schema.existing_table_staging -- New data
UNION
SELECT
  col1,
  col2,
  col3
FROM
  my_schema.existing_table_historical; -- Historical data
```

Changes to columns in the source data or staging model may require modifying the code selecting from the historical data so the two tables can be safely unioned.

### Snapshot replacement

The snapshot replacement method works by renaming an existing table to a name that SQLMesh recognizes as an existing SQLMesh model.

#### Background

This section briefly describes how SQLMesh's virtual data environments, forward-only models, and start times work. This information is not necessary for migrating tables but is necessary for understanding why each step in the migration process is required.

##### Virtual data environments

Conceptually, SQLMesh divides the database into a "physical layer" where data is stored and a "virtual layer" where data is accessed by end users. The physical layer stores materialized objects like tables, and the virtual layer contains views that point to the physical layer objects.

Each time a SQLMesh `plan` adds or modifies a model, SQLMesh creates a physical layer "snapshot" object to which the virtual layer view points. The snapshot replacement method simply renames the migrating table to the name of the appropriate snapshot table.

##### Forward-only models

Sometimes a model's data may be so large that it is not feasible to rebuild either its own or its downstream models' physical tables. In those situations a  "forward only" model can be used. The name reflects that the change is only applied "going forward" in time.

Historical data already in the migrated table should not be overwritten, so we specify that the new model is forward-only in step 3a below.

##### Start time

SQLMesh incremental by time models track the time periods whose data a model has loaded with the [interval approach](https://sqlmesh.readthedocs.io/en/stable/guides/incremental_time/#counting-time).

The interval approach requires specifying the earliest time interval SQLMesh should track - when time "starts" for the model. For migrated tables, SQLMesh should never load data for the time intervals the table ingested before migration, so interval tracking should start immediately after the time of the last ingested record.

In the example below, we set the model's start time in its `MODEL` DDL (step 3b) and pass it as an option to the `sqlmesh plan` command (step 3c). The same value must be used in both the `MODEL` DDL and the plan command. In this example, the existing table's data ingestion stopped on 2023-12-31, so the model and plan start date is the next day 2024-01-01.

#### Example

Consider an existing table named `my_schema.existing_table`. Migrating this table with the snapshot replacement method involves five steps:

1. Ensure `my_schema.existing_table` is up to date (has ingested all available source data)
2. Rename `my_schema.existing_table` to any other name, such as `my_schema.existing_table_temp`
3. Create and initialize an empty incremental model named `my_schema.existing_table`:

    a. Make the model [forward only](./incremental_time.md#forward-only-models) by setting the `MODEL` DDL `kind`'s `forward_only` key to `true`

    b. Specify the start of the first time interval SQLMesh should track in the `MODEL` DDL `start` key (example uses "2024-01-01")

    c. Create the model in the SQLMesh project without backfilling any data by running `sqlmesh plan [environment name] --skip-backfill --start 2024-01-01`, replacing "[environment name]" with an environment name other than `prod` and using the same start date from the `MODEL` DDL in step 3b.

4. Determine the name of the model's snapshot physical table by running `sqlmesh table_name my_schema.existing_table`. For example, it might return `sqlmesh__my_schema.existing_table_123456`.
5. Rename the original table `my_schema.existing_table_temp` to `sqlmesh__my_schema.existing_table_123456`

The model would have code similar to:

``` sql linenums="1" hl_lines="5 7-9"
MODEL(
  name my_schema.existing_table,
  kind INCREMENTAL_BY_TIME_RANGE( -- or INCREMENTAL_BY_UNIQUE_KEY
    time_column table_time_column,
    forward_only true -- Forward-only model
  ),
  -- Start of first time interval SQLMesh should track, immediately
  --  after the last data point the table ingested. Must match
  --  the value passed to the `sqlmesh plan --start` option.
  start "2024-01-01"
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
