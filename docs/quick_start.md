# Overview
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

After SQLMesh creates a plan, it summarizes the breaking and non-breaking changes so you can understand what will happen if you apply the plan. It will prompt you to "backfill" data to apply the plan - in this context, backfill is a generic term for updating or adding to a table's data (including an initial load or full refresh).

### Model kinds

A plan's actions are determined by the [kinds](./concepts/models/model_kinds.md) of models the project uses. This example project uses three model kinds:

1. [`SEED` models](./concepts/models/model_kinds.md#seed) read data from CSV files stored in the SQLMesh project directory.
2. [`FULL` models](./concepts/models/model_kinds.md#full) fully refresh (rewrite) the data associated with the model every time the model is run.
3. [`INCREMENTAL_BY_TIME_RANGE` models](./concepts/models/model_kinds.md#incremental_by_time_range) use a date/time data column to track which time intervals are affected by a plan and process only the affected intervals when a model is run.

### Project directories and files

SQLMesh uses a scaffold generator to initiate a new project. The generator will create multiple sub-directories and files for organizing your SQLMesh project code. 

See the [CLI](./quickstart/cli.md), [Notebook](./quickstart/notebook.md), or [UI](./quickstart/ui.md) quickstart guides for details on how to initiate a SQLMesh project with the scaffold generator.

The scaffold generator will create the following configuration file and directories:

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

It will also create the files needed for this quickstart example:

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

### Project configuration

SQLMesh project-level configuration parameters are specified in the `config.yaml` file in the project directory.

This example project uses the embedded DuckDB SQL engine, so its configuration specifies `duckdb` as the local gateway's connection and the `local` gateway as the default. 

The command to run the scaffold generator **requires** a default SQL dialect for your models, which it places in the config `model_defaults` `dialect` key. In this example, we specified the `duckdb` SQL dialect as the default:

```yaml linenums="1"
gateways:
    local:
        connection:
            type: duckdb
            database: ./db.db

default_gateway: local

model_defaults:
    dialect: duckdb
```

Learn more about SQLMesh project configuration [here](./reference/configuration.md).

### Project data

The data used in this example project is contained in the `seed_data.csv` file in the `/seeds` project directory. The data reflects sales of 3 items over 7 days in January 2020.

The file contains three columns, `id`, `item_id`, and `ds`, which correspond to each row's unique ID, the sold item's ID number, and the date the item was sold, respectively.

This is the complete dataset:

| id | item_id | ds         |
| -- | ------- | ---------- |
| 1  | 2       | 2020-01-01 |
| 2  | 1       | 2020-01-01 |
| 3  | 3       | 2020-01-03 |
| 4  | 1       | 2020-01-04 |
| 5  | 1       | 2020-01-05 |
| 6  | 1       | 2020-01-06 |
| 7  | 1       | 2020-01-07 |

### Project models

We now briefly review each model in the project. 

The first model is a `SEED` model that imports `seed_data.csv`. This model consists of only a `MODEL` statement because `SEED` models do not query a database. 

In addition to specifying the model name and CSV path relative to the model file, it includes the column names and data types of the columns in the CSV. It also sets the `grain` of the model to the columns that collectively form the model's unique identifier, `id` and `ds`.

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
    ),
    grain [id, ds]
);
```

The second model is an `INCREMENTAL_BY_TIME_RANGE` model that includes both a `MODEL` statement and a SQL query selecting from the first seed model. 

The `MODEL` statement's `kind` property includes the required specification of the data column containing each record's timestamp. It also includes the optional `start` property specifying the earliest date/time for which the model should process data and the `cron` property specifying that the model should run daily. It sets the model's grain to columns `id` and `ds`.

The SQL query includes a `WHERE` clause that SQLMesh uses to filter the data to a specific date/time interval when loading data incrementally:

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
    item_id,
    ds,
FROM
    sqlmesh_example.seed_model
WHERE
    ds between @start_ds and @end_ds
```

The final model in the project is a `FULL` model. In addition to properties used in the other models, its `MODEL` statement includes the [`audits`](./concepts/audits.md) property. The project includes a custom `assert_positive_order_ids` audit in the project `audits` directory; it verifies that all `item_id` values are positive numbers. It will be run every time the model is executed.

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
GROUP BY item_id
```

## Project guides

Choose a SQLMesh API to work through the example project:

- [Command-line interface](./quickstart/cli.md)
- [Jupyter notebook](./quickstart/notebook.md)
- [Web user interface](./quickstart/ui.md)
