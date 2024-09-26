# dbt

SQLMesh has native support for running dbt projects with its dbt adapter.

## Getting started
### Reading a dbt project

Prepare an existing dbt project to be run by SQLMesh by executing the `sqlmesh init` command *within the dbt project root directory* and with the `dbt` template option:

```bash
$ sqlmesh init -t dbt
```

SQLMesh will use the data warehouse connection target in your dbt project `profiles.yml` file. The target can be changed at any time.

### Setting model backfill start dates

Models **require** a start date for backfilling data through use of the `start` configuration parameter. `start` can be defined individually for each model in its `config` block or globally in the `dbt_project.yml` file as follows:

```
> models:
>   +start: Jan 1 2000
```

### Configuration

SQLMesh determines a project's configuration settings from its dbt configuration files.

This section describes using runtime variables to create multiple configurations and how to disable SQLMesh's automatic model description and comment registration.

#### Runtime vars

dbt supports passing variable values at runtime with its [CLI `vars` option](https://docs.getdbt.com/docs/build/project-variables#defining-variables-on-the-command-line).

In SQLMesh, these variables are passed via configurations. When you initialize a dbt project with `sqlmesh init`, a file `config.py` is created in your project directory.

The file creates a SQLMesh `config` object pointing to the project directory:

```python
config = sqlmesh_config(Path(__file__).parent)
```

Specify runtime variables by adding a Python dictionary to the `sqlmesh_config()` `variables` argument.

For example, we could specify the runtime variable `is_marketing` and its value `no` as:

```python
config = sqlmesh_config(
    Path(__file__).parent,
    variables={"is_marketing": "no"}
    )
```

Some projects use combinations of runtime variables to control project behavior. Different combinations can be specified in different `sqlmesh_config` objects, with the relevant configuration passed to the SQLMesh CLI command.

For example, consider a project with a special configuration for the `marketing` department. We could create separate configurations to pass at runtime like this:

```python
config = sqlmesh_config(
    Path(__file__).parent,
    variables={"is_marketing": "no", "include_pii": "no"}
    )

marketing_config = sqlmesh_config(
    Path(__file__).parent,
    variables={"is_marketing": "yes", "include_pii": "yes"}
    )
```

By default, SQLMesh will use the configuration object named `config`. Use a different configuration by passing the object name to SQLMesh CLI commands with the `--config` option. For example, we could run a `plan` with the marketing configuration like this:

```python
sqlmesh --config marketing_config plan
```

Note that the `--config` option is specified between the word `sqlmesh` and the command being executed (e.g., `plan`, `run`).

#### Registering comments

SQLMesh automatically registers model descriptions and column comments with the target SQL engine, as described in the [Models Overview documentation](../concepts/models/overview#model-description-and-comments). Comment registration is on by default for all engines that support it.

dbt offers similar comment registration functionality via its [`persist_docs` model configuration parameter](https://docs.getdbt.com/reference/resource-configs/persist_docs), specified by model. SQLMesh comment registration is configured at the project level, so it does not use dbt's model-specific `persist_docs` configuration.

SQLMesh's project-level comment registration defaults are overridden with the `sqlmesh_config()` `register_comments` argument. For example, this configuration turns comment registration off:

```python
config = sqlmesh_config(
    Path(__file__).parent,
    register_comments=False,
    )
```

### Running SQLMesh

Run SQLMesh as with a SQLMesh project, generating and applying [plans](../concepts/overview.md#make-a-plan), running [tests](../concepts/overview.md#tests) or [audits](../concepts/overview.md#audits), and executing models with a [scheduler](../guides/scheduling.md) if desired.

You continue to use your dbt file and project format.

## Workflow differences between SQLMesh and dbt

Consider the following when using a dbt project:

* SQLMesh will detect and deploy new or modified seeds as part of running the `plan` command and applying changes - there is no separate seed command. Refer to [seed models](../concepts/models/seed_models.md) for more information.
* The `plan` command dynamically creates environments, so environments do not need to be hardcoded into your `profiles.yml` file as targets. To get the most out of SQLMesh, point your dbt profile target at the production target and let SQLMesh handle the rest for you.
* The term "test" has a different meaning in dbt than in SQLMesh:
    - dbt "tests" are [audits](../concepts/audits.md) in SQLMesh.
    - SQLMesh "tests" are [unit tests](../concepts/tests.md), which test query logic before applying a SQLMesh plan.
* dbt's' recommended incremental logic is not compatible with SQLMesh, so small tweaks to the models are required (don't worry - dbt can still use the models!).

## How to use SQLMesh incremental models with dbt projects

Incremental loading is a powerful technique when datasets are large and recomputing tables is expensive. SQLMesh offers first-class support for incremental models, and its approach differs from dbt's.

This section describes how to adapt dbt's incremental models to run on sqlmesh and maintain backwards compatibility with dbt.

### Incremental types

SQLMesh supports two approaches to implement [idempotent](../concepts/glossary.md#idempotency) incremental loads:

* Using merge (with the sqlmesh [`INCREMENTAL_BY_UNIQUE_KEY` model kind](../concepts/models/model_kinds.md#incremental_by_unique_key))
* Using insert-overwrite/delete+insert (with the sqlmesh [`INCREMENTAL_BY_TIME_RANGE` model kind](../concepts/models/model_kinds.md#incremental_by_time_range))

#### Incremental by unique key

To enable incremental_by_unique_key incrementality, the model configuration should contain:

* The `unique_key` key with the model's unique key field name or names as the value
* The `materialized` key with value `'incremental'`
* Either:
    * No `incremental_strategy` key or
    * The `incremental_strategy` key with value `'merge'`

#### Incremental by time range

To enable incremental_by_time_range incrementality, the model configuration should contain:

* The `time_column` key with the model's time column field name as the value (see [`time column`](../concepts/models/model_kinds.md#time-column) for details)
* The `materialized` key with value `'incremental'`
* Either:
    * The `incremental_strategy` key with value `'insert_overwrite'` or
    * The `incremental_strategy` key with value `'delete+insert'`
    * Note: in this context, these two strategies are synonyms. Regardless of which one is specified SQLMesh will use the [`best incremental strategy`](../concepts/models/model_kinds.md#materialization-strategy) for the target engine.

### Incremental logic

SQLMesh requires a new jinja block gated by `{% if sqlmesh_incremental is defined %}`. The new block should supersede the existing `{% if is_incremental() %}` block and contain the `WHERE` clause selecting the time interval.

For example, the SQL `WHERE` clause with the "ds" column goes in a new jinja block gated by `{% if sqlmesh_incremental is defined %}` as follows:

```bash
> {% if sqlmesh_incremental is defined %}
>   WHERE
>     ds BETWEEN '{{ start_ds }}' AND '{{ end_ds }}'
> {% elif is_incremental() %}
>   ; < your existing is_incremental block >
> {% endif %}
```

`{{ start_ds }}` and `{{ end_ds }}` are the jinja equivalents of SQLMesh's `@start_ds` and `@end_ds` predefined time macro variables. See all [predefined time variables](../concepts/macros/macro_variables.md) available in jinja.

### Incremental model configuration

SQLMesh provides configuration parameters that enable control over how incremental computations occur. These parameters are set in the model's `config` block.

The [`batch_size` parameter](../concepts/models/overview.md#batch_size) determines the maximum number of time intervals to run in a single job.

The [`lookback` parameter](../concepts/models/overview.md#lookback) is used to capture late arriving data. It sets the number of units of late arriving data the model should expect and must be a positive integer.

**Note:** By default, all incremental dbt models are configured to be [forward-only](../concepts/plans.md#forward-only-plans). However, you can change this behavior by setting the `forward_only: false` setting either in the configuration of an individual model or globally for all models in the `dbt_project.yaml` file. The [forward-only](../concepts/plans.md#forward-only-plans) mode aligns more closely with the typical operation of dbt and therefore better meets user's expectations.

#### on_schema_change

SQLMesh automatically detects destructive schema changes to [forward-only incremental models](../guides/incremental_time.md#forward-only-models) and to all incremental models in [forward-only plans](../concepts/plans.md#destructive-changes).

A model's [`on_destructive_change` setting](../guides/incremental_time.md#destructive-changes) determines whether it errors (default), warns, or silently allows the changes. SQLMesh always allows non-destructive forward-only schema changes, such as adding or casting a column in place.

`on_schema_change` configuration values are mapped to these SQLMesh `on_destructive_change` values:

| `on_schema_change` | SQLMesh `on_destructive_change` |
| ------------------ | ------------------------------- |
| ignore             | warn                            |
| append_new_columns | warn                            |
| sync_all_columns   | allow                           |
| fail               | error                           |


## Snapshot support

SQLMesh supports both dbt snapshot strategies of either `timestamp` or `check`.
Only unsupported snapshot functionality is `invalidate_hard_deletes` which must be set to `True`.
If set to `False`, then the snapshot will be skipped and a warning will be logged indicating this happened.
Support for this will be added soon.

## Tests
SQLMesh uses dbt tests to perform SQLMesh [audits](../concepts/audits.md) (coming soon).

Add SQLMesh [unit tests](../concepts/tests.md) to a dbt project by placing them in the "tests" directory.

## Seed column types

SQLMesh parses seed CSV files using [Panda's `read_csv` utility](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html) and its default column type inference.

dbt parses seed CSV files using [agate's csv reader](https://agate.readthedocs.io/en/latest/api/csv.html#csv-reader-and-writer) and [customizes agate's default type inference](https://github.com/dbt-labs/dbt-common/blob/ae8ffe082926fdb3ef2a15486588f40c7739aea9/dbt_common/clients/agate_helper.py#L59).

If SQLMesh and dbt infer different column types for a seed CSV file, you may specify a [column_types](https://docs.getdbt.com/reference/resource-configs/column_types) dictionary in your `dbt_project.yml` file, where the keys define the column names and the values the data types.

``` yaml
seeds:
  <seed name>
    +column_types:
      <column name>: <SQL data type>
```

Alternatively, you can define this dictionary in the seed [seed properties configuration file](https://docs.getdbt.com/reference/seed-properties).

``` yaml
seeds:
  - name: <seed name>
    config:
      column_types:
        <column name>: <SQL data type>
```

You may also specify a column's SQL data type in its `data_type` key, as shown below. The file must list all columns present in the CSV file; SQLMesh's default type inference will be used for columns that do not specify the `data_type` key.

``` yaml
seeds:
  - name: <seed name>
    columns:
      - name: <column name>
        data_type: <SQL data type>
```

## Package Management
SQLMesh does not have its own package manager; however, SQLMesh's dbt adapter is compatible with dbt's package manager. Continue to use [dbt deps](https://docs.getdbt.com/reference/commands/deps) and [dbt clean](https://docs.getdbt.com/reference/commands/clean) to update, add, or remove packages.

## Documentation
Model documentation is available in the [SQLMesh UI](../quickstart/ui.md#2-open-the-sqlmesh-web-ui).

## Using Airflow
To use SQLMesh and dbt projects with Airflow, first configure SQLMesh to use Airflow as described in the [Airflow integrations documentation](./airflow.md).

Then, install dbt-core within airflow.

Finally, replace the contents of `config.py` with:

```bash
> from pathlib import Path
>
> from sqlmesh.core.config import AirflowSchedulerConfig
> from sqlmesh.dbt.loader import sqlmesh_config
>
> config = sqlmesh_config(
>     Path(__file__).parent,
>     default_scheduler=AirflowSchedulerConfig(
>         airflow_url="https://<Airflow Webserver Host>:<Airflow Webserver Port>/",
>         username="<Airflow Username>",
>         password="<Airflow Password>",
>     )
> )
```

See the [Airflow configuration documentation](https://airflow.apache.org/docs/apache-airflow/2.1.0/configurations-ref.html) for a list of all AirflowSchedulerConfig configuration options. Note: only the python config file format is supported for dbt at this time.

The project is now configured to use airflow. Going forward, this also means that the engine configured in airflow will be used instead of the target engine specified in profiles.yml.

## Supported dbt jinja methods

SQLMesh supports running dbt projects using the majority of dbt jinja methods, including:

| Method      | Method         | Method       | Method  |
| ----------- | -------------- | ------------ | ------- |
| adapter (*) | env_var        | project_name | target  |
| as_bool     | exceptions     | ref          | this    |
| as_native   | from_yaml      | return       | to_yaml |
| as_number   | is_incremental | run_query    | var     |
| as_text     | load_result    | schema       | zip     |
| api         | log            | set          |         |
| builtins    | modules        | source       |         |
| config      | print          | statement    |         |

\* `adapter.rename_relation` and `adapter.expand_target_column_types` are not currently supported.

## Unsupported dbt jinja methods

The dbt jinja methods that are not currently supported are:

* debug
* selected_sources
* adapter.expand_target_column_types
* adapter.rename_relation
* schemas
* graph.nodes.values
* graph.metrics.values
* version - learn more about why SQLMesh doesn't support model versions at the [Tobiko Data blog](https://tobikodata.com/the-false-promise-of-dbt-contracts.html)

## Missing something you need?

Submit an [issue](https://github.com/TobikoData/sqlmesh/issues), and we'll look into it!
