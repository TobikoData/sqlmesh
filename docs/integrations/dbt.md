# dbt

SQLMesh has native support for running dbt projects with its dbt adapter.

**Note:** This feature is currently under development. You can view the [development backlog](https://github.com/orgs/TobikoData/projects/1/views/3) to see what improvements are already planned. If you are interested in this feature, we encourage you to try it with your dbt projects and [submit issues](https://github.com/TobikoData/sqlmesh/issues) so we can make it more robust.

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

It's important to note, that the `on_schema_change` setting is ignored by SQLMesh. Schema changes are only applied during the [plan](../concepts/plans.md) application (i.e. `sqlmesh plan`) and never during runtime (i.e. `sqlmesh run`). The target table's schema is **always** updated to match the model's query, as if the `on_schema_change` setting was set to `sync_all_columns`.

## Tests
SQLMesh uses dbt tests to perform SQLMesh [audits](../concepts/audits.md) (coming soon).

Add SQLMesh [unit tests](../concepts/tests.md) to a dbt project by placing them in the "tests" directory.

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
>     scheduler=AirflowSchedulerConfig(
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

| Method      | Method          | Method       | Method
| ------      | ------          | ------       | ------
| adapter (*) | env_var         | project_name | target
| as_bool     | exceptions      | ref          | this
| as_native   | from_yaml       | return       | to_yaml
| as_number   | is_incremental  | run_query    | var
| as_text     | load_result     | schema       | zip
| api         | log             | set          |
| builtins    | modules         | source       |
| config      | print           | statement    |

\* `adapter.rename_relation` and `adapter.expand_target_column_types` are not currently supported.

## Unsupported dbt features

SQLMesh is continuously adding functionality to run dbt projects. This is a list of major dbt features that are currently unsupported, but it is not exhaustive:

* dbt deps
    - While SQLMesh can read dbt packages, it does not currently support managing those packages.
    - Continue to use dbt deps and dbt clean to update, add, or remove packages. For more information, refer to the [dbt deps](https://docs.getdbt.com/reference/commands/deps) documentation.
* dbt test (in development)
* dbt docs
* dbt snapshots

The dbt jinja methods that are not currently supported are:

* debug
* run_started_at
* selected_sources
* adapter.expand_target_column_types
* adapter.rename_relation
* schemas
* graph.nodes.values
* graph.metrics.values

## Missing something you need?

Submit an [issue](https://github.com/TobikoData/sqlmesh/issues), and we'll look into it!
