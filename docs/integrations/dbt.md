# dbt

SQLMesh has native support for reading dbt projects. 

**Note:** This feature is currently under development. You can view the [development backlog](https://github.com/orgs/TobikoData/projects/1/views/3) to see what improvements are already planned. If you are interested in this feature, we encourage you to try it with your dbt projects and [submit issues](https://github.com/TobikoData/sqlmesh/issues) so we can make it more robust.

## Getting started
### Reading a dbt project

Create a SQLMesh project from an existing dbt project by running the `init` command *within the dbt project root directory* and with the `dbt` template option:

```bash
$ sqlmesh init -t dbt
```

SQLMesh will use the data warehouse connection target in your dbt project `profiles.yml` file. The target can be changed at any time.

### Setting model backfill start dates

Models **require** a start date for backfilling data through use of the `start` configuration parameter. `start` can be defined individually for each model, or globally in the `dbt_project.yml` file as follows:

```
> models:
>   +start: Jan 1 2000
```

### Running SQLMesh

Run SQLMesh as with any SQLMesh project, generating and applying [plans](../concepts/overview.md#make-a-plan), running [tests](../concepts/overview.md#tests) or [audits](../concepts/overview.md#audits), and executing models with a [scheduler](../guides/scheduling.md) if desired. 

You continue to use your dbt file and project format.

## Workflow differences between SQLMesh and dbt

Consider the following when using a dbt project:

* SQLMesh will detect and deploy new or modified seeds as part of running the `plan` command and applying changes - there is no separate seed command. Refer to [seed models](../concepts/models/seed_models.md) for more information.
* The `plan` command dynamically creates environments, so environments do not need to be hardcoded into your `profiles.yml` file as targets. To get the most out of SQLMesh, point your dbt profile target at the production target, and let SQLMesh handle the rest for you.
* The term "test" has a different meaning in dbt than in SQLMesh: 
    - dbt "tests" are [audits](../concepts/audits.md) in SQLMesh.
    - SQLMesh "tests" are [unit tests](../concepts/tests.md), which test query logic before applying a SQLMesh plan.
* dbt's' recommended incremental logic is not compatible with SQLMesh, so small tweaks to the models are required (don't worry - dbt can still use the models!).

## How to use SQLMesh incremental models with dbt projects

Incremental loading is a powerful technique when datasets are large and recomputing tables is expensive. SQLMesh offers first-class support for incremental models, and its approach differs from dbt's.

SQLMesh automatically detects and offers to backfill missing time intervals for incremental models. dbt's incremental logic does not support intervals and is not compatible with SQLMesh.

This section describes how to implement SQLMesh incremental models in a dbt-formatted project.

### dbt's incremental logic
dbt's incremental logic is implemented with jinja blocks gated by `{% if is_incremental() %}`. 

Existing uses of these blocks do not need to be removed from the dbt project's models, but SQLMesh will ignore them.

### SQLMesh's incremental logic
SQLMesh's incremental logic is implemented in dbt projects with jinja blocks gated by `{% if sqlmesh is defined %}`.

SQLMesh supports two approaches to implement [idempotent](../concepts/glossary.md#idempotency) incremental loads: 
- Using merge (with the sqlmesh [`incremental_by_unique_key` model kind](../concepts/models/model_kinds.md#incremental_by_unique_key)) 
- Using insert-overwrite/delete+insert (with the sqlmesh [`incremental_by_time_range` model kind](../concepts/models/model_kinds.md#incremental_by_time_range))

A model using the insert-overwrite approach must specify the model's time column. The following example jinja block is for an `INCREMENTAL_BY_TIME_RANGE` model kind with a `time_column` named "ds". 

The SQL `WHERE` clause selecting a time interval with the "ds" column goes in a jinja block gated by `{% if sqlmesh is defined %}`:

```bash
> {% if sqlmesh is defined %}
>   WHERE
>     ds BETWEEN '{{ start_ds }}' AND '{{ end_ds }}'
> {% endif %}
```

For more information about how to use different time types or unique keys with incremental loads, refer to [incremental model kinds](../concepts/models/model_kinds.md).

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

The majority of dbt jinja methods are supported, including:

| Method      | Method              | Method       | Method
| ------      | ------              | ------       | ------
| adapter (*) | env_var             | project_name | target
| as_bool     | exceptions          | ref          | this
| as_native   | from_yaml           | return       | to_yaml
| as_number   | is_incremental (**) | run_query    | var
| as_text     | load_result         | schema       | zip
| api         | log                 | set          |
| builtins    | modules             | source       |
| config      | print               | statement    |

\* `adapter.rename_relation` and `adapter.expand_target_column_types` are not currently supported.

\*\* this is ignored, see [above](#insert-overwrite-and-deleteinsert-modifications).

## Unsupported dbt features

SQLMesh is continuously adding more dbt features. This is a list of major features that are currently unsupported, but it is not exhaustive:

* dbt deps 
    - While SQLMesh can read dbt packages, it does not currently support managing those packages. 
    - Continue to use dbt deps and dbt clean to update, add, or remove packages. For more information, refer to the [dbt deps](https://docs.getdbt.com/reference/commands/deps) documentation.
* dbt test (in development)
* dbt docs 
* dbt snapshots

## Missing something you need?

Submit an [issue](https://github.com/TobikoData/sqlmesh/issues), and we'll look into it!
