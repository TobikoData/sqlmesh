# dbt

SQLMesh has native support for running dbt projects. This featuring is currently under development. You can view the development backlog [here](https://github.com/orgs/TobikoData/projects/1/views/3). If you are interested in this feature, we encourage you to try it with your dbt projects and submit issues here (https://github.com/TobikoData/sqlmesh/issues), so we can make it more robust.

## Getting Started
### Importing a dbt project

A SQLMesh project can be configured during initialization to read from a dbt formatted project. To do so, run the following command within the dbt project root:

```bash
$ sqlmesh init -t dbt
```

The target specified in your `profiles.yml` file will be used by default. The target can be changed at anytime.

**Note:** Models require a start date for backfilling data through use of the `start` configuration parameter. Start can be defined for each model, or globally in the `dbt_project.yml` file as follows:

```
> models:
>   +start: Jan 1 2000
```

### Running SQLMesh

Link to how to normally run sqlmesh here (plan, run). Continue to use your dbt format.

### Workflow differences between SQLMesh and dbt

The following are considerations when importing a dbt project:

* SQLMesh will detect and deploy new or modified seeds as part of running the `plan` command and applying changes. There is no separate seed command. Refer to [seed models](/concepts/models/seed_models) for more information.
* The `plan` command dynamically creates environments, and therefore environments do not need to be hardcoded into your `profiles.yml` file as targets. To get the most out of SQLMesh, point your profile target at the production target, and let SQLMesh handle the rest for you.
* dbt tests are considered [audits](/concepts/audits) in SQLMesh. SQLMesh tests are [unit tests](/concepts/tests), which test query logic before applying a plan.
* SQLMesh's incremental models track which intervals have been filled and automatically detects and fills interval gaps. dbt does not support intervals and their recommended incremental logic is not compatible, requiring small tweaks to the models (don't worry dbt compatibility is maintained).

## How to use SQLMesh incremental models within dbt

SQLMesh's incremental models track uses true incremental models, which are capable of detecting and backfilling any missing intervals. dbt's incremental logic does not support intervals, and is not compatible with SQLMesh.

### Mapping dbt incremental to SQLMesh incremental
SQLMesh supports [idempotent](/concepts/glossary#idempotency) incremental loads through the use of merge (sqlmesh calls this `incremental_by_unique_key`) and insert-overwrite/delete+insert (sqlmesh calls this `incremental_by_time`) incremental strategies. Append is not currently supported and not recommended due to not being idempotent.



#### Merge modifications



#### Insert-overwrite and delete+insert modifications
1. For insert-overwrite, add a `time_column` configuration field with the value of the name of the model's time column to use. 

As mentioned in the workflow changes, a small model tweak is required. In order to maintain backwards compatibility with dbt, SQLMesh will ignore any jinja blocks using `{% if is_incremental() %}`, and will instead ask you define a new jinja block gated by `{% if sqlmesh is defined %}`. 

For example, for incremental by time using a ds `time_column`:

```bash
> {% if sqlmesh is defined %}
>   WHERE
>     ds BETWEEN '{{ start_ds }}' AND '{{ end_ds }}'
> {% endif %}
```

For more information about how to use different time types or unique keys, refer to [incremental model kinds](/concepts/models/model_kinds).

### Unit Tests
This is the same as sqlmesh unit tests...link to that. Yes, they go in the same folder as dbt tests (audits).

## Using airflow
Setup airflow following the airflow docs section

In config.py within the project root dir, add:

```bash
> airflow_config = sqlmesh_config(Path(__file__).parent, scheduler=AirflowSchedulerConfig())
```

See airflow docs for AirflowSchedulerConfig configuration options.


## Support dbt jinja methods

The majority of dbt jinja methods are supported. Here is a list (it'd be nice if the list was multiple columns so it wasn't so long):

- adapter
- as_bool
- as_native
- as_number
- as_text
- api
- builtins
- config
- env_var
- exceptions
- from_yaml
- is_incremental (always returns false, see incremental section)
- load_result
- log
- modules
- print
- project_name
- ref
- return
- run_query
- schema
- set
- source
- statement
- target
- this
- to_yaml
- var
- zip

## Unsupported dbt features

SQLMesh is continuously adding more dbt features

Not an exhaustive list, but trying to catch the major features

dbt deps 
- While SQLMesh can read dbt packages, it does not currently support managing those packages. Continue to use dbt deps and dbt clean to update, add, or remove packages. For more information, refer to [dbt deps](https://docs.getdbt.com/reference/commands/deps).

dbt test not currently supported, but in development

dbt docs is not supported, snapshots not supported

## Missing something you need?

Submit an issue here (https://github.com/TobikoData/sqlmesh/issues) and we'll look into it

