# dbt

SQLMesh provides support for the dbt project structure.

## Importing a dbt project

SQLMesh supports reading an existing dbt project in order to create a new project. To initialize SQLMesh to do so, run the following command within the project root:

```bash
$ sqlmesh init -t dbt
```

The target in your `profiles.yml` file will be used by default. To use a different target, run the command above with the `--connection TARGET_NAME` command line option.

**Note:** Models require a start date for backfilling data through use of the `start` configuration parameter. Start can be defined for each model, or globally in the `dbt_project.yml` file as follows:

```
> models:
>   +start: Jan 1 2000
```

## Workflow differences between SQLMesh and dbt

The following are considerations when importing a dbt project:

* SQLMesh will detect and deploy new or modified seeds as part of running the `plan` command and applying changes. There is no separate seed command. Refer to [seed models](/concepts/models/seed_models) for more information.
* The `plan` command dynamically creates environments, and therefore environments do not need to be hardcoded into your `profiles.yml` file as targets. To get the most out of SQLMesh, point your default target at the production target, and let SQLMesh handle the rest for you.
* dbt tests are considered [audits](/concepts/audits) in SQLMesh. SQLMesh tests are [unit tests](/concepts/tests), which test query logic before applying a plan.

## How to use SQLMesh incremental models within dbt

SQLMesh uses true incremental models, which are capable of detecting and backfilling any missing intervals. dbt's incremental logic does not support intervals, and is not compatible with SQLMesh.

### Mapping dbt incremental to SQLMesh incremental
SQLMesh ensures [idempotent](/concepts/glossary#idempotency) incremental loads through the use of merge (sqlmesh calls this `incremental_by_unique_key`) and insert-overwrite (sqlmesh calls this `incremental_by_time`) incremental strategies: 

1. For merge, specify the `unique_key` configuration field containing the name of the model's unique key column to use. Append is not idempotent, and therefore is not supported.
2. For insert-overwrite, add a `time_column` configuration field with the value of the name of the model's time column to use. 

### Model modifications

Since SQLMesh tracks intervals to deliver true incremental behavior, the dbt incremental `WHERE` statements are not compatible with SQLMesh's incremental `WHERE` statements. 

In order to maintain backwards compatibility with dbt, SQLMesh will ignore any jinja blocks using `{% if is_incremental() %}`, and will instead ask you define a new jinja block gated by `{% if sqlmesh is defined %}`. 

For example, for incremental by time using a ds `time_column`:

```bash
> {% if sqlmesh is defined %}
>   WHERE
>     ds BETWEEN '{{ start_ds }}' AND '{{ end_ds }}'
> {% endif %}
```

For more information about how to use different time types or unique keys, refer to [incremental model kinds](/concepts/models/model_kinds).

## Unsupported dbt features

While SQLMesh can read dbt packages, it does not currently support managing those packages. Continue to use dbt deps and dbt clean to update, add, or remove packages. For more information, refer to [dbt deps](https://docs.getdbt.com/reference/commands/deps).
