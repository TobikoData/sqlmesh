# dbt

SQLMesh supports the DBT project structure.

# Basics
- SQLMesh supports reading from a DBT project
- To initialize SQLMesh to read from a DBT project, run "sqlmesh init -t dbt" within the project root.
- The default target in your profiles.yml will be used by default. To use a different target, use the "--connection TARGET_NAME" command line option.
- Models require a start date for backfilling data through use of the "start" configuration parameter. Start can be defined for each model or globally in dbt_project.yml as follows
> models:
>   +start: Jan 1 2000

# Workflow Differences
- SQLMesh will detect and deploy new/modified seeds as part of plan/apply. There is no separate "seed" command.
- SQLMesh plan dynamically creates environments and thus environments do not need to be hardcoded into your dbt profiles file as targets. To get the most out of SQLMesh, point your default target at the production target and let SQLMesh handle the rest for you.
- DBT tests are considered audits in SQLMesh. SQLMesh tests are unit tests, which test query logic before applying a plan. (Link to audits and tests documentation)

# How to use SQLMesh incremental models within a DBT project
- SQLMesh uses true incremental models, capable of detecting and backfilling any missing intervals. DBT's recommend incremental logic does not support intervals and is not compatible with SQLMesh.

## Mapping DBT incremental to SQLMesh incremental
- SQLMesh ensures idempotent (link to idempotent) incremental loads through use of merge (sqlmesh calls this incremental_by_unique_key) and insert-overwrite (sqlmesh calls this incremental_by_time) incremental strategies. To use insert-overwrite, add a time_column configuration field with the value being the name of the model's time column to use. For merge, specify the unique_key configuration field containing the name of the model's unique key column to use. Append is not idempotent and thus is not supported.
## Model Modifications
- Since SQLMesh tracks intervals to deliver true incremental behavior, the DBT incremental WHERE statements are not compatible with SQLMesh's incremental WHERE statements. 
- In order to maintain backwards compatibility with DBT, SQLMesh will ignore any jinja blocks using {% if is_incremental() %} and instead ask you define a new jinja block gated by {% if sqlmesh is defined %}. For example for incremental by time using a ds time_column:
> {% if sqlmesh is defined %}
>   WHERE
>     ds BETWEEN '{{ start_ds }}' AND '{{ end_ds }}'
> {% endif %}
- See SQLMesh incremental documentation here for using different time types or unique_key
# DBT Features Not Yet Supported
- dbt docs (https://docs.getdbt.com/reference/commands/cmd-docs)
- dbt deps - While SQLMesh can read DBT packages, it does not currently support managing the packages. Continue to use dbt deps and dbt clean to update/add/remove packages.
