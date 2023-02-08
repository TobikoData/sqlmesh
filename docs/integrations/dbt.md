# dbt

SQLMesh can read existing DBT projects.

- SQLMesh supports reading from a DBT project
- To initialize SQLMesh to read from a DBT project, run "sqlmesh init -t dbt" within the project root.
- The default target in your profiles will be used by default. To use a different target, use the "--connection TARGET_NAME" command line option.
# Workflow Differences
- SQLMesh will detect and deploy new/modified seeds as part of plan/apply. There is no separate "seed" command.
- SQLMesh plan dynamically creates environments and thus environments do not need to be hardcoded into your dbt profiles file as targets. To get the most out of SQLMesh, point your default target at the production target and let SQLMesh handle the rest for you.
- DBT tests are considered audits in SQLMesh

# How to use SQLMesh incremental models within a DBT project
- SQLMesh uses true incremental models, capable of detecting and backfilling any missing intervals. DBT's recommend incremental logic does not support intervals and is not compatible with SQLMesh.
- More to come. This section will be a bit involved.


# DBT Features Not Yet Supported
- Docs generation (https://docs.getdbt.com/reference/commands/cmd-docs)
- DBT package management. SQLMesh can read DBT packages, but it does not currently support managing the packages. Please continue to use dbt commands to update/add/remove packages.
