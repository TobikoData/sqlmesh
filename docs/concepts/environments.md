## Environments
Environments are isolated namespaces that allow you to test and preview your changes.

SQLMesh differentiates between production and development environments. Currently, only the environment with the name `prod` is treated by SQLMesh as the production one. Environments with other names are considered to be development ones.

[Models](models/overview.md) in development environments get a special suffix appended to the schema portion of their names. For example, to access data for a model with name `db.model_a` in the target environment `my_dev`, the `db__my_dev.model_a` table name should be used in a query. Models in the production environment are referred to by their original names.

## Why use environments
Data pipelines and their dependencies tend to grow in complexity over time, and so assessing the impact of local changes can become quite challenging. Pipeline owners may not be aware of all downstream consumers of their pipelines, or may drastically underestimate the impact a change would have. That's why it is so important to be able to iterate and test model changes using production dependencies and data, while simultaneously avoiding any impact to existing datasets or pipelines that are currently used in production. Recreating the entire data warehouse with given changes would be an ideal solution to fully understand their impact, but this process is usually excessively expensive and time consuming.

SQLMesh environments allow you to easily spin up shallow 'clones' of the data warehouse quickly and efficiently. SQLMesh understands which models have changed compared to the target environment, and only computes data gaps that have been directly caused by the changes. Any changes or backfills within the target environment **do not impact** other environments. At the same time, any computation that was done in this environment **can be safely reused** in other environments.

## How to use environments
When running the [plan](plans.md) command, the environment name can be supplied in the first argument. An arbitrary string can be used as an environment name. The only special environment name by default is `prod`, which refers to the production environment. Environment with names other than `prod` are considered to be development environments.

By default, the [`sqlmesh plan`](plans.md) command targets the production (`prod`) environment.

### Example
A custom name can be provided as an argument to create or update a development environment. For example, to target an environment with name `my_dev`, run:

```bash
sqlmesh plan my_dev
```
A new environment is created automatically the first time a plan is applied to it.

## How environments work
Whenever a model definition changes, a new model snapshot is created with a unique [fingerprint](architecture/snapshots.md#fingerprints). This fingerprint allows SQLMesh to detect if a given model variant exists in other environments or if it's a brand new variant. Because models may depend on other models, the fingerprint of a target model variant also includes fingerprints of its upstream dependencies. If a fingerprint already exists in SQLMesh, it is safe to reuse the existing physical table associated with that model variant, since we're confident that the logic that populates that table is exactly the same. This makes an environment a collection of references to model [snapshots](architecture/snapshots.md).

Refer to [plans](plans.md#plan-application) for additional details.

## Date range
A development environment includes a start date and end date. When creating a development environment, the intent is usually to test changes on a subset of data. The size of such a subset is determined by a time range defined through the start and end date of the environment. Both start and end date are provided during the [plan](plans.md) creation.
