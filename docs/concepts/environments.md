## Environments
Environments are isolated namespaces that allow you to develop and deploy SQLMesh projects. If an environment isn't specified, the `prod` environment is used, which does not append a prefix to model names. Given a [model](/concepts/models) `db.table`, the `prod` environment would create this model in `db.table`. The `dev` environment would be located at `dev__db.table`. All environments other than `prod` are considered to be development environments.

Models in `dev` environments also get a special suffix appended to the schema portion of their names. For example, if the model's name is `db.model_a`, it will be available under the name `db__my_dev.model_a` in the `my_dev` environment.

By default, the [`sqlmesh plan`](/concepts/plans) command targets the `prod` environment.

## Why use environments?
It is important to be able to iterate and test changes to models with production data. Data pipelines can be very complex and can consist of many chained jobs. Being able to recreate your entire warehouse with these changes is powerful in order to understand the full impact of your changes, but usually expensive or time consuming.

SQLMesh environments allow you to easily spin up 'clones' of your warehouse quickly and efficiently. SQLMesh understands which models have changed compared to the base environment, and only recomputes/backfills what doesn't already exist. Any changes or backfills within this environment **will not impact** other environments. However, any work that was done in this environment **can be reused safely** from other environments.

## How do you use an environment?
When running the [plan](/concepts/plans) command, the environment is the first variable. You can specify any string as your environment name. The only special environment by default is `prod`. All other environments will prefix the environment name to all models.

### Example
A custom name can be provided as an argument to create/update a development environment. For example, to target an environment with name `my_dev`, run:

```bash
$ sqlmesh plan my_dev
```
A new environment is created automatically the first time a plan is applied to it.

## How do environments work?
Every model definition has a unique [fingerprint](/concepts/architecture/snapshots/#fingerprints). This fingerprint allows SQLMesh to detect if it exists in another environment or if it brand new. Because models depend on other models, the fingerprint also takes into account its upstream fingerprints. If a fingerpint already exists in SQLMesh, it is safe to reuse the existing table because the logic is exactly the same. An environment is essentially a collection of [snapshots](/concepts/architecture/snapshots) of models.

## Date ranges ##
A non-production environment consists of a start date and end date. When creating development environments, you usally want to test your data on a subset of dates, such as the last week or last month of data. Non-production environments do not automatically schedule recurring jobs.
