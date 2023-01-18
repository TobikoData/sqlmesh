# Environments

Environments are isolated namespaces that allow you to develop and deploy SQLMesh projects. If an environment isn't specified, the `prod` environment is used which does not append a prefix to model names. Given a [model](/concepts/models) `db.table`, the `prod` environment would create this model in `db.table`, but the `dev` environment would be located at `dev__db.table`.

## What are environments for?
It is important to be able to iterate and test changes to models with production data. Data pipelines can be very complex, consisting of many chained jobs. Being able to recreate your entire warehouse with these changes is powerful for understanding the full impact of your changes, but is usually expensive or time consuming. SQLMesh environments allow you to easily spin up 'clones' of your warehouse quickly and efficiently. SQLMesh understands which models have changed compared to the base environment and only recomputes / backfills what doesn't already exist. Any changes or backfills within this environment **will not impact** other environments. However, any work that was done in this environment **can be reused safely** from other environments.

## How do you use an environment?
When running the [plan](/concepts/plans) command, the environment is the first variable. You can specify any string as your environment name. The only special environment by default is `prod`. All other environments will prefix the environment name to all models.

## How does it work?
Every model definition has a unique [fingerprint](/concepts/snapshots/#fingerprints). This fingerprint allows SQLMesh to detect if it exists in another environment or if it brand new. Because models depend on other models, the fingerprint also takes into account its upstream fingerprints. If a fingerpint already exists in SQLMesh, it is safe to reuse the existing table because the logic is exactly the same. So an environment is essential a collection of [snapshots](/concepts/snapshots) of models.

## Date ranges
A non-production environment consists of a start date and end date. When creating development environments, you usally want to test your data on a subset of dates, like the last week or last month of data. Non-production environments do not automatically schedule recurring jobs.

## Forward only environments
For users with massive datasets, it can be infeasible or too expensive to backfill some tables. With these datasets, testing changes is traditionally very difficult and changes are dangerous because they must be done in-place with [alter table](https://www.w3schools.com/SQl/sql_alter.asp) statements. SQLMesh has first class support for handling these types of models with [forward-only plans](/concepts/plans/#forward-only-plans).

When an environment is created with a forward-only plan, backfilled tables are not able to reuse existing tables. This is because forward-only plans mutate existing tables. In order to guarantee all SQLMesh operations are safe, all backfilled tables are brand new. Because forward-only models are usually large and expensive, it is recommended to only backfill as much data as is necessary to validate your changes.
