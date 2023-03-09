# Overview

This collection of topics provides a conceptual overview of what SQLMesh does and how its components fit together.

## What is data transformation?
Data is essential for understanding what is happening with your business or application; it helps you make informed decisions. 

However, data in its raw form (application logs, transactional database tables, and so forth) is not particularly useful for making decisions. By joining various tables together or computing aggregations, it's easier to interpret, analyze, and then take action based on data. 

We refer to the entire system that creates analysis results from raw data as a collection of "data pipelines."

As data becomes integrated into your business operations, you are likely to add more raw data sources, create more joins and aggregations, and send results to more places (such as BI dashboards or executive reports). Pipelines proliferate, and the overall system complexity can become overwhelming.

This is where a data transformation platform comes in: to make it easy to create and organize complex data pipelines with many dependencies and relationships.

## Types of data transformation processes

There are many types of data transformation processes. We will focus on three, which we refer to as manual, scheduler-based, and model-aware.

### Manual
If your data or organization is small, you may only have a couple of key metrics that you want to compute. In these scenarios, running SQL queries or Python scripts manually will get the job done. As your organization grows (more people or more data), a manual process quickly becomes unmaintainable.

### Scheduler-based
Organizations that have outgrown manual pipelines often build around an orchestration framework such as [Airflow](https://airflow.apache.org/) or [Prefect](https://www.prefect.io/). Although these frameworks handle dependencies and scheduling, they are very generic. 

You may need to develop custom tooling to make it easier to work with these frameworks. Additionally, less technical data professionals may have trouble working with them because they are complex and designed for engineers.

### Model-aware
The final class of data transformation processes integrate more elements of building data pipelines than just scheduling. It includes tools like SQLMesh, [dbt](https://www.getdbt.com/), and [coalesce](https://coalesce.io/). 

Unlike generic scheduling tools, these platforms automate common data modeling patterns. For example, they might support various materialization strategies instead of requiring users to specify materialization for each model individually.

They are "model-aware" because they can automatically determine the set of actions needed to accomplish a task based on the pipeline's models and their dependency structure. 

Read more about why SQLMesh is the most efficient and powerful data transformation platform [here](../index.md).

## How SQLMesh works
SQLMesh is a Python framework that automates everything needed to run a scaleable data transformation platform. SQLMesh works with a variety of [engines and schedulers](../integrations/overview.md). 

It was created with a focus on both data and organizational scale and works regardless of your data warehouse or SQL engine's capabilities.

You can use SQLMesh with the [CLI](../reference/cli.md), [notebook](../reference/notebook.md), or [Python](../reference/python.md) APIs.

### Create models
You begin by writing your business logic in SQL or Python. A model consists of code that returns a single table or view; a pipeline contains more than one (and potentially many) models.

### Plan
Changing a single model can have dramatic effects downstream when working with complex pipelines. After making a change you generate a SQLMesh plan using the `plan` command.

The plan allows you to understand the full scope of a change's effects by automatically identifying both directly and indirectly-impacted workflows. This gives a holistic view of all impacts a change will have.

### Apply
After using `plan` to understand the impacts of a change, you need to test whether the change does what you intend. You do this with the SQLMesh `apply` command.

Applying a SQLMesh plan executes the changed models in isolated environments for testing and validation, seamlessly handling backfilling and reuse of existing tables. 

### Deploy
When development is complete, promoting an environment to production is quick and has no downtime. 

TODO

## Infrastructure and deployment
Every company's data infrastructure is different. SQLMesh is flexible with regard to which engines and orchestration frameworks you use. SQLMesh's only requirement is access to the target SQL / analytics engine.

SQLMesh is able to keep track of model versions and intervals using your existing infrastructure. If SQLMesh is configured without a scheduler, it will automatically create a `sqlmesh` database in your data warehouse. It will use this database for internal metadata and physical storage of SQLMesh managed tables. 

If SQLMesh is configured with Airflow, then it will store all its metadata in the Airflow database. Read more about how [SQLMesh integrates with Airflow](../integrations/airflow.md).
