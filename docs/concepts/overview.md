# Overview

The concepts section provides various topics about SQLMesh and how they all fit together.

## What is a data transformation platform
Data is essential for understanding what is happening to your business or application and helps you make informed decisions. Data in its raw form (application logs, transactional database tables) is not particular useful for making decisions. By joining various tables together or computing aggregations, it's easier to interpret and take action on data.

A data transformation platform makes it easy to create and organize complex data pipelines with many dependencies and relationships.

There are three levels of data transformation processes.

### Manual
If your data / organization is small, you may only have a couple of key metrics that you want to compute. In these scenariors, running SQL queries or Python scripts manually will get the job done. As your organization grows (more people / more data), a manual process quickly becomes unmaintainable.

### Scheduler based
A common approach for organizations that have grown past manual pipelines is to build around an orchestration framework like [Airflow](https://airflow.apache.org/) or [Prefect](https://www.prefect.io/). Although these frameworks handle dependencies and scheduling, they are very generic. Custom tooling needs to be developed in order to make it easier to work with these frameworks. Less technical data professionals may have trouble working with these tools directly because they are complex and geared towards engineers.

### Model aware
The final class of data transformation platforms provides more integrations to common data modeling patterns like **SQLMesh**, [dbt](https://www.getdbt.com/) and [coalesce](https://coalesce.io/). Unlike generic scheduling tools, these platforms provide automation around common patterns like natively supporting various materialization strategies.

Read more about why SQLMesh is the most efficient and powerful data transformation platform [here](/#why-sqlmesh).

## How it works
SQLMesh is a Python framework that automates everything needed to run a scaleable data transformation platform. SQLMesh works with a variety of [engines and schedulers](/integrations/overview). It was created with a focus on both data / organizational scale in mind.

### Write models
You begin by writing your business logic in SQL or Python which will result in a Table or View.

### Plan / Apply
Changing SQL query models can have dramatic effects downstream when working with complex pipelines. SQLMesh's plan command allows developers to understand the full scope of directly and indirectly-impacted workflows automatically, giving them a holistic view of the changes.

Deploying new pipelines can be time-consuming, expensive, and error-prone. A SQLMesh plan can be applied allowing developers to deploy their changes to isolated environments for testing and validation, seamlessly handling backfilling and reuse of existing tables. When development is complete, promoting an environment to production is quick and has no downtime. SQLMesh is able to accomplish all of this regardless of your data warehouse or SQL engine's capabilities.

You can interact with SQLMesh through a [CLI](/api/cli), [Notebook](/api/notebook), or [Python API](/api/python).

## Infrastructure / Deployment
Every company's data infrastructure and situation is different. SQLMesh is flexible with regard to which engines and orchestration frameworks you use. The only requirement for SQLMesh is that you have access to a SQL engine.

SQLMesh is able to keep track of model versions and intervals using your existing infrastructure. If SQLMesh is configured without a scheduler, it will automatically create a `sqlmesh` database in your warehouse. It will use this database for internal metadata and physical storage of SQLMesh managed tables. If SQLMesh is configured with Airflow, then it will store all metadata in [XComs](https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html).
