# Overview

The concepts section provides various topics about SQLMesh and how they all fit together.

## What is a data transformation platform
Data is essential for understanding what is happening to your business or application and helps you make informed decisions. Data in its raw form (application logs, transactional database tables) is not particular useful for making decisions. By joining various tables together or computing aggregations, it's easier to interpret and take action on data.

A data transformation platform makes it easy to create and organize complex data pipelines with many dependencies and relationships.

There are three levels of data transformation processes.

### Manual
If your data / organization is small, you may only have a couple of key metrics that you want to compute. In these scenariors, running SQL queries or Python scripts manually will get the job done. As your organization grows (more people / more data), a manual process quickly becomes unmaintainable.

### Scheduler Based
A common approach for organizations that have grown past manual pipelines is to build around an orchestration framework like [Airflow](https://airflow.apache.org/) or [Prefect](https://www.prefect.io/). Although these frameworks handle dependencies and scheduling, they are very generic. Custom tooling needs to be developed in order to make it easier to work with these frameworks. Less technical data professionals may have trouble working with these tools directly because they are complex and geared towards engineers.

### Model Aware
The final class of data transformation platforms provides more integrations to common data modeling patterns like **SQLMesh**, [dbt](https://www.getdbt.com/) and [coalesce](https://coalesce.io/). Unlike generic scheduling tools, these platforms provide automation around common patterns like natively supporting various materialization strategies.

Read more about why SQLMesh is the most efficient and powerful data transformation platform [here](/#why-sqlmesh).

## How it works
SQLMesh allows data analysts, scientists, and engineers to unify around common tooling while guaranteeing scalable modern data best practices.

SQLMesh also makes it easy to iterate, test, and deploy code and data changes, and is built around two main commands: plan and apply.
